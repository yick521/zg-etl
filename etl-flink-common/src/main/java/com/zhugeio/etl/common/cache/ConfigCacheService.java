package com.zhugeio.etl.common.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 配置缓存服务
 *
 * 使用 CacheKeyConstants 定义的 Key，保持与原 Scala FrontCache 完全一致
 *
 * 功能:
 * 1. 两级缓存: L1 Caffeine + L2 KVRocks
 * 2. 版本检测: 自动感知配置变更
 * 3. 覆盖所有 ID 模块和 DW 模块的配置类缓存
 *
 * ⚠️ 重要: 集群模式下 Key 需要带 Hash Tag {}
 * ⚠️ 不包含运行时 ID 映射 (deviceId, userId, zgid)，请使用 OneIdService
 */
public class ConfigCacheService implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCacheService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private static final Integer NOT_FOUND = -999999;
    
    // 缓存 Key 常量
    private static final String SYNC_VERSION = "sync_version";
    private static final String APP_KEY_APP_ID_MAP = "app_key_app_id_map";
    private static final String APP_ID_EVENT_ID_MAP = "app_id_event_id_map";
    private static final String EVENT_ATTR_COLUMN_MAP = "event_attr_column_map";
    private static final String OPEN_CDP_APPID_MAP = "open_cdp_appid_map";

    //  新增: 初始化状态标记
    private transient volatile AtomicBoolean initialized = new AtomicBoolean(false);

    private transient KvrocksClient kvrocksClient;

    // L1 本地缓存
    private transient Cache<String, Integer> appKeyToIdCache;
    private transient Cache<Integer, Integer> appIdToCompanyIdCache;
    private transient Cache<String, Integer> appSdkHasDataCache;
    private transient Cache<String, Integer> eventIdCache;
    private transient Cache<String, Integer> eventAttrIdCache;
    private transient Cache<String, String> eventAttrColumnCache;
    private transient Cache<String, String> eventAttrAliasCache;
    private transient Cache<String, Integer> userPropIdCache;
    private transient Cache<String, String> userPropOriginalCache;
    private transient Cache<String, Integer> devicePropIdCache;
    private transient Cache<String, Boolean> blackEventIdCache;
    private transient Cache<String, Boolean> blackUserPropCache;
    private transient Cache<String, Boolean> blackEventAttrIdCache;
    private transient Cache<String, Boolean> createEventForbidCache;
    private transient Cache<String, Boolean> autoCreateDisabledCache;
    private transient Cache<String, Boolean> createAttrForbidCache;
    private transient Cache<String, Boolean> uploadDataCache;
    private transient Cache<String, Boolean> eventPlatformCache;
    private transient Cache<String, Boolean> eventAttrPlatformCache;
    private transient Cache<String, Boolean> devicePropPlatformCache;
    private transient Cache<String, Boolean> virtualEventAppCache;
    private transient Cache<String, Boolean> virtualPropAppCache;
    private transient Cache<String, List<String>> virtualEventCache;
    private transient Cache<String, Set<String>> virtualEventAttrCache;
    private transient Cache<String, List<String>> virtualEventPropCache;
    private transient Cache<String, List<String>> virtualUserPropCache;
    private transient Cache<String, Boolean> eventVirtualAttrCache;
    private transient Cache<String, Integer> advertisingAppCache;
    private transient Cache<String, String> adsLinkEventCache;
    private transient Cache<String, String> lidChannelEventCache;
    private transient Cache<String, Boolean> adFrequencyCache;
    private transient Cache<String, String> baseCurrentCache;
    private transient Cache<String, String> cdpConfigCache;
    private transient Cache<String, Boolean> businessCache;
    private transient Cache<String, String> yearweekCache;
    private transient Cache<String, Map<String, String>> hashDataCache;

    // 版本控制
    private transient volatile long currentVersion = 0;
    private transient volatile long lastVersionCheckTime = 0;
    private static final long VERSION_CHECK_INTERVAL_MS = 5000;

    // 配置 (需要序列化)
    private final CacheConfig config;

    // 统计
    private transient Map<String, AtomicLong> l1Hits;
    private transient Map<String, AtomicLong> l2Hits;
    private transient Map<String, AtomicLong> misses;

    public ConfigCacheService(CacheConfig config) {
        this.config = config;
    }

    /**
     *  修复: 添加 readObject 方法
     * 反序列化后自动重新初始化所有 transient 字段
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        
        // 重新初始化 AtomicBoolean (transient 字段反序列化后为 null)
        this.initialized = new AtomicBoolean(false);
        
        LOG.info("ConfigCacheService 反序列化完成，准备重新初始化");
        
        // 重新初始化
        init();
    }


    public void init() {
        // 确保 initialized 不为 null
        if (initialized == null) {
            initialized = new AtomicBoolean(false);
        }
        
        // 防止重复初始化
        if (!initialized.compareAndSet(false, true)) {
            LOG.debug("ConfigCacheService 已初始化，跳过");
            return;
        }

        try {
            // 初始化 KVRocks 客户端
            kvrocksClient = new KvrocksClient(
                    config.getKvrocksHost(),
                    config.getKvrocksPort(),
                    config.isKvrocksCluster()
            );
            kvrocksClient.init();

            if (!kvrocksClient.testConnection()) {
                initialized.set(false);
                throw new RuntimeException("KVRocks连接失败: " + config.getKvrocksHost());
            }

            // 初始化缓存
            initL1Caches();
            
            // 初始化统计
            initStats();
            
            // 加载版本
            loadVersion();

            LOG.info("ConfigCacheService 初始化完成, version={}", currentVersion);
            
        } catch (Exception e) {
            initialized.set(false);
            throw new RuntimeException("ConfigCacheService 初始化失败", e);
        }
    }

    /**
     *  新增: 确保已初始化
     */
    private void ensureInitialized() {
        if (initialized == null || !initialized.get()) {
            synchronized (this) {
                if (initialized == null) {
                    initialized = new AtomicBoolean(false);
                }
                if (!initialized.get()) {
                    LOG.warn("ConfigCacheService 未初始化，正在初始化...");
                    init();
                }
            }
        }
    }

    private void initL1Caches() {
        // App (相对稳定，容量小)
        appKeyToIdCache = buildCache(10000, 10);
        appIdToCompanyIdCache = buildCache(10000, 10);
        appSdkHasDataCache = buildCache(50000, 10);

        // 事件 (量大，按热度淘汰)
        // 注意: 40万事件属性无法全部缓存，依赖LRU淘汰冷数据
        // 如果命中率低，考虑增大容量或缩短过期时间让热数据常驻
        eventIdCache = buildCache(200000, 30);           // 事件数一般几万
        eventAttrIdCache = buildCache(500000, 30);       // 事件属性可达40万+
        eventAttrColumnCache = buildCache(500000, 60);   // 同上
        eventAttrAliasCache = buildCache(100000, 30);

        // 属性
        userPropIdCache = buildCache(100000, 30);
        userPropOriginalCache = buildCache(100000, 30);
        devicePropIdCache = buildCache(50000, 30);

        // 黑名单
        blackEventIdCache = buildCache(10000, 30);
        blackUserPropCache = buildCache(10000, 30);
        blackEventAttrIdCache = buildCache(10000, 30);
        createEventForbidCache = buildCache(5000, 10);
        autoCreateDisabledCache = buildCache(5000, 10);
        createAttrForbidCache = buildCache(10000, 10);
        uploadDataCache = buildCache(10000, 10);

        // 平台
        eventPlatformCache = buildCache(100000, 30);
        eventAttrPlatformCache = buildCache(200000, 30);
        devicePropPlatformCache = buildCache(50000, 30);

        // 虚拟
        virtualEventAppCache = buildCache(5000, 5);
        virtualPropAppCache = buildCache(5000, 5);
        virtualEventCache = buildCache(10000, 5);
        virtualEventAttrCache = buildCache(10000, 5);
        virtualEventPropCache = buildCache(10000, 5);
        virtualUserPropCache = buildCache(5000, 5);
        eventVirtualAttrCache = buildCache(50000, 30);

        // 广告
        advertisingAppCache = buildCache(5000, 10);
        adsLinkEventCache = buildCache(10000, 10);
        lidChannelEventCache = buildCache(10000, 10);
        adFrequencyCache = buildCache(100000, 60);

        // DW
        baseCurrentCache = buildCache(1000, 60);
        cdpConfigCache = buildCache(5000, 60);
        businessCache = buildCache(10000, 30);
        yearweekCache = buildCache(1000, 1440);
        hashDataCache = buildCache(50, 5);
    }

    private void initStats() {
        l1Hits = new ConcurrentHashMap<>();
        l2Hits = new ConcurrentHashMap<>();
        misses = new ConcurrentHashMap<>();
    }

    private <K, V> Cache<K, V> buildCache(int maxSize, int expireMinutes) {
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();
    }

    // ===================== 版本控制 =====================

    public void checkAndRefreshVersion() {
        ensureInitialized();
        
        long now = System.currentTimeMillis();
        if (now - lastVersionCheckTime < VERSION_CHECK_INTERVAL_MS) {
            return;
        }
        lastVersionCheckTime = now;

        try {
            String versionStr = kvrocksClient.asyncGet(SYNC_VERSION).get(1, TimeUnit.SECONDS);
            if (versionStr != null) {
                long newVersion = Long.parseLong(versionStr);
                if (newVersion > currentVersion) {
                    LOG.info("Cache version changed: {} -> {}", currentVersion, newVersion);
                    invalidateAllL1();
                    currentVersion = newVersion;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to check cache version", e);
        }
    }

    public void checkVersionAsync() {
        ensureInitialized();
        
        kvrocksClient.asyncGet(SYNC_VERSION)
                .thenAccept(versionStr -> {
                    if (versionStr != null) {
                        try {
                            long newVersion = Long.parseLong(versionStr);
                            if (newVersion > currentVersion) {
                                LOG.info("Cache version changed: {} -> {}", currentVersion, newVersion);
                                invalidateAllL1();
                                currentVersion = newVersion;
                            }
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid version format: {}", versionStr);
                        }
                    }
                })
                .exceptionally(ex -> {
                    LOG.warn("Failed to check cache version: {}", ex.getMessage());
                    return null;
                });
    }

    private void loadVersion() {
        try {
            String versionStr = kvrocksClient.asyncGet(SYNC_VERSION).get(5, TimeUnit.SECONDS);
            if (versionStr != null) {
                currentVersion = Long.parseLong(versionStr);
            }
        } catch (Exception e) {
            LOG.warn("Failed to load initial version", e);
        }
    }

    private void invalidateAllL1() {
        if (appKeyToIdCache != null) {
            appKeyToIdCache.invalidateAll();
        }
        if (appIdToCompanyIdCache != null) {
            appIdToCompanyIdCache.invalidateAll();
        }
        if (appSdkHasDataCache != null) {
            appSdkHasDataCache.invalidateAll();
        }
        if (eventIdCache != null) {
            eventIdCache.invalidateAll();
        }
        if (eventAttrIdCache != null) {
            eventAttrIdCache.invalidateAll();
        }
        if (eventAttrColumnCache != null) {
            eventAttrColumnCache.invalidateAll();
        }
        if (eventAttrAliasCache != null) {
            eventAttrAliasCache.invalidateAll();
        }
        if (userPropIdCache != null) {
            userPropIdCache.invalidateAll();
        }
        if (userPropOriginalCache != null) {
            userPropOriginalCache.invalidateAll();
        }
        if (devicePropIdCache != null) {
            devicePropIdCache.invalidateAll();
        }
        if (blackEventIdCache != null) {
            blackEventIdCache.invalidateAll();
        }
        if (blackUserPropCache != null) {
            blackUserPropCache.invalidateAll();
        }
        if (blackEventAttrIdCache != null) {
            blackEventAttrIdCache.invalidateAll();
        }
        if (createEventForbidCache != null) {
            createEventForbidCache.invalidateAll();
        }
        if (autoCreateDisabledCache != null) {
            autoCreateDisabledCache.invalidateAll();
        }
        if (createAttrForbidCache != null) {
            createAttrForbidCache.invalidateAll();
        }
        if (uploadDataCache != null) {
            uploadDataCache.invalidateAll();
        }
        if (eventPlatformCache != null) {
            eventPlatformCache.invalidateAll();
        }
        if (eventAttrPlatformCache != null) {
            eventAttrPlatformCache.invalidateAll();
        }
        if (devicePropPlatformCache != null) {
            devicePropPlatformCache.invalidateAll();
        }
        if (virtualEventAppCache != null) {
            virtualEventAppCache.invalidateAll();
        }
        if (virtualPropAppCache != null) {
            virtualPropAppCache.invalidateAll();
        }
        if (virtualEventCache != null) {
            virtualEventCache.invalidateAll();
        }
        if (virtualEventAttrCache != null) {
            virtualEventAttrCache.invalidateAll();
        }
        if (virtualEventPropCache != null) {
            virtualEventPropCache.invalidateAll();
        }
        if (virtualUserPropCache != null) {
            virtualUserPropCache.invalidateAll();
        }
        if (eventVirtualAttrCache != null) {
            eventVirtualAttrCache.invalidateAll();
        }
        if (advertisingAppCache != null) {
            advertisingAppCache.invalidateAll();
        }
        if (adsLinkEventCache != null) {
            adsLinkEventCache.invalidateAll();
        }
        if (lidChannelEventCache != null) {
            lidChannelEventCache.invalidateAll();
        }
        if (adFrequencyCache != null) {
            adFrequencyCache.invalidateAll();
        }
        if (baseCurrentCache != null) {
            baseCurrentCache.invalidateAll();
        }
        if (cdpConfigCache != null) {
            cdpConfigCache.invalidateAll();
        }
        if (businessCache != null) {
            businessCache.invalidateAll();
        }
        if (yearweekCache != null) {
            yearweekCache.invalidateAll();
        }
        if (hashDataCache != null) {
            hashDataCache.invalidateAll();
        }
    }

    // ===================== 查询方法 (添加 ensureInitialized) =====================

    public CompletableFuture<Integer> getAppIdByAppKey(String appKey) {
        ensureInitialized();
        
        if (appKey == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        Integer cached = appKeyToIdCache.getIfPresent(appKey);
        if (cached != null) {
            hit("appKey");
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(APP_KEY_APP_ID_MAP, appKey)
                .thenApply(value -> {
                    if (value != null) {
                        Integer appId = Integer.parseInt(value);
                        appKeyToIdCache.put(appKey, appId);
                        l2Hit("appKey");
                        return appId;
                    }
                    miss("appKey");
                    return null;
                });
    }

    public CompletableFuture<Integer> getEventId(Integer appId, String owner, String eventName) {
        ensureInitialized();
        
        if (appId == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + owner + "_" + eventName;
        Integer cached = eventIdCache.getIfPresent(field);
        if (cached != null) {
            hit("eventId");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND) ? null : cached);
        }

        return kvrocksClient.asyncHGet(APP_ID_EVENT_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer eventId = Integer.parseInt(value);
                        eventIdCache.put(field, eventId);
                        l2Hit("eventId");
                        return eventId;
                    }
                    eventIdCache.put(field, NOT_FOUND);
                    miss("eventId");
                    return null;
                });
    }

    public CompletableFuture<String> getEventAttrColumn(String eventId, String attrId) {
        ensureInitialized();
        
        if (eventId == null || attrId == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = eventId + "_" + attrId;
        String cached = eventAttrColumnCache.getIfPresent(field);
        if (cached != null) {
            hit("eventAttrColumn");
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(EVENT_ATTR_COLUMN_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        eventAttrColumnCache.put(field, value);
                        l2Hit("eventAttrColumn");
                    } else {
                        miss("eventAttrColumn");
                    }
                    return value;
                });
    }

    /**
     * 获取事件属性列索引 (从 cus1 解析为 1)
     */
    public CompletableFuture<Integer> getEventAttrColumnIndex(String eventId, String attrId) {
        return getEventAttrColumn(eventId, attrId)
                .thenApply(column -> {
                    if (column != null && column.startsWith("cus")) {
                        try {
                            return Integer.parseInt(column.substring(3));
                        } catch (NumberFormatException ignored) {}
                    }
                    return -1;
                });
    }

    public CompletableFuture<Boolean> isCdpEnabled(Integer appId) {
        ensureInitialized();
        
        if (appId == null) {
            return CompletableFuture.completedFuture(false);
        }
        
        String field = String.valueOf(appId);
        String cached = cdpConfigCache.getIfPresent(field);
        if (cached != null) {
            return CompletableFuture.completedFuture(!cached.isEmpty());
        }

        return kvrocksClient.asyncHGet(OPEN_CDP_APPID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        cdpConfigCache.put(field, value);
                        return !value.isEmpty();
                    }
                    cdpConfigCache.put(field, "");
                    return false;
                });
    }

    // ===================== 统计 =====================

    private void hit(String name) {
        if (l1Hits != null) {
            l1Hits.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    private void l2Hit(String name) {
        if (l2Hits != null) {
            l2Hits.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    private void miss(String name) {
        if (misses != null) {
            misses.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
        }
    }

    public KvrocksClient getKvrocksClient() {
        ensureInitialized();
        return kvrocksClient;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public String getStats() {
        return String.format("ConfigCacheService: version=%d, L1=%s, L2=%s, Miss=%s",
                currentVersion, l1Hits, l2Hits, misses);
    }

    public void close() {
        LOG.info("Closing ConfigCacheService - {}", getStats());
        if (kvrocksClient != null) {
            kvrocksClient.shutdown();
        }
        if (initialized != null) {
            initialized.set(false);
        }
    }
}
