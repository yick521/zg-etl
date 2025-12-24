package com.zhugeio.etl.common.cache;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统一配置缓存服务 - 与 cache-sync 同步服务完全兼容版
 *
 * ⚠️ 重要: 本类的 Key/Field 格式必须与 MySQL→KVRocks 同步服务完全一致
 *
 * 功能:
 * 1. 两级缓存: L1 Caffeine + L2 KVRocks
 * 2. 版本检测: 自动感知同步服务的配置变更
 *
 */
public class ConfigCacheService implements Serializable {

    private static final long serialVersionUID = 3L;
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCacheService.class);

    // 缓存未找到标记
    private static final Integer NOT_FOUND_INT = -999999;
    private static final String NOT_FOUND_STR = "__NOT_FOUND__";

    // 初始化状态
    private transient volatile AtomicBoolean initialized = new AtomicBoolean(false);
    private transient KvrocksClient kvrocksClient;

    // 是否集群模式 (影响 Key 处理)
    private transient boolean clusterMode;

    // ===================== L1 本地缓存 =====================

    // 应用相关
    private transient Cache<String, Integer> appKeyToIdCache;          // appKey -> appId
    private transient Cache<Integer, Integer> appIdToCompanyIdCache;   // appId -> companyId
    private transient Cache<String, Integer> appSdkHasDataCache;       // appId_platform -> hasData

    // 事件相关
    private transient Cache<String, Integer> eventIdCache;             // appId_owner_eventName -> eventId
    private transient Cache<Integer, Boolean> blackEventCache;         // eventId -> isBlack
    private transient Cache<Integer, Boolean> createEventForbidCache;  // appId -> forbid
    private transient Cache<Integer, Boolean> autoCreateDisabledCache; // appId -> disabled
    private transient Cache<String, Boolean> eventPlatformCache;       // eventId_platform -> valid

    // 事件属性相关
    private transient Cache<String, Integer> eventAttrIdCache;         // appId_eventId_owner_attrName -> attrId
    private transient Cache<String, String> eventAttrColumnCache;      // eventId_attrId -> columnName
    private transient Cache<Integer, Boolean> blackEventAttrCache;     // attrId -> isBlack
    private transient Cache<Integer, Boolean> createAttrForbidCache;   // eventId -> forbid
    private transient Cache<String, String> eventAttrAliasCache;       // appId_owner_eventName_attrName -> alias

    // 用户属性相关
    private transient Cache<String, Integer> userPropIdCache;          // appId_owner_name(大写) -> propId
    private transient Cache<String, String> userPropOriginalCache;     // appId_owner_propId -> originalName
    private transient Cache<Integer, Boolean> blackUserPropCache;      // propId -> isBlack

    // 设备属性相关
    private transient Cache<String, Integer> devicePropIdCache;        // appId_owner_propName -> propId
    private transient Cache<String, Boolean> devicePropPlatformCache;  // propId_platform -> valid

    // 虚拟事件/属性相关
    private transient Cache<Integer, Boolean> virtualEventAppCache;    // appId -> hasVirtualEvent
    private transient Cache<Integer, Boolean> virtualPropAppCache;     // appId -> hasVirtualProp
    private transient Cache<String, String> virtualEventCache;         // appId_owner_eventName -> JSON
    private transient Cache<String, String> virtualEventAttrCache;     // field -> JSON
    private transient Cache<String, String> virtualEventPropCache;     // appId_eP_eventName -> JSON
    private transient Cache<Integer, String> virtualUserPropCache;     // appId -> JSON
    private transient Cache<Integer, Boolean> eventVirtualAttrCache;   // attrId -> isVirtual

    // CDP配置
    private transient Cache<Integer, String> cdpConfigCache;           // appId -> config

    // ===================== 版本控制 =====================

    private transient volatile long currentVersion = 0;
    private transient volatile long lastVersionCheckTime = 0;
    private static final long VERSION_CHECK_INTERVAL_MS = 5000;

    // ===================== 配置 =====================

    private final CacheConfig config;

    // ===================== 统计 =====================

    private transient Map<String, AtomicLong> l1Hits;
    private transient Map<String, AtomicLong> l2Hits;
    private transient Map<String, AtomicLong> misses;

    // ===================== 构造和初始化 =====================

    public ConfigCacheService(CacheConfig config) {
        this.config = config;
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.initialized = new AtomicBoolean(false);
        LOG.info("ConfigCacheService 反序列化完成，准备重新初始化");
        init();
    }

    public void init() {
        if (initialized == null) {
            initialized = new AtomicBoolean(false);
        }

        if (!initialized.compareAndSet(false, true)) {
            LOG.debug("ConfigCacheService 已初始化，跳过");
            return;
        }

        try {
            kvrocksClient = new KvrocksClient(
                    config.getKvrocksHost(),
                    config.getKvrocksPort(),
                    config.isKvrocksCluster()
            );
            kvrocksClient.init();

            this.clusterMode = config.isKvrocksCluster();

            if (!kvrocksClient.testConnection()) {
                initialized.set(false);
                throw new RuntimeException("KVRocks连接失败: " + config.getKvrocksHost());
            }

            initL1Caches();
            initStats();
            loadVersion();

            LOG.info("ConfigCacheService 初始化完成, version={}, clusterMode={}",
                    currentVersion, clusterMode);

        } catch (Exception e) {
            initialized.set(false);
            throw new RuntimeException("ConfigCacheService 初始化失败", e);
        }
    }

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
        // 应用 (相对稳定)
        appKeyToIdCache = buildCache(10000, 10);
        appIdToCompanyIdCache = buildCache(10000, 10);
        appSdkHasDataCache = buildCache(50000, 10);

        // 事件
        eventIdCache = buildCache(200000, 30);
        blackEventCache = buildCache(10000, 30);
        createEventForbidCache = buildCache(5000, 10);
        autoCreateDisabledCache = buildCache(10000, 10);
        eventPlatformCache = buildCache(100000, 30);

        // 事件属性 (量最大)
        eventAttrIdCache = buildCache(500000, 30);
        eventAttrColumnCache = buildCache(500000, 60);
        blackEventAttrCache = buildCache(10000, 30);
        createAttrForbidCache = buildCache(50000, 10);
        eventAttrAliasCache = buildCache(100000, 30);

        // 用户属性
        userPropIdCache = buildCache(100000, 30);
        userPropOriginalCache = buildCache(100000, 30);
        blackUserPropCache = buildCache(10000, 30);

        // 设备属性
        devicePropIdCache = buildCache(50000, 30);
        devicePropPlatformCache = buildCache(50000, 30);

        // 虚拟
        virtualEventAppCache = buildCache(5000, 5);
        virtualPropAppCache = buildCache(5000, 5);
        virtualEventCache = buildCache(10000, 5);
        virtualEventAttrCache = buildCache(10000, 5);
        virtualEventPropCache = buildCache(10000, 5);
        virtualUserPropCache = buildCache(5000, 5);
        eventVirtualAttrCache = buildCache(50000, 30);

        // CDP
        cdpConfigCache = buildCache(5000, 60);
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

    // ===================== Key 处理 (集群模式 Hash Tag) =====================

    /**
     * 获取实际的 KVRocks Key (集群模式加 Hash Tag)
     */
    private String getActualKey(String key) {
        return clusterMode ? "{" + key + "}" : key;
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
            String versionStr = kvrocksClient.asyncGet(CacheKeyConstants.SYNC_VERSION)
                    .get(1, TimeUnit.SECONDS);
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

        kvrocksClient.asyncGet(CacheKeyConstants.SYNC_VERSION)
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
            String versionStr = kvrocksClient.asyncGet(CacheKeyConstants.SYNC_VERSION)
                    .get(5, TimeUnit.SECONDS);
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
        if (blackEventCache != null) {
            blackEventCache.invalidateAll();
        }
        if (createEventForbidCache != null) {
            createEventForbidCache.invalidateAll();
        }
        if (autoCreateDisabledCache != null) {
            autoCreateDisabledCache.invalidateAll();
        }
        if (eventPlatformCache != null) {
            eventPlatformCache.invalidateAll();
        }
        if (eventAttrIdCache != null) {
            eventAttrIdCache.invalidateAll();
        }
        if (eventAttrColumnCache != null) {
            eventAttrColumnCache.invalidateAll();
        }
        if (blackEventAttrCache != null) {
            blackEventAttrCache.invalidateAll();
        }
        if (createAttrForbidCache != null) {
            createAttrForbidCache.invalidateAll();
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
        if (blackUserPropCache != null) {
            blackUserPropCache.invalidateAll();
        }
        if (devicePropIdCache != null) {
            devicePropIdCache.invalidateAll();
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
        if (cdpConfigCache != null) {
            cdpConfigCache.invalidateAll();
        }
    }

    // ===================== 应用相关 (SetAppIdAndBusinessOperator) =====================

    /**
     * 根据 appKey 获取 appId
     * KVRocks Key: appKeyAppIdMap (Hash)
     * Field: appKey
     */
    public CompletableFuture<Integer> getAppIdByAppKey(String appKey) {
        ensureInitialized();

        if (appKey == null) {
            return CompletableFuture.completedFuture(null);
        }

        Integer cached = appKeyToIdCache.getIfPresent(appKey);
        if (cached != null) {
            hit("appKey");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND_INT) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_KEY_APP_ID_MAP);
        return kvrocksClient.asyncHGet(actualKey, appKey)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Integer appId = Integer.parseInt(value);
                            appKeyToIdCache.put(appKey, appId);
                            l2Hit("appKey");
                            return appId;
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid appId format: {}", value);
                        }
                    }
                    appKeyToIdCache.put(appKey, NOT_FOUND_INT);
                    miss("appKey");
                    return null;
                });
    }

    /**
     * 根据 appId 获取 companyId
     * KVRocks Key: cidByAidMap (Hash)
     * Field: appId
     */
    public CompletableFuture<Integer> getCompanyIdByAppId(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(null);
        }

        Integer cached = appIdToCompanyIdCache.getIfPresent(appId);
        if (cached != null) {
            hit("companyId");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND_INT) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.CID_BY_AID_MAP);
        return kvrocksClient.asyncHGet(actualKey, String.valueOf(appId))
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Integer companyId = Integer.parseInt(value);
                            appIdToCompanyIdCache.put(appId, companyId);
                            l2Hit("companyId");
                            return companyId;
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid companyId format: {}", value);
                        }
                    }
                    appIdToCompanyIdCache.put(appId, NOT_FOUND_INT);
                    miss("companyId");
                    return null;
                });
    }

    /**
     * 检查应用平台是否有数据
     * KVRocks Key: appIdSdkHasDataMap (Hash)
     * Field: appId_platform
     */
    public CompletableFuture<Boolean> hasAppSdkData(Integer appId, Integer platform) {
        ensureInitialized();

        if (appId == null || platform == null) {
            return CompletableFuture.completedFuture(false);
        }

        String field = CacheKeyConstants.sdkHasDataField(appId, platform);
        Integer cached = appSdkHasDataCache.getIfPresent(field);
        if (cached != null) {
            hit("sdkHasData");
            return CompletableFuture.completedFuture(cached == 1);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    int hasData = 0;
                    if (value != null) {
                        try {
                            hasData = Integer.parseInt(value);
                        } catch (NumberFormatException ignored) {}
                    }
                    appSdkHasDataCache.put(field, hasData);
                    l2Hit("sdkHasData");
                    return hasData == 1;
                });
    }

    // ===================== 事件相关 (EventAsyncOperator) =====================

    /**
     * 获取事件 ID
     * KVRocks Key: appIdEventIdMap (Hash)
     * Field: appId_owner_eventName
     */
    public CompletableFuture<Integer> getEventId(Integer appId, String owner, String eventName) {
        ensureInitialized();

        if (appId == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.eventIdField(appId, owner, eventName);
        Integer cached = eventIdCache.getIfPresent(field);
        if (cached != null) {
            hit("eventId");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND_INT) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_EVENT_ID_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Integer eventId = Integer.parseInt(value);
                            eventIdCache.put(field, eventId);
                            l2Hit("eventId");
                            return eventId;
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid eventId format: {}", value);
                        }
                    }
                    eventIdCache.put(field, NOT_FOUND_INT);
                    miss("eventId");
                    return null;
                });
    }

    /**
     * 检查事件是否在黑名单中
     * KVRocks Key: blackEventIdSet (Set)
     * Member: eventId
     */
    public CompletableFuture<Boolean> isBlackEvent(Integer eventId) {
        ensureInitialized();

        if (eventId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = blackEventCache.getIfPresent(eventId);
        if (cached != null) {
            hit("blackEvent");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.BLACK_EVENT_ID_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(eventId))
                .thenApply(isBlack -> {
                    blackEventCache.put(eventId, isBlack);
                    if (isBlack) {
                        l2Hit("blackEvent");
                    } else {
                        miss("blackEvent");
                    }
                    return isBlack;
                });
    }

    /**
     * 检查是否禁止创建事件 (已达到事件数量上限)
     * KVRocks Key: appIdCreateEventForbidSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> isCreateEventForbid(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = createEventForbidCache.getIfPresent(appId);
        if (cached != null) {
            hit("createEventForbid");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(appId))
                .thenApply(forbid -> {
                    createEventForbidCache.put(appId, forbid);
                    if (forbid){
                        l2Hit("createEventForbid");
                    } else {
                        miss("createEventForbid");
                    }
                    return forbid;
                });
    }

    /**
     * 检查是否禁用自动创建 (auto_event=0)
     * KVRocks Key: appIdNoneAutoCreateSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> isAutoCreateDisabled(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = autoCreateDisabledCache.getIfPresent(appId);
        if (cached != null) {
            hit("autoCreateDisabled");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(appId))
                .thenApply(disabled -> {
                    autoCreateDisabledCache.put(appId, disabled);
                    if (disabled) l2Hit("autoCreateDisabled"); else miss("autoCreateDisabled");
                    return disabled;
                });
    }

    /**
     * 检查事件平台是否有效
     * KVRocks Key: eventIdPlatform (Set)
     * Member: eventId_platform
     */
    public CompletableFuture<Boolean> checkEventPlatform(Integer eventId, Integer platform) {
        ensureInitialized();

        if (eventId == null || platform == null) {
            return CompletableFuture.completedFuture(true);
        }

        String member = CacheKeyConstants.eventPlatformMember(eventId, platform);
        Boolean cached = eventPlatformCache.getIfPresent(member);
        if (cached != null) {
            hit("eventPlatform");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.EVENT_ID_PLATFORM);
        return kvrocksClient.asyncSIsMember(actualKey, member)
                .thenApply(valid -> {
                    eventPlatformCache.put(member, valid);
                    if (valid) l2Hit("eventPlatform"); else miss("eventPlatform");
                    return valid;
                });
    }

    // ===================== 事件属性相关 (EventAttrAsyncOperator) =====================

    /**
     * 获取事件属性 ID
     * KVRocks Key: appIdEventAttrIdMap (Hash)
     * Field: appId_eventId_owner_attrName(大写)
     */
    public CompletableFuture<Integer> getEventAttrId(Integer appId, Integer eventId,
                                                     String owner, String attrName) {
        ensureInitialized();

        if (appId == null || eventId == null || owner == null || attrName == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.eventAttrIdField(appId, eventId, owner, attrName);
        Integer cached = eventAttrIdCache.getIfPresent(field);
        if (cached != null) {
            hit("eventAttrId");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND_INT) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Integer attrId = Integer.parseInt(value);
                            eventAttrIdCache.put(field, attrId);
                            l2Hit("eventAttrId");
                            return attrId;
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid attrId format: {}", value);
                        }
                    }
                    eventAttrIdCache.put(field, NOT_FOUND_INT);
                    miss("eventAttrId");
                    return null;
                });
    }

    /**
     * 获取事件属性列名 (Integer版本)
     * KVRocks Key: eventAttrColumnMap (Hash)
     * Field: eventId_attrId
     */
    public CompletableFuture<String> getEventAttrColumn(Integer eventId, Integer attrId) {
        return getEventAttrColumn(String.valueOf(eventId), String.valueOf(attrId));
    }

    /**
     * 获取事件属性列名 (String版本，支持雪花ID)
     * KVRocks Key: eventAttrColumnMap (Hash)
     * Field: eventId_attrId
     */
    public CompletableFuture<String> getEventAttrColumn(String eventId, String attrId) {
        ensureInitialized();

        if (eventId == null || attrId == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.eventAttrColumnField(eventId, attrId);
        String cached = eventAttrColumnCache.getIfPresent(field);
        if (cached != null) {
            hit("eventAttrColumn");
            return CompletableFuture.completedFuture(NOT_FOUND_STR.equals(cached) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.EVENT_ATTR_COLUMN_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        eventAttrColumnCache.put(field, value);
                        l2Hit("eventAttrColumn");
                        return value;
                    }
                    eventAttrColumnCache.put(field, NOT_FOUND_STR);
                    miss("eventAttrColumn");
                    return null;
                });
    }

    /**
     * 获取事件属性列索引号 (从 cus1 解析为 1) - Integer版本
     */
    public CompletableFuture<Integer> getEventAttrColumnIndex(Integer eventId, Integer attrId) {
        return getEventAttrColumnIndex(String.valueOf(eventId), String.valueOf(attrId));
    }

    /**
     * 获取事件属性列索引号 (从 cus1 解析为 1) - String版本，支持雪花ID
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

    /**
     * 检查事件属性是否在黑名单中
     * KVRocks Key: blackEventAttrIdSet (Set)
     * Member: attrId
     */
    public CompletableFuture<Boolean> isBlackEventAttr(Integer attrId) {
        ensureInitialized();

        if (attrId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = blackEventAttrCache.getIfPresent(attrId);
        if (cached != null) {
            hit("blackEventAttr");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(attrId))
                .thenApply(isBlack -> {
                    blackEventAttrCache.put(attrId, isBlack);
                    if (isBlack) l2Hit("blackEventAttr"); else miss("blackEventAttr");
                    return isBlack;
                });
    }

    /**
     * 检查是否禁止创建属性 (已达到属性数量上限)
     * KVRocks Key: eventIdCreateAttrForbiddenSet (Set)
     * Member: eventId
     */
    public CompletableFuture<Boolean> isCreateAttrForbid(Integer eventId) {
        ensureInitialized();

        if (eventId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = createAttrForbidCache.getIfPresent(eventId);
        if (cached != null) {
            hit("createAttrForbid");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(eventId))
                .thenApply(forbid -> {
                    createAttrForbidCache.put(eventId, forbid);
                    if (forbid) l2Hit("createAttrForbid"); else miss("createAttrForbid");
                    return forbid;
                });
    }

    /**
     * 获取事件属性别名
     * KVRocks Key: eventAttrAliasMap (Hash)
     * Field: appId_owner_eventName_attrName
     */
    public CompletableFuture<String> getEventAttrAlias(Integer appId, String owner,
                                                       String eventName, String attrName) {
        ensureInitialized();

        if (appId == null || owner == null || eventName == null || attrName == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.eventAttrAliasField(appId, owner, eventName, attrName);
        String cached = eventAttrAliasCache.getIfPresent(field);
        if (cached != null) {
            hit("eventAttrAlias");
            return CompletableFuture.completedFuture(NOT_FOUND_STR.equals(cached) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.EVENT_ATTR_ALIAS_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        eventAttrAliasCache.put(field, value);
                        l2Hit("eventAttrAlias");
                        return value;
                    }
                    eventAttrAliasCache.put(field, NOT_FOUND_STR);
                    miss("eventAttrAlias");
                    return null;
                });
    }

    // ===================== 用户属性相关 (UserPropAsyncOperator) =====================

    /**
     * 获取用户属性 ID
     * KVRocks Key: appIdPropIdMap (Hash)
     * Field: appId_owner_name(大写)
     */
    public CompletableFuture<Integer> getUserPropId(Integer appId, String owner, String propName) {
        ensureInitialized();

        if (appId == null || owner == null || propName == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.userPropIdField(appId, owner, propName);
        Integer cached = userPropIdCache.getIfPresent(field);
        if (cached != null) {
            hit("userPropId");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND_INT) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_PROP_ID_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Integer propId = Integer.parseInt(value);
                            userPropIdCache.put(field, propId);
                            l2Hit("userPropId");
                            return propId;
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid propId format: {}", value);
                        }
                    }
                    userPropIdCache.put(field, NOT_FOUND_INT);
                    miss("userPropId");
                    return null;
                });
    }

    /**
     * 获取用户属性原始名称
     * KVRocks Key: appIdPropIdOriginalMap (Hash)
     * Field: appId_owner_propId
     */
    public CompletableFuture<String> getUserPropOriginalName(Integer appId, String owner, Integer propId) {
        ensureInitialized();

        if (appId == null || owner == null || propId == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.userPropOriginalField(appId, owner, propId);
        String cached = userPropOriginalCache.getIfPresent(field);
        if (cached != null) {
            hit("userPropOriginal");
            return CompletableFuture.completedFuture(NOT_FOUND_STR.equals(cached) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        userPropOriginalCache.put(field, value);
                        l2Hit("userPropOriginal");
                        return value;
                    }
                    userPropOriginalCache.put(field, NOT_FOUND_STR);
                    miss("userPropOriginal");
                    return null;
                });
    }

    /**
     * 检查用户属性是否在黑名单中
     * KVRocks Key: blackUserPropSet (Set)
     * Member: propId
     */
    public CompletableFuture<Boolean> isBlackUserProp(Integer propId) {
        ensureInitialized();

        if (propId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = blackUserPropCache.getIfPresent(propId);
        if (cached != null) {
            hit("blackUserProp");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.BLACK_USER_PROP_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(propId))
                .thenApply(isBlack -> {
                    blackUserPropCache.put(propId, isBlack);
                    if (isBlack) l2Hit("blackUserProp"); else miss("blackUserProp");
                    return isBlack;
                });
    }

    // ===================== 设备属性相关 (DevicePropertyOperator) =====================

    /**
     * 获取设备属性 ID
     * KVRocks Key: appIdDevicePropIdMap (Hash)
     * Field: appId_owner_propName
     */
    public CompletableFuture<Integer> getDevicePropId(Integer appId, String owner, String propName) {
        ensureInitialized();

        if (appId == null || owner == null || propName == null) {
            return CompletableFuture.completedFuture(null);
        }

        String field = CacheKeyConstants.devicePropIdField(appId, owner, propName);
        Integer cached = devicePropIdCache.getIfPresent(field);
        if (cached != null) {
            hit("devicePropId");
            return CompletableFuture.completedFuture(cached.equals(NOT_FOUND_INT) ? null : cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Integer propId = Integer.parseInt(value);
                            devicePropIdCache.put(field, propId);
                            l2Hit("devicePropId");
                            return propId;
                        } catch (NumberFormatException e) {
                            LOG.warn("Invalid devicePropId format: {}", value);
                        }
                    }
                    devicePropIdCache.put(field, NOT_FOUND_INT);
                    miss("devicePropId");
                    return null;
                });
    }

    /**
     * 检查设备属性平台是否有效
     * KVRocks Key: devicePropPlatform (Set)
     * Member: propId_platform
     */
    public CompletableFuture<Boolean> checkDevicePropPlatform(Integer propId, Integer platform) {
        ensureInitialized();

        if (propId == null || platform == null) {
            return CompletableFuture.completedFuture(true);
        }

        String member = CacheKeyConstants.devicePropPlatformMember(propId, platform);
        Boolean cached = devicePropPlatformCache.getIfPresent(member);
        if (cached != null) {
            hit("devicePropPlatform");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.DEVICE_PROP_PLATFORM);
        return kvrocksClient.asyncSIsMember(actualKey, member)
                .thenApply(valid -> {
                    devicePropPlatformCache.put(member, valid);
                    if (valid) l2Hit("devicePropPlatform"); else miss("devicePropPlatform");
                    return valid;
                });
    }

    // ===================== 虚拟事件/属性 (VirtualEventOperator, VirtualPropertyOperator) =====================

    /**
     * 检查应用是否有虚拟事件
     * KVRocks Key: virtualEventAppidsSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> hasVirtualEvent(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = virtualEventAppCache.getIfPresent(appId);
        if (cached != null) {
            hit("virtualEventApp");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(appId))
                .thenApply(hasVirtual -> {
                    virtualEventAppCache.put(appId, hasVirtual);
                    l2Hit("virtualEventApp");
                    return hasVirtual;
                });
    }

    /**
     * 检查应用是否有虚拟属性
     * KVRocks Key: virtualPropAppIdsSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> hasVirtualProp(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = virtualPropAppCache.getIfPresent(appId);
        if (cached != null) {
            hit("virtualPropApp");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(appId))
                .thenApply(hasVirtual -> {
                    virtualPropAppCache.put(appId, hasVirtual);
                    l2Hit("virtualPropApp");
                    return hasVirtual;
                });
    }

    /**
     * 获取虚拟事件列表
     * KVRocks Key: virtualEventMap (Hash)
     * Field: appId_owner_eventName
     * Value: JSON Array
     */
    public CompletableFuture<List<Map<String, Object>>> getVirtualEvents(Integer appId,
                                                                         String owner,
                                                                         String eventName) {
        ensureInitialized();

        if (appId == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        String field = CacheKeyConstants.virtualEventField(appId, owner, eventName);
        String cached = virtualEventCache.getIfPresent(field);
        if (cached != null) {
            hit("virtualEvent");
            if (NOT_FOUND_STR.equals(cached)) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            return CompletableFuture.completedFuture(parseJsonArray(cached));
        }

        String actualKey = getActualKey(CacheKeyConstants.VIRTUAL_EVENT_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        virtualEventCache.put(field, value);
                        l2Hit("virtualEvent");
                        return parseJsonArray(value);
                    }
                    virtualEventCache.put(field, NOT_FOUND_STR);
                    miss("virtualEvent");
                    return Collections.emptyList();
                });
    }

    /**
     * 获取虚拟事件属性集合
     * KVRocks Key: virtualEventAttrMap (Hash)
     * Field: appId_virtualEventName_owner_eventName
     * Value: JSON Set
     */
    public CompletableFuture<Set<String>> getVirtualEventAttrs(Integer appId, String virtualEventName,
                                                               String owner, String eventName) {
        ensureInitialized();

        if (appId == null || virtualEventName == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        String field = CacheKeyConstants.virtualEventAttrField(appId, virtualEventName, owner, eventName);
        String cached = virtualEventAttrCache.getIfPresent(field);
        if (cached != null) {
            hit("virtualEventAttr");
            if (NOT_FOUND_STR.equals(cached)) {
                return CompletableFuture.completedFuture(Collections.emptySet());
            }
            return CompletableFuture.completedFuture(parseJsonSet(cached));
        }

        String actualKey = getActualKey(CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        virtualEventAttrCache.put(field, value);
                        l2Hit("virtualEventAttr");
                        return parseJsonSet(value);
                    }
                    virtualEventAttrCache.put(field, NOT_FOUND_STR);
                    miss("virtualEventAttr");
                    return Collections.emptySet();
                });
    }

    /**
     * 获取虚拟事件属性 (事件级)
     * KVRocks Key: virtualEventPropMap (Hash)
     * Field: appId_eP_eventName
     * Value: JSON Array
     */
    public CompletableFuture<List<Map<String, Object>>> getVirtualEventProps(Integer appId, String eventName) {
        ensureInitialized();

        if (appId == null || eventName == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        String field = CacheKeyConstants.virtualEventPropField(appId, eventName);
        String cached = virtualEventPropCache.getIfPresent(field);
        if (cached != null) {
            hit("virtualEventProp");
            if (NOT_FOUND_STR.equals(cached)) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            return CompletableFuture.completedFuture(parseJsonArray(cached));
        }

        String actualKey = getActualKey(CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP);
        return kvrocksClient.asyncHGet(actualKey, field)
                .thenApply(value -> {
                    if (value != null) {
                        virtualEventPropCache.put(field, value);
                        l2Hit("virtualEventProp");
                        return parseJsonArray(value);
                    }
                    virtualEventPropCache.put(field, NOT_FOUND_STR);
                    miss("virtualEventProp");
                    return Collections.emptyList();
                });
    }

    /**
     * 获取虚拟用户属性
     * KVRocks Key: virtualUserPropMap (Hash)
     * Field: appId
     * Value: JSON Array
     */
    public CompletableFuture<List<Map<String, Object>>> getVirtualUserProps(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        String cached = virtualUserPropCache.getIfPresent(appId);
        if (cached != null) {
            hit("virtualUserProp");
            if (NOT_FOUND_STR.equals(cached)) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            }
            return CompletableFuture.completedFuture(parseJsonArray(cached));
        }

        String actualKey = getActualKey(CacheKeyConstants.VIRTUAL_USER_PROP_MAP);
        return kvrocksClient.asyncHGet(actualKey, String.valueOf(appId))
                .thenApply(value -> {
                    if (value != null) {
                        virtualUserPropCache.put(appId, value);
                        l2Hit("virtualUserProp");
                        return parseJsonArray(value);
                    }
                    virtualUserPropCache.put(appId, NOT_FOUND_STR);
                    miss("virtualUserProp");
                    return Collections.emptyList();
                });
    }

    /**
     * 检查属性是否是虚拟属性
     * KVRocks Key: eventVirtualAttrIdsSet (Set)
     * Member: attrId
     */
    public CompletableFuture<Boolean> isVirtualAttr(Integer attrId) {
        ensureInitialized();

        if (attrId == null) {
            return CompletableFuture.completedFuture(false);
        }

        Boolean cached = eventVirtualAttrCache.getIfPresent(attrId);
        if (cached != null) {
            hit("eventVirtualAttr");
            return CompletableFuture.completedFuture(cached);
        }

        String actualKey = getActualKey(CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET);
        return kvrocksClient.asyncSIsMember(actualKey, String.valueOf(attrId))
                .thenApply(isVirtual -> {
                    eventVirtualAttrCache.put(attrId, isVirtual);
                    l2Hit("eventVirtualAttr");
                    return isVirtual;
                });
    }

    // ===================== CDP 配置 =====================

    /**
     * 检查应用是否启用 CDP
     * KVRocks Key: openCdpAppidMap (Hash)
     * Field: appId
     */
    public CompletableFuture<Boolean> isCdpEnabled(Integer appId) {
        ensureInitialized();

        if (appId == null) {
            return CompletableFuture.completedFuture(false);
        }

        String cached = cdpConfigCache.getIfPresent(appId);
        if (cached != null) {
            hit("cdpConfig");
            return CompletableFuture.completedFuture(!NOT_FOUND_STR.equals(cached) && !cached.isEmpty());
        }

        String actualKey = getActualKey(CacheKeyConstants.OPEN_CDP_APPID_MAP);
        return kvrocksClient.asyncHGet(actualKey, String.valueOf(appId))
                .thenApply(value -> {
                    if (value != null && !value.isEmpty()) {
                        cdpConfigCache.put(appId, value);
                        l2Hit("cdpConfig");
                        return true;
                    }
                    cdpConfigCache.put(appId, NOT_FOUND_STR);
                    miss("cdpConfig");
                    return false;
                });
    }

    // ===================== 缓存更新方法 (算子写入后同步调用) =====================

    /**
     * 更新事件ID缓存
     */
    public void putEventIdCache(Integer appId, String owner, String eventName, Integer eventId) {
        if (eventIdCache != null) {
            String field = CacheKeyConstants.eventIdField(appId, owner, eventName);
            eventIdCache.put(field, eventId);
        }
    }

    /**
     * 更新事件属性ID缓存
     */
    public void putEventAttrIdCache(Integer appId, Integer eventId, String owner,
                                    String attrName, Integer attrId) {
        if (eventAttrIdCache != null) {
            String field = CacheKeyConstants.eventAttrIdField(appId, eventId, owner, attrName);
            eventAttrIdCache.put(field, attrId);
        }
    }

    /**
     * 更新事件属性列名缓存 (Integer版本)
     */
    public void putEventAttrColumnCache(Integer eventId, Integer attrId, String columnName) {
        putEventAttrColumnCache(String.valueOf(eventId), String.valueOf(attrId), columnName);
    }

    /**
     * 更新事件属性列名缓存 (String版本，支持雪花ID)
     */
    public void putEventAttrColumnCache(String eventId, String attrId, String columnName) {
        if (eventAttrColumnCache != null) {
            String field = CacheKeyConstants.eventAttrColumnField(eventId, attrId);
            eventAttrColumnCache.put(field, columnName);
        }
    }

    /**
     * 更新用户属性ID缓存
     */
    public void putUserPropIdCache(Integer appId, String owner, String propName, Integer propId) {
        if (userPropIdCache != null) {
            String field = CacheKeyConstants.userPropIdField(appId, owner, propName);
            userPropIdCache.put(field, propId);
        }
    }

    // ===================== JSON 解析辅助方法 =====================

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseJsonArray(String json) {
        try {
            JSONArray array = JSON.parseArray(json);
            List<Map<String, Object>> result = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                Object item = array.get(i);
                if (item instanceof String) {
                    // 同步服务存储的是 JSON 字符串数组
                    result.add(JSON.parseObject((String) item, Map.class));
                } else if (item instanceof Map) {
                    result.add((Map<String, Object>) item);
                }
            }
            return result;
        } catch (Exception e) {
            LOG.warn("解析JSON数组失败: {}", json, e);
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unchecked")
    private Set<String> parseJsonSet(String json) {
        try {
            return new HashSet<>(JSON.parseArray(json, String.class));
        } catch (Exception e) {
            LOG.warn("解析JSON Set失败: {}", json, e);
            return Collections.emptySet();
        }
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
        StringBuilder sb = new StringBuilder();
        sb.append("ConfigCacheService Stats:\n");
        sb.append("  Version: ").append(currentVersion).append("\n");
        sb.append("  ClusterMode: ").append(clusterMode).append("\n");
        sb.append("  L1 Hits: ").append(l1Hits).append("\n");
        sb.append("  L2 Hits: ").append(l2Hits).append("\n");
        sb.append("  Misses: ").append(misses).append("\n");
        return sb.toString();
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