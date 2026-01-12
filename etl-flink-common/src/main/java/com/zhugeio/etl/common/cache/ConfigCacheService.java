package com.zhugeio.etl.common.cache;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 统一配置缓存服务 - 与 cache-sync 同步服务完全兼容版
 *
 * 重要: 本类的 Key/Field 格式必须与 MySQL→KVRocks 同步服务完全一致
 *
 * 功能:
 * 1. 两级缓存: L1 Caffeine (AsyncLoadingCache) + L2 KVRocks
 * 2. 版本检测: 自动感知同步服务的配置变更
 * 3. 异步刷新: refreshAfterWrite 不阻塞请求
 * 4. 定时全量刷新: HGETALL/SMEMBERS 批量拉取，多线程并行
 * 5. 负缓存: 避免重复查询不存在的 key
 */
public class ConfigCacheService implements Serializable {

    private static final long serialVersionUID = 4L;
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCacheService.class);

    // ===================== 刷新配置 =====================

    /**
     * 刷新配置类
     */
    public static class RefreshConfig implements Serializable {
        private static final long serialVersionUID = 1L;

        private int parallelism = 4;              // 并行线程数
        private int refreshIntervalMinutes = 10;  // 刷新间隔(分钟)
        private int timeoutSeconds = 120;         // 单个缓存刷新超时(秒)
        private boolean enabled = true;           // 是否启用定时刷新

        public static RefreshConfig defaultConfig() {
            return new RefreshConfig();
        }

        public RefreshConfig parallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public RefreshConfig refreshIntervalMinutes(int minutes) {
            this.refreshIntervalMinutes = minutes;
            return this;
        }

        public RefreshConfig timeoutSeconds(int seconds) {
            this.timeoutSeconds = seconds;
            return this;
        }

        public RefreshConfig enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public int getParallelism() { return parallelism; }
        public int getRefreshIntervalMinutes() { return refreshIntervalMinutes; }
        public int getTimeoutSeconds() { return timeoutSeconds; }
        public boolean isEnabled() { return enabled; }
    }

    // ===================== 负缓存包装类 =====================

    /**
     * 包装类，用于区分"找到值"和"确认不存在"
     * - value != null: 找到了值
     * - value == null 且 CacheValue 存在: 确认不存在（负缓存）
     */
    private static class CacheValue<T> {
        private final T value;

        private CacheValue(T value) {
            this.value = value;
        }

        static <T> CacheValue<T> of(T value) {
            return new CacheValue<>(value);
        }

        static <T> CacheValue<T> notFound() {
            return new CacheValue<>(null);
        }

        T getValue() {
            return value;
        }

        boolean isPresent() {
            return value != null;
        }
    }

    // ===================== 配置 =====================

    private final CacheConfig config;
    private final RefreshConfig refreshConfig;

    // ===================== 初始化状态 =====================

    private transient volatile AtomicBoolean initialized = new AtomicBoolean(false);
    private transient KvrocksClient kvrocksClient;

    // 是否集群模式 (影响 Key 处理)
    private transient boolean clusterMode;

    // ===================== 线程池 =====================

    // 缓存异步加载线程池
    private transient Executor cacheExecutor;
    // 定时刷新调度器
    private transient ScheduledExecutorService scheduler;
    // 刷新任务线程池
    private transient ExecutorService refreshPool;

    // ===================== L1 本地缓存 (AsyncLoadingCache) =====================

    // 应用相关
    private transient AsyncLoadingCache<String, CacheValue<Integer>> appKeyToIdCache;          // appKey -> appId
    private transient AsyncLoadingCache<Integer, CacheValue<Integer>> appIdToCompanyIdCache;   // appId -> companyId
    private transient AsyncLoadingCache<String, CacheValue<Integer>> appSdkHasDataCache;       // appId_platform -> hasData

    // 事件相关
    private transient AsyncLoadingCache<String, CacheValue<Integer>> eventIdCache;             // appId_owner_eventName -> eventId
    private transient AsyncLoadingCache<Integer, Boolean> blackEventCache;                     // eventId -> isBlack
    private transient AsyncLoadingCache<Integer, Boolean> createEventForbidCache;              // appId -> forbid
    private transient AsyncLoadingCache<Integer, Boolean> autoCreateDisabledCache;             // appId -> disabled
    private transient AsyncLoadingCache<String, Boolean> eventPlatformCache;
    private transient AsyncLoadingCache<String, Boolean> eventAttrdPlatformCache;
    private transient AsyncLoadingCache<String, CacheValue<String>> eventLastUpdateTimeCache; // eventId -> lastUpdateTime
    private transient AsyncLoadingCache<String, CacheValue<String>> eventCurrentProcessTimeCache; // eventId -> currentProcessTime

    // 事件属性相关
    private transient AsyncLoadingCache<String, CacheValue<Integer>> eventAttrIdCache;         // appId_eventId_owner_attrName -> attrId
    private transient AsyncLoadingCache<String, CacheValue<String>> eventAttrColumnCache;      // eventId_attrId -> columnName
    private transient AsyncLoadingCache<Integer, Boolean> blackEventAttrCache;                 // attrId -> isBlack
    private transient AsyncLoadingCache<Integer, Boolean> createAttrForbidCache;               // eventId -> forbid
    private transient AsyncLoadingCache<String, CacheValue<String>> eventAttrAliasCache;       // appId_owner_eventName_attrName -> alias

    // 用户属性相关
    private transient AsyncLoadingCache<String, CacheValue<Integer>> userPropIdCache;          // appId_owner_name(大写) -> propId
    private transient AsyncLoadingCache<String, CacheValue<String>> userPropOriginalCache;     // appId_owner_propId -> originalName
    private transient AsyncLoadingCache<Integer, Boolean> blackUserPropCache;                  // propId -> isBlack

    // 设备属性相关
    private transient AsyncLoadingCache<String, CacheValue<Integer>> devicePropIdCache;        // appId_owner_propName -> propId
    private transient AsyncLoadingCache<String, Boolean> devicePropPlatformCache;              // propId_platform -> valid

    // 虚拟事件/属性相关
    private transient AsyncLoadingCache<Integer, Boolean> virtualEventAppCache;                // appId -> hasVirtualEvent
    private transient AsyncLoadingCache<Integer, Boolean> virtualPropAppCache;                 // appId -> hasVirtualProp
    private transient AsyncLoadingCache<String, CacheValue<String>> virtualEventCache;         // appId_owner_eventName -> JSON
    private transient AsyncLoadingCache<String, CacheValue<String>> virtualEventAttrCache;     // field -> JSON
    private transient AsyncLoadingCache<String, CacheValue<String>> virtualEventPropCache;     // appId_eP_eventName -> JSON
    private transient AsyncLoadingCache<Integer, CacheValue<String>> virtualUserPropCache;     // appId -> JSON
    private transient AsyncLoadingCache<Integer, Boolean> eventVirtualAttrCache;               // attrId -> isVirtual

    // CDP配置
    private transient AsyncLoadingCache<Integer, CacheValue<String>> cdpConfigCache;           // appId -> config

    // ===================== 版本控制 =====================

    private transient volatile long currentVersion = 0;
    private transient volatile long lastVersionCheckTime = 0;
    private static final long VERSION_CHECK_INTERVAL_MS = 5000;

    // ===================== 定时同步任务 =====================

    private transient ScheduledExecutorService eventProcessTimeSyncScheduler;
    private static final long EVENT_PROCESS_TIME_SYNC_INTERVAL_SECONDS = 30; // 30秒同步一次

    // ===================== 构造函数 =====================

    public ConfigCacheService(CacheConfig config) {
        this(config, RefreshConfig.defaultConfig());
    }

    public ConfigCacheService(CacheConfig config, RefreshConfig refreshConfig) {
        this.config = config;
        this.refreshConfig = refreshConfig;
    }

    // ===================== 序列化支持 =====================

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.initialized = new AtomicBoolean(false);
        LOG.info("ConfigCacheService 反序列化完成，准备重新初始化");
        init();
    }

    // ===================== 初始化 =====================

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

            initExecutors();
            initL1Caches();
            loadVersion();

            // 启动定时刷新
            if (refreshConfig.isEnabled()) {
                startScheduledRefresh();
            }

            // 启动事件处理时间同步定时任务
            startEventProcessTimeSync();

            LOG.info("ConfigCacheService 初始化完成, version={}, clusterMode={}, refreshEnabled={}, parallelism={}",
                    currentVersion, clusterMode, refreshConfig.isEnabled(), refreshConfig.getParallelism());

            // 异步预热
            warmUpAsync();

        } catch (Exception e) {
            initialized.set(false);
            throw new RuntimeException("ConfigCacheService 初始化失败", e);
        }
    }

    private void initExecutors() {
        // 缓存异步加载线程池
        cacheExecutor = new ForkJoinPool(
                Runtime.getRuntime().availableProcessors(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, true
        );

        // 刷新线程池
        refreshPool = Executors.newFixedThreadPool(
                refreshConfig.getParallelism(),
                new ThreadFactory() {
                    private int count = 0;
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "cache-refresh-" + count++);
                        t.setDaemon(true);
                        return t;
                    }
                }
        );
    }

    private void startScheduledRefresh() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-refresh-scheduler");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(
                this::refreshAllCachesParallel,
                refreshConfig.getRefreshIntervalMinutes(),
                refreshConfig.getRefreshIntervalMinutes(),
                TimeUnit.MINUTES
        );

        LOG.info("定时刷新已启动，间隔 {} 分钟，并行度 {}",
                refreshConfig.getRefreshIntervalMinutes(), refreshConfig.getParallelism());
    }

    private void startEventProcessTimeSync() {
        eventProcessTimeSyncScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-process-time-sync");
            t.setDaemon(true);
            return t;
        });

        eventProcessTimeSyncScheduler.scheduleAtFixedRate(
                this::syncEventProcessTimeToKv,
                EVENT_PROCESS_TIME_SYNC_INTERVAL_SECONDS,
                EVENT_PROCESS_TIME_SYNC_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        LOG.info("事件处理时间同步定时任务已启动，间隔 {} 秒", EVENT_PROCESS_TIME_SYNC_INTERVAL_SECONDS);
    }

    /**
     * 同步事件处理时间到KV
     */
    private void syncEventProcessTimeToKv() {
        try {
            LOG.debug("开始同步事件处理时间到KV");

            // 从本地缓存获取所有事件处理时间
            Map<String, CacheValue<String>> cachedValues = eventCurrentProcessTimeCache.synchronous().asMap();

            if (cachedValues.isEmpty()) {
                LOG.debug("没有需要同步的事件处理时间");
                return;
            }

            // 准备批量更新到KV
            String actualKey = getActualKey(CacheKeyConstants.EVENT_CURRENT_PROCESS_TIME_MAP);
            
            // 收集需要同步的数据
            Map<String, String> syncData = new HashMap<>();
            for (Map.Entry<String, CacheValue<String>> entry : cachedValues.entrySet()) {
                String eventId = entry.getKey();
                CacheValue<String> cacheValue = entry.getValue();
                
                if (cacheValue != null && cacheValue.isPresent()) {
                    String processTime = cacheValue.getValue();
                    syncData.put(eventId, processTime);
                }
            }

            if (!syncData.isEmpty()) {
                // 批量更新到KV
                kvrocksClient.asyncHMSet(actualKey, syncData)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                LOG.warn("批量同步事件处理时间到KVRocks失败", ex);
                            } else {
                                LOG.debug("批量同步事件处理时间到KVRocks成功: count={}", syncData.size());
                            }
                        });
            }

            LOG.debug("事件处理时间同步到KV完成，共处理 {} 个事件", syncData.size());
        } catch (Exception e) {
            LOG.error("同步事件处理时间到KV时发生异常", e);
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

    // ===================== 缓存初始化 =====================

    private void initL1Caches() {
        // 应用 (相对稳定)
        appKeyToIdCache = buildHashCacheWithNegative(10000, 10, 30,
                CacheKeyConstants.APP_KEY_APP_ID_MAP, this::parseInteger);
        appIdToCompanyIdCache = buildHashCacheIntKeyWithNegative(10000, 10, 30,
                CacheKeyConstants.CID_BY_AID_MAP, this::parseInteger);
        appSdkHasDataCache = buildHashCacheWithNegative(50000, 10, 30,
                CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP, this::parseInteger);

        // 事件
        eventIdCache = buildHashCacheWithNegative(200000, 30, 90,
                CacheKeyConstants.APP_ID_EVENT_ID_MAP, this::parseInteger);
        blackEventCache = buildSetCacheIntKey(10000, 30, 90,
                CacheKeyConstants.BLACK_EVENT_ID_SET);
        createEventForbidCache = buildSetCacheIntKey(5000, 10, 30,
                CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET);
        autoCreateDisabledCache = buildSetCacheIntKey(10000, 10, 30,
                CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET);
        eventPlatformCache = buildSetCache(100000, 30, 90,
                CacheKeyConstants.EVENT_ID_PLATFORM);
        eventAttrdPlatformCache = buildSetCache(500000, 30, 90,
                CacheKeyConstants.EVENT_ATTR_PLATFORM);
        eventLastUpdateTimeCache = buildHashCacheStrWithNegative(500000, 60, 180,
                CacheKeyConstants.EVENT_LAST_UPDATE_TIME_MAP);
        eventCurrentProcessTimeCache = buildHashCacheStrWithNegative(500000, 60, 180,
                CacheKeyConstants.EVENT_CURRENT_PROCESS_TIME_MAP);

        // 事件属性 (量最大)
        eventAttrIdCache = buildHashCacheWithNegative(500000, 30, 90,
                CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP, this::parseInteger);
        eventAttrColumnCache = buildHashCacheStrWithNegative(500000, 60, 180,
                CacheKeyConstants.EVENT_ATTR_COLUMN_MAP);
        blackEventAttrCache = buildSetCacheIntKey(10000, 30, 90,
                CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET);
        createAttrForbidCache = buildSetCacheIntKey(50000, 10, 30,
                CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET);
        eventAttrAliasCache = buildHashCacheStrWithNegative(100000, 30, 90,
                CacheKeyConstants.EVENT_ATTR_ALIAS_MAP);

        // 用户属性
        userPropIdCache = buildHashCacheWithNegative(100000, 30, 90,
                CacheKeyConstants.APP_ID_PROP_ID_MAP, this::parseInteger);
        userPropOriginalCache = buildHashCacheStrWithNegative(100000, 30, 90,
                CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP);
        blackUserPropCache = buildSetCacheIntKey(10000, 30, 90,
                CacheKeyConstants.BLACK_USER_PROP_SET);

        // 设备属性
        devicePropIdCache = buildHashCacheWithNegative(50000, 30, 90,
                CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP, this::parseInteger);
        devicePropPlatformCache = buildSetCache(50000, 30, 90,
                CacheKeyConstants.DEVICE_PROP_PLATFORM);

        // 虚拟
        virtualEventAppCache = buildSetCacheIntKey(5000, 5, 15,
                CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET);
        virtualPropAppCache = buildSetCacheIntKey(5000, 5, 15,
                CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET);
        virtualEventCache = buildHashCacheStrWithNegative(10000, 5, 15,
                CacheKeyConstants.VIRTUAL_EVENT_MAP);
        virtualEventAttrCache = buildHashCacheStrWithNegative(10000, 5, 15,
                CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP);
        virtualEventPropCache = buildHashCacheStrWithNegative(10000, 5, 15,
                CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP);
        virtualUserPropCache = buildHashCacheIntKeyStrWithNegative(5000, 5, 15,
                CacheKeyConstants.VIRTUAL_USER_PROP_MAP);
        eventVirtualAttrCache = buildSetCacheIntKey(50000, 30, 90,
                CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET);

        // CDP
        cdpConfigCache = buildHashCacheIntKeyStrWithNegative(5000, 60, 180,
                CacheKeyConstants.OPEN_CDP_APPID_MAP);
    }

    // ===================== 缓存构建方法 =====================

    /**
     * 构建 Hash 类型缓存 (String Key, 带负缓存, 值需要解析)
     */
    private <V> AsyncLoadingCache<String, CacheValue<V>> buildHashCacheWithNegative(
            int maxSize, int refreshMinutes, int expireMinutes,
            String kvrocksKey, Function<String, V> parser) {

        String actualKey = getActualKey(kvrocksKey);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .executor(cacheExecutor)
                .recordStats()
                .buildAsync((field, executor) ->
                        kvrocksClient.asyncHGet(actualKey, field)
                                .thenApply(value -> {
                                    if (value != null) {
                                        V parsed = parser.apply(value);
                                        return parsed != null ? CacheValue.of(parsed) : CacheValue.notFound();
                                    }
                                    return CacheValue.notFound();
                                })
                );
    }

    /**
     * 构建 Hash 类型缓存 (Integer Key, 带负缓存, 值需要解析)
     */
    private <V> AsyncLoadingCache<Integer, CacheValue<V>> buildHashCacheIntKeyWithNegative(
            int maxSize, int refreshMinutes, int expireMinutes,
            String kvrocksKey, Function<String, V> parser) {

        String actualKey = getActualKey(kvrocksKey);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .executor(cacheExecutor)
                .recordStats()
                .buildAsync((key, executor) ->
                        kvrocksClient.asyncHGet(actualKey, String.valueOf(key))
                                .thenApply(value -> {
                                    if (value != null) {
                                        V parsed = parser.apply(value);
                                        return parsed != null ? CacheValue.of(parsed) : CacheValue.notFound();
                                    }
                                    return CacheValue.notFound();
                                })
                );
    }

    /**
     * 构建 Hash 类型缓存 (String Key, 带负缓存, 值为 String)
     */
    private AsyncLoadingCache<String, CacheValue<String>> buildHashCacheStrWithNegative(
            int maxSize, int refreshMinutes, int expireMinutes, String kvrocksKey) {

        String actualKey = getActualKey(kvrocksKey);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .executor(cacheExecutor)
                .recordStats()
                .buildAsync((field, executor) ->
                        kvrocksClient.asyncHGet(actualKey, field)
                                .thenApply(value -> value != null ? CacheValue.of(value) : CacheValue.notFound())
                );
    }

    /**
     * 构建 Hash 类型缓存 (Integer Key, 带负缓存, 值为 String)
     */
    private AsyncLoadingCache<Integer, CacheValue<String>> buildHashCacheIntKeyStrWithNegative(
            int maxSize, int refreshMinutes, int expireMinutes, String kvrocksKey) {

        String actualKey = getActualKey(kvrocksKey);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .executor(cacheExecutor)
                .recordStats()
                .buildAsync((key, executor) ->
                        kvrocksClient.asyncHGet(actualKey, String.valueOf(key))
                                .thenApply(value -> value != null ? CacheValue.of(value) : CacheValue.notFound())
                );
    }

    /**
     * 构建 Set 类型缓存 (String Key)
     */
    private AsyncLoadingCache<String, Boolean> buildSetCache(
            int maxSize, int refreshMinutes, int expireMinutes, String kvrocksKey) {

        String actualKey = getActualKey(kvrocksKey);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .executor(cacheExecutor)
                .recordStats()
                .buildAsync((member, executor) -> kvrocksClient.asyncSIsMember(actualKey, member));
    }

    /**
     * 构建 Set 类型缓存 (Integer Key)
     */
    private AsyncLoadingCache<Integer, Boolean> buildSetCacheIntKey(
            int maxSize, int refreshMinutes, int expireMinutes, String kvrocksKey) {

        String actualKey = getActualKey(kvrocksKey);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .refreshAfterWrite(refreshMinutes, TimeUnit.MINUTES)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .executor(cacheExecutor)
                .recordStats()
                .buildAsync((key, executor) -> kvrocksClient.asyncSIsMember(actualKey, String.valueOf(key)));
    }

    private Integer parseInteger(String value) {
        if (value == null) return null;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid integer format: {}", value);
            return null;
        }
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
        invalidateCache(appKeyToIdCache);
        invalidateCache(appIdToCompanyIdCache);
        invalidateCache(appSdkHasDataCache);
        invalidateCache(eventIdCache);
        invalidateCache(blackEventCache);
        invalidateCache(createEventForbidCache);
        invalidateCache(autoCreateDisabledCache);
        invalidateCache(eventPlatformCache);
        invalidateCache(eventAttrdPlatformCache);
        invalidateCache(eventAttrIdCache);
        invalidateCache(eventAttrColumnCache);
        invalidateCache(blackEventAttrCache);
        invalidateCache(createAttrForbidCache);
        invalidateCache(eventAttrAliasCache);
        invalidateCache(userPropIdCache);
        invalidateCache(userPropOriginalCache);
        invalidateCache(blackUserPropCache);
        invalidateCache(devicePropIdCache);
        invalidateCache(devicePropPlatformCache);
        invalidateCache(virtualEventAppCache);
        invalidateCache(virtualPropAppCache);
        invalidateCache(virtualEventCache);
        invalidateCache(virtualEventAttrCache);
        invalidateCache(virtualEventPropCache);
        invalidateCache(virtualUserPropCache);
        invalidateCache(eventVirtualAttrCache);
        invalidateCache(cdpConfigCache);
    }

    private void invalidateCache(AsyncLoadingCache<?, ?> cache) {
        if (cache != null) {
            cache.synchronous().invalidateAll();
        }
    }

    // ===================== 异步预热 =====================

    /**
     * 异步全量预热 - 不阻塞启动
     */
    public void warmUpAsync() {
        CompletableFuture.runAsync(() -> {
            LOG.info("开始异步全量预热缓存...");
            refreshAllCachesParallel();
        }, refreshPool);
    }

    // ===================== 并行刷新 =====================

    /**
     * 刷新任务
     */
    private static class RefreshTask {
        final String name;
        final Supplier<Integer> action;

        RefreshTask(String name, Supplier<Integer> action) {
            this.name = name;
            this.action = action;
        }
    }

    /**
     * 刷新结果
     */
    private static class RefreshResult {
        final String name;
        final int count;
        final long costMs;
        final Exception error;

        RefreshResult(String name, int count, long costMs, Exception error) {
            this.name = name;
            this.count = count;
            this.costMs = costMs;
            this.error = error;
        }
    }

    /**
     * 并行刷新所有缓存
     */
    private void refreshAllCachesParallel() {
        LOG.info("开始并行刷新缓存，parallelism={}", refreshConfig.getParallelism());
        long start = System.currentTimeMillis();

        // 定义所有刷新任务
        List<RefreshTask> tasks = Arrays.asList(
                // Hash 类型
                new RefreshTask("appKeyToId", () -> refreshHashCacheStr(
                        CacheKeyConstants.APP_KEY_APP_ID_MAP, appKeyToIdCache, this::parseInteger)),
                new RefreshTask("appIdToCompanyId", () -> refreshHashCacheInt(
                        CacheKeyConstants.CID_BY_AID_MAP, appIdToCompanyIdCache, this::parseInteger)),
                new RefreshTask("appSdkHasData", () -> refreshHashCacheStr(
                        CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP, appSdkHasDataCache, this::parseInteger)),
                new RefreshTask("eventId", () -> refreshHashCacheStr(
                        CacheKeyConstants.APP_ID_EVENT_ID_MAP, eventIdCache, this::parseInteger)),
                new RefreshTask("eventAttrId", () -> refreshHashCacheStr(
                        CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP, eventAttrIdCache, this::parseInteger)),
                new RefreshTask("eventAttrColumn", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.EVENT_ATTR_COLUMN_MAP, eventAttrColumnCache)),
                new RefreshTask("eventAttrAlias", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.EVENT_ATTR_ALIAS_MAP, eventAttrAliasCache)),
                new RefreshTask("userPropId", () -> refreshHashCacheStr(
                        CacheKeyConstants.APP_ID_PROP_ID_MAP, userPropIdCache, this::parseInteger)),
                new RefreshTask("userPropOriginal", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP, userPropOriginalCache)),
                new RefreshTask("devicePropId", () -> refreshHashCacheStr(
                        CacheKeyConstants.APP_ID_DEVICE_PROP_ID_MAP, devicePropIdCache, this::parseInteger)),
                new RefreshTask("cdpConfig", () -> refreshHashCacheIntStrValue(
                        CacheKeyConstants.OPEN_CDP_APPID_MAP, cdpConfigCache)),
                new RefreshTask("virtualEvent", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.VIRTUAL_EVENT_MAP, virtualEventCache)),
                new RefreshTask("virtualEventAttr", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.VIRTUAL_EVENT_ATTR_MAP, virtualEventAttrCache)),
                new RefreshTask("virtualEventProp", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.VIRTUAL_EVENT_PROP_MAP, virtualEventPropCache)),
                new RefreshTask("virtualUserProp", () -> refreshHashCacheIntStrValue(
                        CacheKeyConstants.VIRTUAL_USER_PROP_MAP, virtualUserPropCache)),
                new RefreshTask("eventCurrentProcessTime", () -> refreshHashCacheStrValue(
                        CacheKeyConstants.EVENT_CURRENT_PROCESS_TIME_MAP, eventCurrentProcessTimeCache)),

                // Set 类型
                new RefreshTask("blackEvent", () -> refreshSetCacheInt(
                        CacheKeyConstants.BLACK_EVENT_ID_SET, blackEventCache)),
                new RefreshTask("blackEventAttr", () -> refreshSetCacheInt(
                        CacheKeyConstants.BLACK_EVENT_ATTR_ID_SET, blackEventAttrCache)),
                new RefreshTask("blackUserProp", () -> refreshSetCacheInt(
                        CacheKeyConstants.BLACK_USER_PROP_SET, blackUserPropCache)),
                new RefreshTask("createEventForbid", () -> refreshSetCacheInt(
                        CacheKeyConstants.APP_ID_CREATE_EVENT_FORBID_SET, createEventForbidCache)),
                new RefreshTask("autoCreateDisabled", () -> refreshSetCacheInt(
                        CacheKeyConstants.APP_ID_NONE_AUTO_CREATE_SET, autoCreateDisabledCache)),
                new RefreshTask("createAttrForbid", () -> refreshSetCacheInt(
                        CacheKeyConstants.EVENT_ID_CREATE_ATTR_FORBIDDEN_SET, createAttrForbidCache)),
                new RefreshTask("eventPlatform", () -> refreshSetCacheStr(
                        CacheKeyConstants.EVENT_ID_PLATFORM, eventPlatformCache)),
                new RefreshTask("eventAttrdPlatform", () -> refreshSetCacheStr(
                        CacheKeyConstants.EVENT_ATTR_PLATFORM, eventAttrdPlatformCache)),
                new RefreshTask("devicePropPlatform", () -> refreshSetCacheStr(
                        CacheKeyConstants.DEVICE_PROP_PLATFORM, devicePropPlatformCache)),
                new RefreshTask("virtualEventApp", () -> refreshSetCacheInt(
                        CacheKeyConstants.VIRTUAL_EVENT_APPIDS_SET, virtualEventAppCache)),
                new RefreshTask("virtualPropApp", () -> refreshSetCacheInt(
                        CacheKeyConstants.VIRTUAL_PROP_APP_IDS_SET, virtualPropAppCache)),
                new RefreshTask("eventVirtualAttr", () -> refreshSetCacheInt(
                        CacheKeyConstants.EVENT_VIRTUAL_ATTR_IDS_SET, eventVirtualAttrCache))
        );

        // 并行执行
        List<CompletableFuture<RefreshResult>> futures = tasks.stream()
                .map(task -> CompletableFuture.supplyAsync(() -> {
                    long taskStart = System.currentTimeMillis();
                    try {
                        int count = task.action.get();
                        return new RefreshResult(task.name, count, System.currentTimeMillis() - taskStart, null);
                    } catch (Exception e) {
                        return new RefreshResult(task.name, 0, System.currentTimeMillis() - taskStart, e);
                    }
                }, refreshPool))
                .collect(Collectors.toList());

        // 等待所有完成
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(refreshConfig.getTimeoutSeconds() * 2L, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.warn("部分刷新任务超时", e);
        }

        // 统计结果
        int totalCount = 0, successCount = 0;
        for (CompletableFuture<RefreshResult> future : futures) {
            try {
                RefreshResult result = future.getNow(null);
                if (result != null && result.error == null) {
                    totalCount += result.count;
                    successCount++;
                    LOG.trace("刷新 {} 成功: {} 条, 耗时 {} ms", result.name, result.count, result.costMs);
                } else if (result != null) {
                    LOG.warn("刷新 {} 失败: {}", result.name, result.error.getMessage());
                }
            } catch (Exception ignored) {}
        }

        long totalCost = System.currentTimeMillis() - start;
        LOG.info("并行刷新完成，成功 {}/{} 个缓存，共 {} 条数据，总耗时 {} ms",
                successCount, tasks.size(), totalCount, totalCost);
    }

    // ===================== 刷新方法 (HGETALL / SMEMBERS) =====================

    /**
     * 刷新 Hash 缓存 (String Key, 值需要解析)
     */
    private <V> int refreshHashCacheStr(String kvrocksKey,
                                        AsyncLoadingCache<String, CacheValue<V>> cache,
                                        Function<String, V> parser) {
        if (cache == null) return 0;
        String actualKey = getActualKey(kvrocksKey);

        try {
            Map<String, String> all = kvrocksClient.asyncHGetAll(actualKey)
                    .get(refreshConfig.getTimeoutSeconds(), TimeUnit.SECONDS);
            if (all == null || all.isEmpty()) return 0;

            int count = 0;
            for (Map.Entry<String, String> entry : all.entrySet()) {
                V value = parser.apply(entry.getValue());
                if (value != null) {
                    cache.synchronous().put(entry.getKey(), CacheValue.of(value));
                    count++;
                }
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException("刷新 " + kvrocksKey + " 失败", e);
        }
    }

    /**
     * 刷新 Hash 缓存 (Integer Key, 值需要解析)
     */
    private <V> int refreshHashCacheInt(String kvrocksKey,
                                        AsyncLoadingCache<Integer, CacheValue<V>> cache,
                                        Function<String, V> parser) {
        if (cache == null) return 0;
        String actualKey = getActualKey(kvrocksKey);

        try {
            Map<String, String> all = kvrocksClient.asyncHGetAll(actualKey)
                    .get(refreshConfig.getTimeoutSeconds(), TimeUnit.SECONDS);
            if (all == null || all.isEmpty()) return 0;

            int count = 0;
            for (Map.Entry<String, String> entry : all.entrySet()) {
                try {
                    Integer key = Integer.parseInt(entry.getKey());
                    V value = parser.apply(entry.getValue());
                    if (value != null) {
                        cache.synchronous().put(key, CacheValue.of(value));
                        count++;
                    }
                } catch (NumberFormatException ignored) {}
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException("刷新 " + kvrocksKey + " 失败", e);
        }
    }

    /**
     * 刷新 Hash 缓存 (String Key, 值为 String)
     */
    private int refreshHashCacheStrValue(String kvrocksKey,
                                         AsyncLoadingCache<String, CacheValue<String>> cache) {
        if (cache == null) return 0;
        String actualKey = getActualKey(kvrocksKey);

        try {
            Map<String, String> all = kvrocksClient.asyncHGetAll(actualKey)
                    .get(refreshConfig.getTimeoutSeconds(), TimeUnit.SECONDS);
            if (all == null || all.isEmpty()) return 0;

            for (Map.Entry<String, String> entry : all.entrySet()) {
                cache.synchronous().put(entry.getKey(), CacheValue.of(entry.getValue()));
            }
            return all.size();
        } catch (Exception e) {
            throw new RuntimeException("刷新 " + kvrocksKey + " 失败", e);
        }
    }

    /**
     * 刷新 Hash 缓存 (Integer Key, 值为 String)
     */
    private int refreshHashCacheIntStrValue(String kvrocksKey,
                                            AsyncLoadingCache<Integer, CacheValue<String>> cache) {
        if (cache == null) return 0;
        String actualKey = getActualKey(kvrocksKey);

        try {
            Map<String, String> all = kvrocksClient.asyncHGetAll(actualKey)
                    .get(refreshConfig.getTimeoutSeconds(), TimeUnit.SECONDS);
            if (all == null || all.isEmpty()) return 0;

            int count = 0;
            for (Map.Entry<String, String> entry : all.entrySet()) {
                try {
                    Integer key = Integer.parseInt(entry.getKey());
                    cache.synchronous().put(key, CacheValue.of(entry.getValue()));
                    count++;
                } catch (NumberFormatException ignored) {}
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException("刷新 " + kvrocksKey + " 失败", e);
        }
    }

    /**
     * 刷新 Set 缓存 (Integer Key)
     */
    private int refreshSetCacheInt(String kvrocksKey, AsyncLoadingCache<Integer, Boolean> cache) {
        if (cache == null) return 0;
        String actualKey = getActualKey(kvrocksKey);

        try {
            Set<String> members = kvrocksClient.asyncSMembers(actualKey)
                    .get(refreshConfig.getTimeoutSeconds(), TimeUnit.SECONDS);

            // 先把已缓存的设为 false
            for (Integer key : cache.synchronous().asMap().keySet()) {
                cache.synchronous().put(key, false);
            }

            if (members == null || members.isEmpty()) return 0;

            int count = 0;
            for (String member : members) {
                try {
                    cache.synchronous().put(Integer.parseInt(member), true);
                    count++;
                } catch (NumberFormatException ignored) {}
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException("刷新 " + kvrocksKey + " 失败", e);
        }
    }

    /**
     * 刷新 Set 缓存 (String Key)
     */
    private int refreshSetCacheStr(String kvrocksKey, AsyncLoadingCache<String, Boolean> cache) {
        if (cache == null) return 0;
        String actualKey = getActualKey(kvrocksKey);

        try {
            Set<String> members = kvrocksClient.asyncSMembers(actualKey)
                    .get(refreshConfig.getTimeoutSeconds(), TimeUnit.SECONDS);

            // 先把已缓存的设为 false
            for (String key : cache.synchronous().asMap().keySet()) {
                cache.synchronous().put(key, false);
            }

            if (members == null || members.isEmpty()) return 0;

            for (String member : members) {
                cache.synchronous().put(member, true);
            }
            return members.size();
        } catch (Exception e) {
            throw new RuntimeException("刷新 " + kvrocksKey + " 失败", e);
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
        if (appKey == null) return CompletableFuture.completedFuture(null);
        return appKeyToIdCache.get(appKey).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 根据 appId 获取 companyId
     * KVRocks Key: cidByAidMap (Hash)
     * Field: appId
     */
    public CompletableFuture<Integer> getCompanyIdByAppId(Integer appId) {
        ensureInitialized();
        if (appId == null) return CompletableFuture.completedFuture(null);
        return appIdToCompanyIdCache.get(appId).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 检查应用平台是否有数据
     * KVRocks Key: appIdSdkHasDataMap (Hash)
     * Field: appId_platform
     */
    public CompletableFuture<Boolean> hasAppSdkData(Integer appId, Integer platform) {
        ensureInitialized();
        if (appId == null || platform == null) return CompletableFuture.completedFuture(false);
        String field = CacheKeyConstants.sdkHasDataField(appId, platform);
        LOG.debug("检查应用平台是否有数据: appId={}, platform={},field: {}", appId, platform,field);
        return appSdkHasDataCache.get(field)
                .thenApply(cv -> {
                    LOG.debug("检查应用平台是否有数据: field: {}, cv: {}", field,cv.getValue());
                    return cv != null && cv.isPresent() && cv.getValue() == 1;
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
        if (appId == null || owner == null || eventName == null) return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.eventIdField(appId, owner, eventName);
        return eventIdCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 检查事件是否在黑名单中
     * KVRocks Key: blackEventIdSet (Set)
     * Member: eventId
     */
    public CompletableFuture<Boolean> isBlackEvent(Integer eventId) {
        ensureInitialized();
        if (eventId == null) return CompletableFuture.completedFuture(false);
        return blackEventCache.get(eventId).thenApply(r -> r != null && r);
    }

    /**
     * 检查是否禁止创建事件 (已达到事件数量上限)
     * KVRocks Key: appIdCreateEventForbidSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> isCreateEventForbid(Integer appId) {
        ensureInitialized();
        if (appId == null)
            return CompletableFuture.completedFuture(false);
        return createEventForbidCache.get(appId).thenApply(r -> r != null && r);
    }

    /**
     * 检查是否禁用自动创建 (auto_event=0)
     * KVRocks Key: appIdNoneAutoCreateSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> isAutoCreateDisabled(Integer appId) {
        LOG.debug("检查是否禁用自动创建 : appId={}", appId);
        ensureInitialized();
        if (appId == null) return CompletableFuture.completedFuture(false);
        return autoCreateDisabledCache.get(appId).thenApply(r -> r != null && r);
    }

    /**
     * 检查事件平台是否有效
     * KVRocks Key: eventIdPlatform (Set)
     * Member: eventId_platform
     */
    public CompletableFuture<Boolean> checkEventPlatform(Integer eventId, Integer platform) {
        ensureInitialized();
        if (eventId == null || platform == null) return CompletableFuture.completedFuture(true);
        String member = CacheKeyConstants.eventPlatformMember(eventId, platform);
        return eventPlatformCache.get(member).thenApply(r -> r != null && r);
    }

    /**
     * 检查事件属性平台是否有效
     * KVRocks Key: eventIdPlatform (Set)
     * Member: eventId_platform
     */
    public CompletableFuture<Boolean> checkEventEttrPlatform(Integer attrId, Integer platform) {
        ensureInitialized();
        if (attrId == null || platform == null) return CompletableFuture.completedFuture(true);
        String member = CacheKeyConstants.attrIdPlatformMember(attrId, platform);
        return eventAttrdPlatformCache.get(member).thenApply(r -> r != null && r);
    }


    /**
     * 获取事件当前处理时间
     * KVRocks Key: eventCurrentProcessTimeMap (Hash)
     * Field: eventId
     * Value: currentTime
     */
    public CompletableFuture<String> getEventProcessTime(Integer eventId) {
        ensureInitialized();
        if (eventId == null) return CompletableFuture.completedFuture(null);
        String field = String.valueOf(eventId);
        return eventCurrentProcessTimeCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 仅更新事件当前处理时间到本地缓存（不立即同步到KV）
     * 用于高频更新场景，配合定时批量同步
     * 
     * @param eventId 事件ID
     */
    public void updateEventProcessTimeLocalOnly(Integer eventId) {
        if (eventId != null) {
            String field = String.valueOf(eventId);
            String currentTime = String.valueOf(System.currentTimeMillis());
            
            // 仅更新本地缓存
            eventCurrentProcessTimeCache.synchronous().put(field, CacheValue.of(currentTime));
        }
    }

    /**
     * 使用指定时间戳更新事件当前处理时间到本地缓存（不立即同步到KV）
     * 用于批量处理场景，配合定时批量同步
     *
     * @param eventId 事件ID
     * @param processTime 处理时间戳
     */
    public void updateEventProcessTimeLocalOnlyWithTime(Integer eventId, Long processTime) {
        if (eventId != null && processTime != null) {
            String field = String.valueOf(eventId);
            String timeStr = String.valueOf(processTime);
            
            // 仅更新本地缓存
            eventCurrentProcessTimeCache.synchronous().put(field, CacheValue.of(timeStr));
        }
    }

    /**
     * 更新事件当前处理时间到缓存并立即同步到KV
     * KVRocks Key: eventCurrentProcessTimeMap (Hash)
     * Field: eventId
     * Value: currentTime
     */
    public void updateEventProcessTime(Integer eventId) {
        if (eventId != null) {
            String field = String.valueOf(eventId);
            String currentTime = String.valueOf(System.currentTimeMillis());
            
            // 更新本地缓存
            eventCurrentProcessTimeCache.synchronous().put(field, CacheValue.of(currentTime));
            
            // 同步到 KVRocks
            String actualKey = getActualKey(CacheKeyConstants.EVENT_CURRENT_PROCESS_TIME_MAP);
            kvrocksClient.asyncHSet(actualKey, field, currentTime)
                    .exceptionally(ex -> {
                        LOG.warn("更新事件处理时间到KVRocks失败: eventId={}", eventId, ex);
                        return null;
                    });
        }
    }
    // ===================== 事件属性相关 (EventAttrAsyncOperator) =====================

    /**
     * 获取事件属性 ID
     * KVRocks Key: appIdEventAttrIdMap (Hash)
     * Field: appId_eventId_owner_attrName(大写)
     */
    public CompletableFuture<Integer> getEventAttrId(Integer appId, Integer eventId, String owner, String attrName) {
        ensureInitialized();
        if (appId == null || eventId == null || owner == null || attrName == null)
            return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.eventAttrIdField(appId, eventId, owner, attrName);
        return eventAttrIdCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
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
        if (eventId == null || attrId == null) return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.eventAttrColumnField(eventId, attrId);
        return eventAttrColumnCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 批量获取事件属性列索引
     *
     * 优化流程：
     * 1. 先从 L1 缓存批量查询
     * 2. 收集未命中的 keys
     * 3. 对未命中的 keys 执行 HMGET 批量查询
     * 4. 更新 L1 缓存
     *
     * @param eventId 事件ID
     * @param propIds 属性ID列表
     * @return Map<propId, columnIndex>，未找到的 propId 不在结果中
     */
    public CompletableFuture<Map<String, Integer>> batchGetEventAttrColumnIndex(
            String eventId, List<String> propIds) {

        ensureInitialized();

        if (propIds == null || propIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Map<String, Integer> result = new ConcurrentHashMap<>();
        List<String> missedPropIds = new ArrayList<>();
        List<String> missedFields = new ArrayList<>();

        // Step 1: 批量检查 L1 缓存
        for (String propId : propIds) {
            String field = CacheKeyConstants.eventAttrColumnField(eventId, propId);

            // 尝试从缓存同步获取（不触发加载）
            CacheValue<String> cached = eventAttrColumnCache.synchronous().getIfPresent(field);

            if (cached != null && cached.isPresent()) {
                // L1 缓存命中
                String column = cached.getValue();
                Integer index = parseColumnIndex(column);
                if (index != null && index > 0) {
                    result.put(propId, index);
                }
            } else if (cached != null && !cached.isPresent()) {
                // 负缓存命中（确认不存在），跳过
            } else {
                // 缓存未命中，需要查询 KVRocks
                missedPropIds.add(propId);
                missedFields.add(field);
            }
        }

        // Step 2: 如果全部命中，直接返回
        if (missedPropIds.isEmpty()) {
            return CompletableFuture.completedFuture(result);
        }

        // Step 3: 批量查询 KVRocks
        String actualKey = getActualKey(CacheKeyConstants.EVENT_ATTR_COLUMN_MAP);

        return kvrocksClient.asyncHMGet(actualKey, missedFields)
                .thenApply(values -> {
                    // Step 4: 处理结果，更新缓存
                    for (int i = 0; i < missedFields.size(); i++) {
                        String field = missedFields.get(i);
                        String propId = missedPropIds.get(i);
                        String column = values.get(i);

                        if (column != null) {
                            // 找到了，更新 L1 缓存
                            eventAttrColumnCache.synchronous().put(field, CacheValue.of(column));

                            Integer index = parseColumnIndex(column);
                            if (index != null && index > 0) {
                                result.put(propId, index);
                            }
                        } else {
                            // 未找到，设置负缓存
                            eventAttrColumnCache.synchronous().put(field, CacheValue.notFound());
                        }
                    }

                    return result;
                })
                .exceptionally(ex -> {
                    LOG.warn("批量查询事件属性列索引失败: eventId={}, count={}",
                            eventId, missedPropIds.size(), ex);
                    return result; // 返回已有的缓存命中结果
                });
    }

    /**
     * 解析列名为索引号 (cus1 -> 1, cus2 -> 2, ...)
     */
    private Integer parseColumnIndex(String column) {
        if (column != null && column.startsWith("cus")) {
            try {
                return Integer.parseInt(column.substring(3));
            } catch (NumberFormatException ignored) {}
        }
        return null;
    }

    // ===================== 新增：批量查询 CDP 状态 =====================

    /**
     * 批量检查应用是否启用 CDP
     *
     * @param appIds 应用ID列表
     * @return Map<appId, isCdpEnabled>
     */
    public CompletableFuture<Map<Integer, Boolean>> batchIsCdpEnabled(List<Integer> appIds) {
        ensureInitialized();

        if (appIds == null || appIds.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        Map<Integer, Boolean> result = new ConcurrentHashMap<>();
        List<Integer> missedAppIds = new ArrayList<>();
        List<String> missedFields = new ArrayList<>();

        // Step 1: 检查 L1 缓存
        for (Integer appId : appIds) {
            CacheValue<String> cached = cdpConfigCache.synchronous().getIfPresent(appId);

            if (cached != null) {
                // 缓存命中
                result.put(appId, cached.isPresent() && !cached.getValue().isEmpty());
            } else {
                // 缓存未命中
                missedAppIds.add(appId);
                missedFields.add(String.valueOf(appId));
            }
        }

        if (missedAppIds.isEmpty()) {
            return CompletableFuture.completedFuture(result);
        }

        // Step 2: 批量查询 KVRocks
        String actualKey = getActualKey(CacheKeyConstants.OPEN_CDP_APPID_MAP);

        return kvrocksClient.asyncHMGet(actualKey, missedFields)
                .thenApply(values -> {
                    for (int i = 0; i < missedAppIds.size(); i++) {
                        Integer appId = missedAppIds.get(i);
                        String value = values.get(i);

                        if (value != null && !value.isEmpty()) {
                            cdpConfigCache.synchronous().put(appId, CacheValue.of(value));
                            result.put(appId, true);
                        } else {
                            cdpConfigCache.synchronous().put(appId, CacheValue.notFound());
                            result.put(appId, false);
                        }
                    }
                    return result;
                })
                .exceptionally(ex -> {
                    LOG.warn("批量查询CDP状态失败: count={}", missedAppIds.size(), ex);
                    // 未查到的默认为 false
                    for (Integer appId : missedAppIds) {
                        result.putIfAbsent(appId, false);
                    }
                    return result;
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
        if (attrId == null) return CompletableFuture.completedFuture(false);
        return blackEventAttrCache.get(attrId).thenApply(r -> r != null && r);
    }

    /**
     * 检查是否禁止创建属性 (已达到属性数量上限)
     * KVRocks Key: eventIdCreateAttrForbiddenSet (Set)
     * Member: eventId
     */
    public CompletableFuture<Boolean> isCreateAttrForbid(Integer eventId) {
        ensureInitialized();
        if (eventId == null) return CompletableFuture.completedFuture(false);
        return createAttrForbidCache.get(eventId).thenApply(r -> r != null && r);
    }

    /**
     * 获取事件属性别名
     * KVRocks Key: eventAttrAliasMap (Hash)
     * Field: appId_owner_eventName_attrName
     */
    public CompletableFuture<String> getEventAttrAlias(Integer appId, String owner, String eventName, String attrName) {
        ensureInitialized();
        if (appId == null || owner == null || eventName == null || attrName == null)
            return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.eventAttrAliasField(appId, owner, eventName, attrName);
        return eventAttrAliasCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    // ===================== 用户属性相关 (UserPropAsyncOperator) =====================

    /**
     * 获取用户属性 ID
     * KVRocks Key: appIdPropIdMap (Hash)
     * Field: appId_owner_name(大写)
     */
    public CompletableFuture<Integer> getUserPropId(Integer appId, String owner, String propName) {
        ensureInitialized();
        if (appId == null || owner == null || propName == null) return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.userPropIdField(appId, owner, propName);
        return userPropIdCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 获取用户属性原始名称
     * KVRocks Key: appIdPropIdOriginalMap (Hash)
     * Field: appId_owner_propId
     */
    public CompletableFuture<String> getUserPropOriginalName(Integer appId, String owner, Integer propId) {
        ensureInitialized();
        if (appId == null || owner == null || propId == null) return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.userPropOriginalField(appId, owner, propId);
        return userPropOriginalCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 检查用户属性是否在黑名单中
     * KVRocks Key: blackUserPropSet (Set)
     * Member: propId
     */
    public CompletableFuture<Boolean> isBlackUserProp(Integer propId) {
        ensureInitialized();
        if (propId == null) return CompletableFuture.completedFuture(false);
        return blackUserPropCache.get(propId).thenApply(r -> r != null && r);
    }

    // ===================== 设备属性相关 (DevicePropertyOperator) =====================

    /**
     * 获取设备属性 ID
     * KVRocks Key: appIdDevicePropIdMap (Hash)
     * Field: appId_owner_propName
     */
    public CompletableFuture<Integer> getDevicePropId(Integer appId, String owner, String propName) {
        ensureInitialized();
        if (appId == null || owner == null || propName == null) return CompletableFuture.completedFuture(null);
        String field = CacheKeyConstants.devicePropIdField(appId, owner, propName);
        return devicePropIdCache.get(field).thenApply(cv -> cv != null ? cv.getValue() : null);
    }

    /**
     * 检查设备属性平台是否有效
     * KVRocks Key: devicePropPlatform (Set)
     * Member: propId_platform
     */
    public CompletableFuture<Boolean> checkDevicePropPlatform(Integer propId, Integer platform) {
        ensureInitialized();
        if (propId == null || platform == null) return CompletableFuture.completedFuture(true);
        String member = CacheKeyConstants.devicePropPlatformMember(propId, platform);
        return devicePropPlatformCache.get(member).thenApply(r -> r != null && r);
    }

    // ===================== 虚拟事件/属性 (VirtualEventOperator, VirtualPropertyOperator) =====================

    /**
     * 检查应用是否有虚拟事件
     * KVRocks Key: virtualEventAppidsSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> hasVirtualEvent(Integer appId) {
        ensureInitialized();
        if (appId == null) return CompletableFuture.completedFuture(false);
        return virtualEventAppCache.get(appId).thenApply(r -> r != null && r);
    }

    /**
     * 检查应用是否有虚拟属性
     * KVRocks Key: virtualPropAppIdsSet (Set)
     * Member: appId
     */
    public CompletableFuture<Boolean> hasVirtualProp(Integer appId) {
        ensureInitialized();
        if (appId == null) return CompletableFuture.completedFuture(false);
        return virtualPropAppCache.get(appId).thenApply(r -> r != null && r);
    }

    /**
     * 获取虚拟事件列表
     * KVRocks Key: virtualEventMap (Hash)
     * Field: appId_owner_eventName
     * Value: JSON Array
     */
    public CompletableFuture<List<Map<String, Object>>> getVirtualEvents(Integer appId, String owner, String eventName) {
        ensureInitialized();
        if (appId == null || owner == null || eventName == null)
            return CompletableFuture.completedFuture(Collections.emptyList());
        String field = CacheKeyConstants.virtualEventField(appId, owner, eventName);
        return virtualEventCache.get(field)
                .thenApply(cv -> cv != null && cv.isPresent() ? parseJsonArray(cv.getValue()) : Collections.emptyList());
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
        if (appId == null || virtualEventName == null || owner == null || eventName == null)
            return CompletableFuture.completedFuture(Collections.emptySet());
        String field = CacheKeyConstants.virtualEventAttrField(appId, virtualEventName, owner, eventName);
        return virtualEventAttrCache.get(field)
                .thenApply(cv -> cv != null && cv.isPresent() ? parseJsonSet(cv.getValue()) : Collections.emptySet());
    }

    /**
     * 获取虚拟事件属性 (事件级)
     * KVRocks Key: virtualEventPropMap (Hash)
     * Field: appId_eP_eventName
     * Value: JSON Array
     */
    public CompletableFuture<List<Map<String, Object>>> getVirtualEventProps(Integer appId, String eventName) {
        ensureInitialized();
        if (appId == null || eventName == null)
            return CompletableFuture.completedFuture(Collections.emptyList());
        String field = CacheKeyConstants.virtualEventPropField(appId, eventName);
        return virtualEventPropCache.get(field)
                .thenApply(cv -> cv != null && cv.isPresent() ? parseJsonArray(cv.getValue()) : Collections.emptyList());
    }

    /**
     * 获取虚拟用户属性
     * KVRocks Key: virtualUserPropMap (Hash)
     * Field: appId
     * Value: JSON Array
     */
    public CompletableFuture<List<Map<String, Object>>> getVirtualUserProps(Integer appId) {
        ensureInitialized();
        if (appId == null) return CompletableFuture.completedFuture(Collections.emptyList());
        return virtualUserPropCache.get(appId)
                .thenApply(cv -> cv != null && cv.isPresent() ? parseJsonArray(cv.getValue()) : Collections.emptyList());
    }

    /**
     * 检查属性是否是虚拟属性
     * KVRocks Key: eventVirtualAttrIdsSet (Set)
     * Member: attrId
     */
    public CompletableFuture<Boolean> isVirtualAttr(Integer attrId) {
        ensureInitialized();
        if (attrId == null) return CompletableFuture.completedFuture(false);
        return eventVirtualAttrCache.get(attrId).thenApply(r -> r != null && r);
    }

    // ===================== CDP 配置 =====================

    /**
     * 检查应用是否启用 CDP
     * KVRocks Key: openCdpAppidMap (Hash)
     * Field: appId
     */
    public CompletableFuture<Boolean> isCdpEnabled(Integer appId) {
        ensureInitialized();
        if (appId == null) return CompletableFuture.completedFuture(false);
        return cdpConfigCache.get(appId)
                .thenApply(cv -> cv != null && cv.isPresent() && !cv.getValue().isEmpty());
    }

    // ===================== 缓存更新方法 (算子写入后同步调用) =====================

    /**
     * 更新事件平台缓存
     * KVRocks Key: eventIdPlatform (Set)
     * Member: eventId_platform
     *
     * 注意: eventPlatformCache 是基于 Set 构建的，存储 Boolean 值
     */
    public void putEventPlatformCache(Integer eventId, Integer platform) {
        if (eventPlatformCache != null && eventId != null && platform != null) {
            String member = CacheKeyConstants.eventPlatformMember(eventId, platform);
            // Set 类型缓存直接存储 Boolean，true 表示成员存在
            eventPlatformCache.synchronous().put(member, true);
        }
    }

    /**
     * 更新事件平台缓存
     * KVRocks Key: eventIdPlatform (Set)
     * Member: eventId_platform
     *
     * 注意: eventPlatformCache 是基于 Set 构建的，存储 Boolean 值
     */
    public void putEventAttrdPlatformCache(Integer eventId, Integer platform) {
        if (eventAttrdPlatformCache != null && eventId != null && platform != null) {
            String member = CacheKeyConstants.eventPlatformMember(eventId, platform);
            // Set 类型缓存直接存储 Boolean，true 表示成员存在
            eventAttrdPlatformCache.synchronous().put(member, true);
        }
    }

    /**
     * 批量更新事件平台缓存
     * @param eventPlatforms Map<eventId, Set<platform>>
     */
    public void putEventPlatformCacheBatch(Map<Integer, Set<Integer>> eventPlatforms) {
        if (eventPlatformCache != null && eventPlatforms != null) {
            for (Map.Entry<Integer, Set<Integer>> entry : eventPlatforms.entrySet()) {
                Integer eventId = entry.getKey();
                for (Integer platform : entry.getValue()) {
                    String member = CacheKeyConstants.eventPlatformMember(eventId, platform);
                    eventPlatformCache.synchronous().put(member, true);
                }
            }
        }
    }

    /**
     * 更新App SDK有数据标记缓存
     * KVRocks Key: appIdSdkHasDataMap (Hash)
     * Field: appId_platform
     * Value: 1 (有数据) 或 0 (无数据)
     */
    public void putAppSdkHasDataCache(Integer appId, Integer platform, boolean hasData) {
        if (appSdkHasDataCache != null && appId != null && platform != null) {
            String field = CacheKeyConstants.sdkHasDataField(appId, platform);
            // 有数据存1，无数据存0（但通常只会存1）
            appSdkHasDataCache.synchronous().put(field, CacheValue.of(hasData ? 1 : 0));
        }
    }

    /**
     * 更新事件ID缓存
     */
    public void putEventIdCache(Integer appId, String owner, String eventName, Integer eventId) {
        if (eventIdCache != null) {
            String field = CacheKeyConstants.eventIdField(appId, owner, eventName);
            eventIdCache.synchronous().put(field, CacheValue.of(eventId));
        }
    }

    /**
     * 更新事件属性ID缓存
     */
    public void putEventAttrIdCache(Integer appId, Integer eventId, String owner, String attrName, Integer attrId) {
        if (eventAttrIdCache != null) {
            String field = CacheKeyConstants.eventAttrIdField(appId, eventId, owner, attrName);
            eventAttrIdCache.synchronous().put(field, CacheValue.of(attrId));
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
            eventAttrColumnCache.synchronous().put(field, CacheValue.of(columnName));
        }
    }

    /**
     * 更新用户属性ID缓存
     */
    public void putUserPropIdCache(Integer appId, String owner, String propName, Integer propId) {
        if (userPropIdCache != null) {
            String field = CacheKeyConstants.userPropIdField(appId, owner, propName);
            userPropIdCache.synchronous().put(field, CacheValue.of(propId));
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

    private Set<String> parseJsonSet(String json) {
        try {
            return new HashSet<>(JSON.parseArray(json, String.class));
        } catch (Exception e) {
            LOG.warn("解析JSON Set失败: {}", json, e);
            return Collections.emptySet();
        }
    }

    // ===================== 公共方法 =====================

    public KvrocksClient getKvrocksClient() {
        ensureInitialized();
        return kvrocksClient;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * 手动触发刷新
     */
    public void manualRefresh() {
        CompletableFuture.runAsync(this::refreshAllCachesParallel, refreshPool);
    }

    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("ConfigCacheService Stats:\n");
        sb.append("  Version: ").append(currentVersion).append("\n");
        sb.append("  ClusterMode: ").append(clusterMode).append("\n");
        sb.append("  RefreshEnabled: ").append(refreshConfig.isEnabled()).append("\n");
        sb.append("  Parallelism: ").append(refreshConfig.getParallelism()).append("\n");
        sb.append("  RefreshInterval: ").append(refreshConfig.getRefreshIntervalMinutes()).append(" min\n");

        // Caffeine 内置统计
        if (eventIdCache != null) {
            sb.append("  EventIdCache: ").append(eventIdCache.synchronous().stats()).append("\n");
        }
        if (eventAttrIdCache != null) {
            sb.append("  EventAttrIdCache: ").append(eventAttrIdCache.synchronous().stats()).append("\n");
        }
        if (appKeyToIdCache != null) {
            sb.append("  AppKeyToIdCache: ").append(appKeyToIdCache.synchronous().stats()).append("\n");
        }

        return sb.toString();
    }

    public void close() {
        LOG.info("Closing ConfigCacheService - {}", getStats());

        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }

        if (eventProcessTimeSyncScheduler != null) {
            eventProcessTimeSyncScheduler.shutdown();
            try {
                if (!eventProcessTimeSyncScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    eventProcessTimeSyncScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                eventProcessTimeSyncScheduler.shutdownNow();
            }
        }

        if (refreshPool != null) {
            refreshPool.shutdown();
            try {
                if (!refreshPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    refreshPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                refreshPool.shutdownNow();
            }
        }

        if (cacheExecutor instanceof ForkJoinPool) {
            ((ForkJoinPool) cacheExecutor).shutdown();
        }

        if (kvrocksClient != null) {
            kvrocksClient.shutdown();
        }

        if (initialized != null) {
            initialized.set(false);
        }
    }
}