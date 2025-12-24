package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.lock.LockManager;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件处理算子
 * 
 * ✅ 核心功能:
 * 1. 获取或创建事件ID
 * 2. 处理事件黑名单过滤
 * 3. 事件平台校验
 * 4. 自动创建新事件 (使用分布式锁防止并发冲突)
 * 5. 集成 OperatorMetrics 监控
 * 
 * ✅ 分布式锁机制:
 * - 创建新事件时使用 KVRocks 分布式锁
 * - 锁的 key: dlock:event:{appId}_{owner}_{eventName}
 * - 防止多个并行实例同时创建相同事件
 * 
 * Hash结构:
 * - event:{appId} field={owner}_{eventName} value={eventId}
 * - black_event field={appId}_{eventId} value=1
 * - create_event_forbid field={appId} value=1
 * - event_platform field={eventId}_{platform} value=1
 */
public class EventAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EventAsyncOperator.class);

    // Hash Key 常量
    private static final String EVENT_HASH_PREFIX = "event:";
    private static final String BLACK_EVENT_HASH = "black_event";
    private static final String CREATE_EVENT_FORBID_HASH = "create_event_forbid";
    private static final String EVENT_PLATFORM_HASH = "event_platform";
    private static final String AUTO_CREATE_DISABLED_HASH = "auto_create_disabled";

    // 最大事件名长度
    private static final int MAX_EVENT_NAME_LENGTH = 100;
    // 每个应用最大事件数
    private static final int MAX_EVENT_PER_APP = 10000;
    // 锁等待时间 (毫秒)
    private static final long LOCK_WAIT_MS = 3000L;
    // 锁过期时间 (毫秒)
    private static final long LOCK_EXPIRE_MS = 10000L;

    private transient ConfigCacheService configCacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient OperatorMetrics metrics;
    private transient LockManager lockManager;

    // 本地缓存
    private transient Cache<String, Integer> eventIdCache;
    private transient Cache<String, Boolean> blackEventCache;
    private transient Cache<String, Boolean> createEventForbidCache;
    private transient Cache<String, Boolean> autoCreateDisabledCache;
    private transient Cache<String, Boolean> eventPlatformCache;
    private transient Cache<String, Integer> appEventCountCache;

    // 统计
    private transient AtomicLong cacheHitCount;
    private transient AtomicLong cacheMissCount;
    private transient AtomicLong newEventCount;
    private transient AtomicLong lockWaitCount;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final Properties dbProperties;
    private final int maxEventNameLength;
    private final int metricsInterval;

    public EventAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, null, MAX_EVENT_NAME_LENGTH, 60);
    }

    public EventAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster,
                               Properties dbProperties, int maxEventNameLength) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, dbProperties, maxEventNameLength, 60);
    }

    public EventAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster,
                               Properties dbProperties, int maxEventNameLength, int metricsInterval) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.dbProperties = dbProperties;
        this.maxEventNameLength = maxEventNameLength;
        this.metricsInterval = metricsInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 Metrics
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "event_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .build();

        configCacheService = CacheServiceFactory.getConfigService("event", cacheConfig);
        kvrocksClient = configCacheService.getKvrocksClient();

        // 初始化分布式锁管理器
        lockManager = new LockManager(kvrocksClient);

        initLocalCaches();

        if (dbProperties != null && dbProperties.containsKey("rdbms.url")) {
            initDataSource();
        }

        cacheHitCount = new AtomicLong(0);
        cacheMissCount = new AtomicLong(0);
        newEventCount = new AtomicLong(0);
        lockWaitCount = new AtomicLong(0);

        LOG.info("[EventAsyncOperator-{}] 初始化成功 (分布式锁已启用)", 
                getRuntimeContext().getIndexOfThisSubtask());
    }

    private void initLocalCaches() {
        eventIdCache = Caffeine.newBuilder()
                .maximumSize(200000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .recordStats()
                .build();

        blackEventCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();

        createEventForbidCache = Caffeine.newBuilder()
                .maximumSize(5000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        autoCreateDisabledCache = Caffeine.newBuilder()
                .maximumSize(5000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        eventPlatformCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();

        appEventCountCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
    }

    private void initDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbProperties.getProperty("rdbms.url"));
        config.setUsername(dbProperties.getProperty("rdbms.userName"));
        config.setPassword(dbProperties.getProperty("rdbms.password"));
        config.setDriverClassName(dbProperties.getProperty("rdbms.driverClass", "com.mysql.cj.jdbc.Driver"));
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000L);
        config.setIdleTimeout(600000L);
        config.setMaxLifetime(1800000L);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("Event-MySQL-Pool-" + getRuntimeContext().getIndexOfThisSubtask());

        dataSource = new HikariDataSource(config);
        LOG.info("Event MySQL连接池初始化成功");
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        metrics.in();

        if (input.getResult() == -1) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        try {
            JSONObject data = (JSONObject) input.getData();
            if (data == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            Integer appId = input.getAppId();
            String owner = data.getString("owner");
            Integer sdk = input.getSdk();

            if (appId == null || appId == 0 || owner == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            JSONArray eventArray = data.getJSONArray("ea");
            if (eventArray == null || eventArray.isEmpty()) {
                metrics.out();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 处理事件列表
            processEvents(input, data, appId, owner, sdk, eventArray, resultFuture);

        } catch (Exception e) {
            LOG.error("[EventAsyncOperator] 处理失败", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 处理事件列表
     */
    private void processEvents(ZGMessage input, JSONObject data, Integer appId, String owner,
                               Integer sdk, JSONArray eventArray, ResultFuture<ZGMessage> resultFuture) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < eventArray.size(); i++) {
            JSONObject event = eventArray.getJSONObject(i);
            String eventName = event.getString("en");

            if (eventName == null || eventName.isEmpty()) {
                continue;
            }

            // 截断过长的事件名
            if (eventName.length() > maxEventNameLength) {
                eventName = eventName.substring(0, maxEventNameLength);
                event.put("en", eventName);
            }

            String finalEventName = eventName;
            CompletableFuture<Void> future = getOrCreateEventId(appId, owner, finalEventName)
                    .thenCompose(eventId -> {
                        if (eventId == null) {
                            event.put("result", -1);
                            event.put("errCode", ErrorMessageEnum.EVENT_NOT_FOUND.getCode());
                            return CompletableFuture.completedFuture(null);
                        }

                        event.put("ei", eventId);

                        // 检查黑名单
                        return isBlackEvent(appId, eventId)
                                .thenCompose(isBlack -> {
                                    if (isBlack) {
                                        event.put("result", -1);
                                        event.put("errCode", ErrorMessageEnum.EVENT_BLACK.getCode());
                                        return CompletableFuture.completedFuture(null);
                                    }

                                    // 检查平台
                                    return checkEventPlatform(eventId, sdk)
                                            .thenAccept(platformOk -> {
                                                if (!platformOk) {
                                                    event.put("platformMismatch", true);
                                                }
                                            });
                                });
                    });

            futures.add(future);
        }

        // 等待所有事件处理完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.error("[EventAsyncOperator] 处理事件列表失败", ex);
                        metrics.error();
                    } else {
                        metrics.out();
                    }
                    resultFuture.complete(Collections.singleton(input));
                });
    }

    /**
     * 获取或创建事件ID
     */
    private CompletableFuture<Integer> getOrCreateEventId(Integer appId, String owner, String eventName) {
        String cacheKey = appId + "_" + owner + "_" + eventName;

        // 1. 先查本地缓存
        Integer cached = eventIdCache.getIfPresent(cacheKey);
        if (cached != null) {
            cacheHitCount.incrementAndGet();
            return CompletableFuture.completedFuture(cached);
        }

        cacheMissCount.incrementAndGet();

        // 2. 查 KVRocks
        String hashKey = EVENT_HASH_PREFIX + appId;
        String field = owner + "_" + eventName;

        return kvrocksClient.asyncHGet(hashKey, field)
                .thenCompose(value -> {
                    if (value != null) {
                        try {
                            Integer eventId = Integer.parseInt(value);
                            eventIdCache.put(cacheKey, eventId);
                            return CompletableFuture.completedFuture(eventId);
                        } catch (NumberFormatException e) {
                            LOG.warn("事件ID格式错误: {}", value);
                        }
                    }

                    // 3. 检查是否禁止自动创建
                    return isAutoCreateDisabled(appId)
                            .thenCompose(disabled -> {
                                if (disabled) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                // 4. 检查是否禁止创建事件
                                return isCreateEventForbid(appId)
                                        .thenCompose(forbid -> {
                                            if (forbid) {
                                                return CompletableFuture.completedFuture(null);
                                            }

                                            // 5. 检查事件数量限制
                                            return getAppEventCount(appId)
                                                    .thenCompose(count -> {
                                                        if (count >= MAX_EVENT_PER_APP) {
                                                            LOG.warn("应用事件数量超限: appId={}, count={}", 
                                                                    appId, count);
                                                            return CompletableFuture.completedFuture(null);
                                                        }

                                                        // 6. 使用分布式锁创建事件
                                                        return createEventWithLock(appId, owner, eventName);
                                                    });
                                        });
                            });
                });
    }

    /**
     * 使用分布式锁创建事件
     * 
     * 流程:
     * 1. 获取分布式锁
     * 2. 双重检查 (再次查询KVRocks，可能其他实例已创建)
     * 3. 写入MySQL
     * 4. 同步到KVRocks
     * 5. 释放锁
     */
    private CompletableFuture<Integer> createEventWithLock(Integer appId, String owner, String eventName) {
        if (dataSource == null) {
            LOG.warn("数据库连接池未初始化，无法创建事件");
            return CompletableFuture.completedFuture(null);
        }

        String lockKey = LockManager.eventLockKey(appId, owner, eventName);
        lockWaitCount.incrementAndGet();

        return lockManager.executeWithLockAsync(lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS, () -> {
            // 双重检查: 获取锁后再次检查KVRocks (可能其他实例已创建)
            String hashKey = EVENT_HASH_PREFIX + appId;
            String field = owner + "_" + eventName;

            return kvrocksClient.asyncHGet(hashKey, field)
                    .thenCompose(existingValue -> {
                        if (existingValue != null) {
                            try {
                                Integer existingId = Integer.parseInt(existingValue);
                                String cacheKey = appId + "_" + owner + "_" + eventName;
                                eventIdCache.put(cacheKey, existingId);
                                LOG.debug("事件已被其他实例创建: appId={}, eventName={}, eventId={}",
                                        appId, eventName, existingId);
                                return CompletableFuture.completedFuture(existingId);
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                        }

                        // 真正创建事件
                        return CompletableFuture.supplyAsync(() -> 
                                createEventInDb(appId, owner, eventName));
                    });
        });
    }

    /**
     * 在数据库中创建事件 (同步方法，在锁内执行)
     */
    private Integer createEventInDb(Integer appId, String owner, String eventName) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // 再次查询数据库 (防止KVRocks和MySQL不一致)
            String selectSql = "SELECT id FROM event WHERE app_id = ? AND event_name = ? AND owner = ?";
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setInt(1, appId);
                stmt.setString(2, eventName);
                stmt.setString(3, owner);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int eventId = rs.getInt("id");
                    String cacheKey = appId + "_" + owner + "_" + eventName;
                    eventIdCache.put(cacheKey, eventId);
                    
                    // 同步到 KVRocks
                    syncEventToKVRocks(appId, owner, eventName, eventId);
                    return eventId;
                }
            }

            // 插入新事件
            String insertSql = "INSERT INTO event(app_id, event_name, owner, is_delete, is_stop) " +
                    "VALUES(?, ?, ?, 0, 0)";
            try (PreparedStatement stmt = conn.prepareStatement(insertSql, 
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                stmt.setInt(1, appId);
                stmt.setString(2, eventName);
                stmt.setString(3, owner);
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    int eventId = rs.getInt(1);
                    String cacheKey = appId + "_" + owner + "_" + eventName;
                    eventIdCache.put(cacheKey, eventId);
                    
                    // 同步到 KVRocks
                    syncEventToKVRocks(appId, owner, eventName, eventId);
                    
                    newEventCount.incrementAndGet();
                    LOG.info("创建事件成功: appId={}, eventName={}, eventId={}", 
                            appId, eventName, eventId);
                    return eventId;
                }
            }

            return null;

        } catch (Exception e) {
            LOG.error("创建事件失败: appId={}, eventName={}", appId, eventName, e);
            return null;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    LOG.error("关闭连接失败", e);
                }
            }
        }
    }

    /**
     * 同步事件到 KVRocks
     */
    private void syncEventToKVRocks(Integer appId, String owner, String eventName, Integer eventId) {
        String hashKey = EVENT_HASH_PREFIX + appId;
        String field = owner + "_" + eventName;
        kvrocksClient.asyncHSet(hashKey, field, String.valueOf(eventId))
                .exceptionally(ex -> {
                    LOG.warn("同步事件到KVRocks失败: appId={}, eventName={}", appId, eventName, ex);
                    return null;
                });
    }

    /**
     * 检查是否在黑名单
     */
    private CompletableFuture<Boolean> isBlackEvent(Integer appId, Integer eventId) {
        String cacheKey = appId + "_" + eventId;
        Boolean cached = blackEventCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(BLACK_EVENT_HASH, cacheKey)
                .thenApply(value -> {
                    boolean isBlack = value != null && "1".equals(value);
                    blackEventCache.put(cacheKey, isBlack);
                    return isBlack;
                });
    }

    /**
     * 检查是否禁止创建事件
     */
    private CompletableFuture<Boolean> isCreateEventForbid(Integer appId) {
        String cacheKey = String.valueOf(appId);
        Boolean cached = createEventForbidCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(CREATE_EVENT_FORBID_HASH, cacheKey)
                .thenApply(value -> {
                    boolean forbid = value != null && "1".equals(value);
                    createEventForbidCache.put(cacheKey, forbid);
                    return forbid;
                });
    }

    /**
     * 检查是否禁用自动创建
     */
    private CompletableFuture<Boolean> isAutoCreateDisabled(Integer appId) {
        String cacheKey = String.valueOf(appId);
        Boolean cached = autoCreateDisabledCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(AUTO_CREATE_DISABLED_HASH, cacheKey)
                .thenApply(value -> {
                    boolean disabled = value != null && "1".equals(value);
                    autoCreateDisabledCache.put(cacheKey, disabled);
                    return disabled;
                });
    }

    /**
     * 检查事件平台
     */
    private CompletableFuture<Boolean> checkEventPlatform(Integer eventId, Integer sdk) {
        String cacheKey = eventId + "_" + sdk;
        Boolean cached = eventPlatformCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(EVENT_PLATFORM_HASH, cacheKey)
                .thenApply(value -> {
                    boolean platformOk = value != null && "1".equals(value);
                    eventPlatformCache.put(cacheKey, platformOk);
                    return platformOk;
                });
    }

    /**
     * 获取应用事件数量
     */
    private CompletableFuture<Integer> getAppEventCount(Integer appId) {
        String cacheKey = String.valueOf(appId);
        Integer cached = appEventCountCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        String hashKey = EVENT_HASH_PREFIX + appId;
        return KvrocksClient.asyncHLen(kvrocksClient, hashKey)
                .thenApply(count -> {
                    int countInt = count != null ? count.intValue() : 0;
                    appEventCountCache.put(cacheKey, countInt);
                    return countInt;
                });
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[EventAsyncOperator] 处理超时");
        metrics.error();
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        if (metrics != null) {
            metrics.shutdown();
        }
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("MySQL连接池已关闭");
        }

        LOG.info("[EventAsyncOperator-{}] 关闭, stats: cacheHit={}, cacheMiss={}, newEvent={}, lockWait={}",
                getRuntimeContext().getIndexOfThisSubtask(),
                cacheHitCount.get(), cacheMissCount.get(), newEventCount.get(), lockWaitCount.get());
    }
}
