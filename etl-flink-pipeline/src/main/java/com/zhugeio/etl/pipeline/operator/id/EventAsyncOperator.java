package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheKeyConstants;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件处理算子 - 改造版
 * 
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 3. 黑名单查询从 Hash 改为 Set (asyncSIsMember)
 * 
 * KVRocks Key 对照 (与同步服务一致):
 * - appIdEventIdMap (Hash): field=appId_owner_eventName
 * - blackEventIdSet (Set): member=eventId
 * - appIdCreateEventForbidSet (Set): member=appId
 * - appIdNoneAutoCreateSet (Set): member=appId
 * - eventIdPlatform (Set): member=eventId_platform
 */
public class EventAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EventAsyncOperator.class);

    private static final int MAX_EVENT_NAME_LENGTH = 100;
    private static final int MAX_EVENT_PER_APP = 10000;
    private static final long LOCK_WAIT_MS = 3000L;
    private static final long LOCK_EXPIRE_MS = 10000L;

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient OperatorMetrics metrics;
    private transient LockManager lockManager;

    // 统计
    private transient AtomicLong newEventCount;
    private transient AtomicLong lockWaitCount;

    private CacheConfig cacheConfig;
    private final Properties dbProperties;
    private final int maxEventNameLength;
    private final int metricsInterval;

    public EventAsyncOperator(CacheConfig cacheConfig) {
        this(cacheConfig, null, MAX_EVENT_NAME_LENGTH, 60);
    }

    public EventAsyncOperator(CacheConfig cacheConfig, Properties dbProperties, int maxEventNameLength) {
        this(cacheConfig, dbProperties, maxEventNameLength, 60);
    }

    public EventAsyncOperator(CacheConfig cacheConfig, Properties dbProperties, 
                               int maxEventNameLength, int metricsInterval) {
        this.cacheConfig = cacheConfig;
        this.dbProperties = dbProperties;
        this.maxEventNameLength = maxEventNameLength;
        this.metricsInterval = metricsInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "event_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        // 获取统一缓存服务 (单例)
        cacheService = CacheServiceFactory.getInstance(cacheConfig);
        kvrocksClient = cacheService.getKvrocksClient();

        lockManager = new LockManager(kvrocksClient);

        if (dbProperties != null && dbProperties.containsKey("rdbms.url")) {
            initDataSource();
        }

        newEventCount = new AtomicLong(0);
        lockWaitCount = new AtomicLong(0);

        LOG.info("[EventAsyncOperator-{}] 初始化成功 (使用统一缓存服务)", 
                getRuntimeContext().getIndexOfThisSubtask());
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

            processEvents(input, data, appId, owner, sdk, eventArray, resultFuture);

        } catch (Exception e) {
            LOG.error("[EventAsyncOperator] 处理失败", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    private void processEvents(ZGMessage input, JSONObject data, Integer appId, String owner,
                               Integer sdk, JSONArray eventArray, ResultFuture<ZGMessage> resultFuture) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < eventArray.size(); i++) {
            JSONObject event = eventArray.getJSONObject(i);
            String eventName = event.getString("en");

            if (eventName == null || eventName.isEmpty()) {
                continue;
            }

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

                        // 使用统一缓存服务检查黑名单
                        return cacheService.isBlackEvent(eventId)
                                .thenCompose(isBlack -> {
                                    if (isBlack) {
                                        event.put("result", -1);
                                        event.put("errCode", ErrorMessageEnum.EVENT_BLACK.getCode());
                                        return CompletableFuture.completedFuture(null);
                                    }

                                    // 使用统一缓存服务检查平台
                                    return cacheService.checkEventPlatform(eventId, sdk)
                                            .thenAccept(platformOk -> {
                                                if (!platformOk) {
                                                    event.put("platformMismatch", true);
                                                }
                                            });
                                });
                    });

            futures.add(future);
        }

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

    private CompletableFuture<Integer> getOrCreateEventId(Integer appId, String owner, String eventName) {
        // 使用统一缓存服务获取事件ID
        return cacheService.getEventId(appId, owner, eventName)
                .thenCompose(eventId -> {
                    if (eventId != null) {
                        return CompletableFuture.completedFuture(eventId);
                    }

                    // 检查是否禁用自动创建
                    return cacheService.isAutoCreateDisabled(appId)
                            .thenCompose(disabled -> {
                                if (disabled) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                // 检查是否禁止创建事件
                                return cacheService.isCreateEventForbid(appId)
                                        .thenCompose(forbid -> {
                                            if (forbid) {
                                                return CompletableFuture.completedFuture(null);
                                            }

                                            // 使用分布式锁创建事件
                                            return createEventWithLock(appId, owner, eventName);
                                        });
                            });
                });
    }

    private CompletableFuture<Integer> createEventWithLock(Integer appId, String owner, String eventName) {
        if (dataSource == null) {
            LOG.warn("数据库连接池未初始化，无法创建事件");
            return CompletableFuture.completedFuture(null);
        }

        String lockKey = LockManager.eventLockKey(appId, owner, eventName);
        lockWaitCount.incrementAndGet();

        return lockManager.executeWithLockAsync(lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS, () -> {
            // 双重检查
            return cacheService.getEventId(appId, owner, eventName)
                    .thenCompose(existingId -> {
                        if (existingId != null) {
                            LOG.debug("事件已被其他实例创建: appId={}, eventName={}, eventId={}",
                                    appId, eventName, existingId);
                            return CompletableFuture.completedFuture(existingId);
                        }

                        return CompletableFuture.supplyAsync(() -> 
                                createEventInDb(appId, owner, eventName));
                    });
        });
    }

    private Integer createEventInDb(Integer appId, String owner, String eventName) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // 再次查询数据库
            String selectSql = "SELECT id FROM event WHERE app_id = ? AND event_name = ? AND owner = ?";
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setInt(1, appId);
                stmt.setString(2, eventName);
                stmt.setString(3, owner);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int eventId = rs.getInt("id");
                    
                    // 更新统一缓存
                    cacheService.putEventIdCache(appId, owner, eventName, eventId);
                    
                    // 同步到 KVRocks (使用正确的 Key 格式)
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
                    
                    // 更新统一缓存
                    cacheService.putEventIdCache(appId, owner, eventName, eventId);
                    
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
                try { conn.close(); } catch (Exception ignored) {}
            }
        }
    }

    /**
     * 同步事件到 KVRocks (使用与同步服务一致的 Key 格式)
     */
    private void syncEventToKVRocks(Integer appId, String owner, String eventName, Integer eventId) {
        // Key: appIdEventIdMap, Field: appId_owner_eventName
        String actualKey = cacheConfig.isKvrocksCluster()
                ? "{" + CacheKeyConstants.APP_ID_EVENT_ID_MAP + "}"
                : CacheKeyConstants.APP_ID_EVENT_ID_MAP;
        String field = CacheKeyConstants.eventIdField(appId, owner, eventName);
        
        kvrocksClient.asyncHSet(actualKey, field, String.valueOf(eventId))
                .exceptionally(ex -> {
                    LOG.warn("同步事件到KVRocks失败: appId={}, eventName={}", appId, eventName, ex);
                    return null;
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

        LOG.info("[EventAsyncOperator-{}] 关闭, newEvent={}, lockWait={}",
                getRuntimeContext().getIndexOfThisSubtask(),
                newEventCount.get(), lockWaitCount.get());
    }
}
