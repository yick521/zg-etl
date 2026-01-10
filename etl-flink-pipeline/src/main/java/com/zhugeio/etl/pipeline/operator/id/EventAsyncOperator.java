package com.zhugeio.etl.pipeline.operator.id;

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
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事件处理算子 - 修复版
 * <p>
 * 修复点:
 * 1. 数据结构: 从 data.data 列表读取，而不是 data.ea
 * 2. 事件名: 从 pr.$eid 读取，而不是 event.en
 * 3. 写入 $zg_eid: 将 eventId 写入 pr 属性 Map
 * 4. 设置 ZGMessage.zgEid: 设置消息对象的 zgEid 字段
 * 5. owner 处理: 根据 dt 类型动态确定 owner（abp 固定用 "zg"）
 * 6. event_platform 写入: 异步写入 MySQL 并同步到 KV/缓存
 */
public class EventAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EventAsyncOperator.class);

    private static final int MAX_EVENT_NAME_LENGTH = 100;
    private static final long LOCK_WAIT_MS = 3000L;
    private static final long LOCK_EXPIRE_MS = 10000L;

    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient OperatorMetrics metrics;
    private transient LockManager lockManager;

    private transient AtomicLong newEventCount;
    private transient AtomicLong lockWaitCount;

    private final CacheConfig cacheConfig;
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

        cacheService = CacheServiceFactory.getInstance(cacheConfig);
        kvrocksClient = cacheService.getKvrocksClient();

        lockManager = new LockManager(kvrocksClient);

        if (dbProperties != null && dbProperties.containsKey("rdbms.url")) {
            initDataSource();
        }

        newEventCount = new AtomicLong(0);
        lockWaitCount = new AtomicLong(0);

        LOG.info("[EventAsyncOperator-{}] 初始化成功",
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
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        metrics.in();

        if (input.getResult() == -1) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        try {
            Map<String, Object> data = input.getData();
            if (data == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            Integer appId = input.getAppId();
            // ★ 修复：从 data 中获取 owner
            String defaultOwner = String.valueOf(data.get("owner"));
            Integer sdk = input.getSdk();

            if (appId == null || appId == 0) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // ★★★ 修复：从 data.data 读取事件列表，而不是 data.ea ★★★
            Object dataListObj = data.get("data");
            if (!(dataListObj instanceof List)) {
                metrics.out();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataListObj;
            if (dataList.isEmpty()) {
                metrics.out();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            processEvents(input, appId, defaultOwner, sdk, dataList, resultFuture);

        } catch (Exception e) {
            LOG.error("[EventAsyncOperator] 处理失败", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    @SuppressWarnings("unchecked")
    private void processEvents(ZGMessage input, Integer appId, String defaultOwner,
                               Integer sdk, List<Map<String, Object>> dataList,
                               ResultFuture<ZGMessage> resultFuture) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // 收集需要更新的事件处理时间，对相同事件ID只保留最新的时间
        Map<Integer, Long> eventProcessTimes = new ConcurrentHashMap<>();
        
        LOG.debug("data={}", dataList);
        for (Map<String, Object> finalDataItem : dataList) {
            LOG.debug("pr={}", finalDataItem);
            if (finalDataItem == null) continue;

            String dt = String.valueOf(finalDataItem.get("dt"));
            LOG.debug("dt={}", dt);
            if (!"evt".equals(dt) && !"mkt".equals(dt) && !"abp".equals(dt) && !"vtl".equals(dt)) {
                continue;
            }

            Map<String, Object> pr = (Map<String, Object>) finalDataItem.get("pr");
            if (pr == null) {
                continue;
            }

            Object eidObj = pr.get("$eid");
            LOG.debug("eid={}", eidObj);
            if (eidObj == null) {
                continue;
            }
            String eventName = String.valueOf(eidObj);
            if (eventName.isEmpty() || "null".equals(eventName)) {
                continue;
            }

            // 截断过长的事件名
            if (eventName.length() > maxEventNameLength) {
                LOG.debug("截断过长的事件名 eventName={}, 截取后:{}", eventName, eventName.substring(0, maxEventNameLength));
                eventName = eventName.substring(0, maxEventNameLength);
                pr.put("$eid", eventName);
            }

            String owner = getOwner(dt, defaultOwner);
            LOG.debug("owner={}", owner);
            String finalEventName = eventName;

            CompletableFuture<Void> future = getOrCreateEventId(appId, owner, finalEventName)
                    .thenCompose(eventId -> {
                        LOG.debug("appId={} ,owner={} ,finalEventName={} ,eventId={}", appId, owner, finalEventName, eventId);
                        if (eventId == null) {
                            // 事件ID获取失败，标记错误
                            finalDataItem.put("errorCode", ErrorMessageEnum.EVENT_NOT_FOUND.getErrorCode());
                            finalDataItem.put("errorDescribe", ErrorMessageEnum.EVENT_NOT_FOUND.getErrorMessage());
                            return CompletableFuture.completedFuture(null);
                        }

                        pr.put("$zg_eid", eventId);

                        // 注意：如果一条消息有多个事件，取第一个有效的事件ID
                        if (input.getZgEid() == null || input.getZgEid() == 0) {
                            input.setZgEid(eventId);
                        }

                        // 记录事件处理时间，如果相同事件ID已存在，则只保留时间更新的那个
                        Long currentTime = System.currentTimeMillis();
                        eventProcessTimes.compute(eventId, (key, existingTime) -> {
                            if (existingTime == null || currentTime > existingTime) {
                                return currentTime;
                            }
                            return existingTime;
                        });

                        // 检查是否是黑名单事件
                        return cacheService.isBlackEvent(eventId)
                                .thenCompose(isBlack -> {
                                    if (isBlack) {
                                        finalDataItem.put("errorCode", ErrorMessageEnum.EVENT_BLACK.getErrorCode());
                                        finalDataItem.put("errorDescribe", ErrorMessageEnum.EVENT_BLACK.getErrorMessage());
                                        return CompletableFuture.completedFuture(null);
                                    }

                                    // 检查平台匹配
                                    return cacheService.checkEventPlatform(eventId, sdk)
                                            .thenAccept(platformOk -> {
                                                if (!platformOk) {
                                                    // ★★★ 异步写入 event_platform 到 MySQL ★★★
                                                    writeEventPlatformToMysql(eventId, sdk);
                                                }
                                            });
                                });
                    });

            futures.add(future);
        }

        CompletableFuture<Void> updateProcessTimeFuture = CompletableFuture.runAsync(() -> {
            for (Map.Entry<Integer, Long> entry : eventProcessTimes.entrySet()) {
                Integer eventId = entry.getKey();
                Long processTime = entry.getValue();
                cacheService.updateEventProcessTimeLocalOnlyWithTime(eventId, processTime);
            }
        });

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
     * 根据数据类型获取 owner
     * 与旧工程 EventService.scala 保持一致
     */
    private String getOwner(String dt, String defaultOwner) {
        // abp 类型固定使用 "zg" 作为 owner
        if ("abp".equals(dt)) {
            return "zg";
        }
        return defaultOwner;
    }

    private CompletableFuture<Integer> getOrCreateEventId(Integer appId, String owner, String eventName) {
        return cacheService.getEventId(appId, owner, eventName)
                .thenCompose(eventId -> {
                    LOG.debug("首次查询事件是否存在,若存在直接返回 eventId={} ,appId={} ,owner={} ,eventName={} ", eventId, appId, owner, eventName);
                    if (eventId != null) {
                        return CompletableFuture.completedFuture(eventId);
                    }
                    LOG.debug("判断 appId={} 是否允许创建事件,不允许直接返回  ", appId);
                    return cacheService.isAutoCreateDisabled(appId)
                            .thenCompose(disabled -> {
                                LOG.debug("是否允许创建事件 disabled={}, appId={} ,owner={} ,eventName={} ,", disabled, appId, owner, eventName);
                                if (disabled) {
                                    return CompletableFuture.completedFuture(null);
                                }
                                LOG.debug("判断是否满足事件创建条件 appId={} ,owner={} ,eventName={} ", appId, owner, eventName);
                                return cacheService.isCreateEventForbid(appId)
                                        .thenCompose(forbid -> {
                                            LOG.debug("条件:forbid={} ,appId={} ,owner={} ,eventName={}", !forbid, appId, owner, eventName);
                                            if (forbid) {
                                                return CompletableFuture.completedFuture(null);
                                            }
                                            LOG.debug("结合分布式全局锁创建事件 appId={} ,owner={} ,eventName={} ", appId, owner, eventName);
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

        LOG.debug("获取锁之后再次查询event是否存在,若不存在就创建. 最终都会释放锁");
        return lockManager.executeWithLockAsync(lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS, () -> cacheService.getEventId(appId, owner, eventName)
                .thenCompose(existingId -> {
                    LOG.debug("appId={} ,owner={} ,eventName={} ,existingId={}", appId, owner, eventName, existingId);
                    if (existingId != null) {
                        LOG.debug("event已存在: appId={}, eventName={}, eventId={}",
                                appId, eventName, existingId);
                        return CompletableFuture.completedFuture(existingId);
                    }
                    LOG.debug("查询event不存在,创建事件 ,appId={}, owner={}, eventName={}", appId, owner, eventName);
                    return CompletableFuture.supplyAsync(() ->
                            createEventInDb(appId, owner, eventName));
                }));
    }

    private Integer createEventInDb(Integer appId, String owner, String eventName) {
        try (Connection conn = dataSource.getConnection()) {

            String selectSql = "SELECT id FROM event WHERE app_id = ? AND event_name = ? AND owner = ?";
            LOG.debug("查询mysql进行最终确认: selectSql={},appId={}, eventName={}, owner={}", selectSql, appId, eventName, owner);
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setInt(1, appId);
                stmt.setString(2, eventName);
                stmt.setString(3, owner);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int eventId = rs.getInt("id");
                    LOG.debug("查询mysql进行最终确认存在该事件: eventId={}", eventId);
                    cacheService.putEventIdCache(appId, owner, eventName, eventId);
                    syncEventToKVRocks(appId, owner, eventName, eventId);
                    return eventId;
                }
            }
            LOG.debug("查询mysql进行最终确认不存在该事件 appId={}, eventName={}, owner={}", appId, eventName, owner);
            String insertSql = "INSERT INTO event(app_id, event_name, owner, is_delete, is_stop, insert_time) " +
                    "VALUES(?, ?, ?, 0, 0, NOW())";
            LOG.debug("插入mysql进行创建: insertSql={},appId={}, eventName={}, owner={}", insertSql, appId, eventName, owner);
            try (PreparedStatement stmt = conn.prepareStatement(insertSql,
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                stmt.setInt(1, appId);
                stmt.setString(2, eventName);
                stmt.setString(3, owner);
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    int eventId = rs.getInt(1);
                    LOG.debug("插入mysql成功: eventId={}", eventId);
                    cacheService.putEventIdCache(appId, owner, eventName, eventId);
                    syncEventToKVRocks(appId, owner, eventName, eventId);

                    newEventCount.incrementAndGet();
                    LOG.info("创建事件成功: appId={}, eventName={}, eventId={}",
                            appId, eventName, eventId);
                    return eventId;
                }
            }
            LOG.error("该事件处理失败 appId={}, eventName={}, owner={}", appId, eventName, owner);
            return null;

        } catch (Exception e) {
            LOG.error("创建事件失败: appId={}, eventName={}", appId, eventName, e);
            return null;
        }
    }

    private void syncEventToKVRocks(Integer appId, String owner, String eventName, Integer eventId) {
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

    /**
     * 异步写入 event_platform 到 MySQL
     */
    private void writeEventPlatformToMysql(Integer eventId, Integer platform) {
        if (dataSource == null) {
            LOG.warn("数据库连接池未初始化，无法写入event_platform");
            return;
        }

        // 异步执行，不阻塞主流程
        CompletableFuture.runAsync(() -> {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();

                // 幂等写入 event_platform
                String sql = "INSERT IGNORE INTO event_platform(event_id, platform) VALUES (?, ?)";
                LOG.debug("写入MySQL[event_platform]: eventId={}, platform={}, sql={}", eventId, platform, sql);

                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setInt(1, eventId);
                    stmt.setInt(2, platform);
                    stmt.executeUpdate();
                }

                LOG.debug("成功写入MySQL[event_platform]: eventId={}, platform={}", eventId, platform);

                // 写入成功后，同步到KV和缓存
                syncEventPlatformToKVAndCache(eventId, platform);

            } catch (SQLException e) {
                LOG.error("写入MySQL[event_platform]失败: eventId={}, platform={}", eventId, platform, e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        }).exceptionally(ex -> {
            LOG.error("异步写入event_platform发生异常: eventId={}, platform={}", eventId, platform, ex);
            return null;
        });
    }

    /**
     * 同步 event_platform 到 KV 和缓存
     */
    private void syncEventPlatformToKVAndCache(Integer eventId, Integer platform) {
        try {
            boolean cluster = cacheConfig.isKvrocksCluster();

            // 构建 Set Key
            String key = cluster
                    ? "{" + CacheKeyConstants.EVENT_ID_PLATFORM + "}"
                    : CacheKeyConstants.EVENT_ID_PLATFORM;

            // 构建 Set Member: eventId_platform
            String member = CacheKeyConstants.eventPlatformMember(eventId, platform);

            // 异步写入 KV (使用 SADD 添加到 Set)
            kvrocksClient.asyncSAdd(key, member)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            LOG.error("同步event_platform到KV失败: eventId={}, platform={}", eventId, platform, ex);
                        } else {
                            LOG.info("同步event_platform到KV成功: eventId={}, platform={}, key={}, member={}",
                                    eventId, platform, key, member);
                        }
                    });

            // 更新内存缓存 (eventPlatformCache 是 AsyncLoadingCache<String, Boolean>)
            cacheService.putEventPlatformCache(eventId, platform);

            LOG.info("同步event_platform到KV和缓存完成: eventId={}, platform={}", eventId, platform);

        } catch (Exception e) {
            LOG.error("同步event_platform到KV和缓存失败: eventId={}, platform={}", eventId, platform, e);
        }
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