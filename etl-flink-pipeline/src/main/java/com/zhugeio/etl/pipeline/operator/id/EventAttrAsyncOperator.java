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
 * 事件属性处理算子 - 改造版
 * 
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 3. 黑名单查询从 Hash 改为 Set (asyncSIsMember)
 * 4. 属性ID查询 Field 格式改为: appId_eventId_owner_attrName(大写)
 * 
 * KVRocks Key 对照 (与同步服务一致):
 * - appIdEventAttrIdMap (Hash): field=appId_eventId_owner_attrName(大写)
 * - eventAttrColumnMap (Hash): field=eventId_attrId
 * - blackEventAttrIdSet (Set): member=attrId
 * - eventIdCreateAttrForbiddenSet (Set): member=eventId
 */
public class EventAttrAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EventAttrAsyncOperator.class);

    private static final int MAX_ATTR_NAME_LENGTH = 100;
    private static final int MAX_ATTR_PER_EVENT = 500;
    private static final long LOCK_WAIT_MS = 3000L;
    private static final long LOCK_EXPIRE_MS = 10000L;

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient OperatorMetrics metrics;
    private transient LockManager lockManager;

    // 统计
    private transient AtomicLong newAttrCount;
    private transient AtomicLong lockWaitCount;

    private CacheConfig cacheConfig;
    private final Properties dbProperties;
    private final int maxAttrLength;
    private final int metricsInterval;

    public EventAttrAsyncOperator(CacheConfig cacheConfig) {
        this(cacheConfig, null, MAX_ATTR_NAME_LENGTH, 60);
    }

    public EventAttrAsyncOperator(CacheConfig cacheConfig, Properties dbProperties, int maxAttrLength) {
        this(cacheConfig, dbProperties, maxAttrLength, 60);
    }

    public EventAttrAsyncOperator(CacheConfig cacheConfig, Properties dbProperties,
                                   int maxAttrLength, int metricsInterval) {
        this.cacheConfig = cacheConfig;
        this.dbProperties = dbProperties;
        this.maxAttrLength = maxAttrLength;
        this.metricsInterval = metricsInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "event_attr_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        cacheService = CacheServiceFactory.getInstance(cacheConfig);
        kvrocksClient = cacheService.getKvrocksClient();

        lockManager = new LockManager(kvrocksClient);

        if (dbProperties != null && dbProperties.containsKey("rdbms.url")) {
            initDataSource();
        }

        newAttrCount = new AtomicLong(0);
        lockWaitCount = new AtomicLong(0);

        LOG.info("[EventAttrAsyncOperator-{}] 初始化成功 (使用统一缓存服务)", 
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
        config.setPoolName("EventAttr-MySQL-Pool-" + getRuntimeContext().getIndexOfThisSubtask());

        dataSource = new HikariDataSource(config);
        LOG.info("EventAttr MySQL连接池初始化成功");
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

            processEventAttrs(input, appId, owner, eventArray, resultFuture);

        } catch (Exception e) {
            LOG.error("[EventAttrAsyncOperator] 处理失败", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    private void processEventAttrs(ZGMessage input, Integer appId, String owner,
                                    JSONArray eventArray, ResultFuture<ZGMessage> resultFuture) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < eventArray.size(); i++) {
            JSONObject event = eventArray.getJSONObject(i);
            Integer eventId = event.getInteger("ei");

            if (eventId == null || event.getIntValue("result", 0) == -1) {
                continue;
            }

            JSONObject pr = event.getJSONObject("pr");
            if (pr == null || pr.isEmpty()) {
                continue;
            }

            for (String attrName : new HashSet<>(pr.keySet())) {
                // 只处理自定义属性 (以 _ 开头)
                if (!attrName.startsWith("_")) {
                    continue;
                }

                String realAttrName = attrName.substring(1);
                if (realAttrName.isEmpty() || realAttrName.length() > maxAttrLength) {
                    continue;
                }

                Object attrValue = pr.get(attrName);
                String attrType = getObjectType(attrValue);

                // 使用统一缓存服务获取属性ID
                CompletableFuture<Void> future = getOrCreateEventAttrId(appId, eventId, owner, realAttrName, attrType)
                        .thenCompose(attrId -> {
                            if (attrId == null) {
                                pr.remove(attrName);
                                return CompletableFuture.completedFuture(null);
                            }

                            // 检查黑名单 (改为按 attrId 查询 Set)
                            return cacheService.isBlackEventAttr(attrId)
                                    .thenCompose(isBlack -> {
                                        if (isBlack) {
                                            pr.remove(attrName);
                                            return CompletableFuture.completedFuture(null);
                                        }

                                        // 获取列名
                                        return cacheService.getEventAttrColumn(eventId, attrId)
                                                .thenAccept(columnName -> {
                                                    if (columnName != null) {
                                                        pr.remove(attrName);
                                                        pr.put(columnName, attrValue);
                                                    }
                                                });
                                    });
                        });

                futures.add(future);
            }
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.error("[EventAttrAsyncOperator] 处理事件属性失败", ex);
                        metrics.error();
                    } else {
                        metrics.out();
                    }
                    resultFuture.complete(Collections.singleton(input));
                });
    }

    private CompletableFuture<Integer> getOrCreateEventAttrId(Integer appId, Integer eventId, 
                                                               String owner, String attrName, 
                                                               String attrType) {
        // 使用统一缓存服务获取属性ID (Field格式: appId_eventId_owner_attrName(大写))
        return cacheService.getEventAttrId(appId, eventId, owner, attrName)
                .thenCompose(attrId -> {
                    if (attrId != null) {
                        return CompletableFuture.completedFuture(attrId);
                    }

                    // 检查是否禁止创建属性
                    return cacheService.isCreateAttrForbid(eventId)
                            .thenCompose(forbid -> {
                                if (forbid) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                // 使用分布式锁创建属性
                                return createEventAttrWithLock(appId, eventId, owner, attrName, attrType);
                            });
                });
    }

    private CompletableFuture<Integer> createEventAttrWithLock(Integer appId, Integer eventId, 
                                                                String owner, String attrName,
                                                                String attrType) {
        if (dataSource == null) {
            LOG.warn("数据库连接池未初始化，无法创建事件属性");
            return CompletableFuture.completedFuture(null);
        }

        String lockKey = LockManager.eventAttrLockKey(eventId, owner, attrName);
        lockWaitCount.incrementAndGet();

        return lockManager.executeWithLockAsync(lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS, () -> {
            // 双重检查
            return cacheService.getEventAttrId(appId, eventId, owner, attrName)
                    .thenCompose(existingId -> {
                        if (existingId != null) {
                            LOG.debug("属性已被其他实例创建: eventId={}, attrName={}, attrId={}",
                                    eventId, attrName, existingId);
                            return CompletableFuture.completedFuture(existingId);
                        }

                        return CompletableFuture.supplyAsync(() -> 
                                createEventAttrInDb(appId, eventId, owner, attrName, attrType));
                    });
        });
    }

    private Integer createEventAttrInDb(Integer appId, Integer eventId, String owner, 
                                         String attrName, String attrType) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // 再次查询数据库
            String selectSql = "SELECT id, column_name FROM event_attr WHERE event_id = ? AND attr_name = ? AND owner = ?";
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setInt(1, eventId);
                stmt.setString(2, attrName);
                stmt.setString(3, owner);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    int attrId = rs.getInt("id");
                    String columnName = rs.getString("column_name");
                    
                    // 更新统一缓存
                    cacheService.putEventAttrIdCache(appId, eventId, owner, attrName, attrId);
                    cacheService.putEventAttrColumnCache(eventId, attrId, columnName);
                    
                    // 同步到 KVRocks
                    syncAttrToKVRocks(appId, eventId, owner, attrName, attrId, columnName);
                    return attrId;
                }
            }

            // 获取下一个可用的列名
            String columnName = getNextColumnName(conn, eventId);
            if (columnName == null) {
                LOG.warn("无法获取下一个列名: eventId={}", eventId);
                return null;
            }

            // 插入新属性
            String insertSql = "INSERT INTO event_attr(event_id, attr_name, prop_type, owner, column_name) " +
                    "VALUES(?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(insertSql, 
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {
                stmt.setInt(1, eventId);
                stmt.setString(2, attrName);
                stmt.setInt(3, getPropType(attrType));
                stmt.setString(4, owner);
                stmt.setString(5, columnName);
                stmt.executeUpdate();

                ResultSet rs = stmt.getGeneratedKeys();
                if (rs.next()) {
                    int attrId = rs.getInt(1);
                    
                    // 更新统一缓存
                    cacheService.putEventAttrIdCache(appId, eventId, owner, attrName, attrId);
                    cacheService.putEventAttrColumnCache(eventId, attrId, columnName);
                    
                    // 同步到 KVRocks
                    syncAttrToKVRocks(appId, eventId, owner, attrName, attrId, columnName);
                    
                    newAttrCount.incrementAndGet();
                    LOG.info("创建事件属性成功: eventId={}, attrName={}, attrId={}, column={}", 
                            eventId, attrName, attrId, columnName);
                    return attrId;
                }
            }

            return null;

        } catch (Exception e) {
            LOG.error("创建事件属性失败: eventId={}, attrName={}", eventId, attrName, e);
            return null;
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (Exception ignored) {}
            }
        }
    }

    private String getNextColumnName(Connection conn, Integer eventId) throws Exception {
        String sql = "SELECT MAX(CAST(SUBSTRING(column_name, 4) AS UNSIGNED)) as max_col " +
                "FROM event_attr WHERE event_id = ? AND column_name LIKE 'cus%'";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, eventId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int maxCol = rs.getInt("max_col");
                return "cus" + (maxCol + 1);
            }
        }
        return "cus1";
    }

    /**
     * 同步属性到 KVRocks (使用与同步服务一致的 Key 格式)
     */
    private void syncAttrToKVRocks(Integer appId, Integer eventId, String owner, 
                                    String attrName, Integer attrId, String columnName) {
        boolean cluster = cacheConfig.isKvrocksCluster();
        
        // 1. 属性ID映射: appIdEventAttrIdMap, field=appId_eventId_owner_attrName(大写)
        String attrIdKey = cluster 
                ? "{" + CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP + "}"
                : CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP;
        String attrIdField = CacheKeyConstants.eventAttrIdField(appId, eventId, owner, attrName);
        kvrocksClient.asyncHSet(attrIdKey, attrIdField, String.valueOf(attrId));
        
        // 2. 列名映射: eventAttrColumnMap, field=eventId_attrId
        String columnKey = cluster 
                ? "{" + CacheKeyConstants.EVENT_ATTR_COLUMN_MAP + "}"
                : CacheKeyConstants.EVENT_ATTR_COLUMN_MAP;
        String columnField = CacheKeyConstants.eventAttrColumnField(eventId, attrId);
        kvrocksClient.asyncHSet(columnKey, columnField, columnName);
    }

    private String getObjectType(Object value) {
        if (value == null) return "string";
        if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return "float";
            }
            return "int";
        }
        return "string";
    }

    private int getPropType(String type) {
        switch (type) {
            case "int": return 1;
            case "float": return 2;
            default: return 0;
        }
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[EventAttrAsyncOperator] 处理超时");
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
        }

        LOG.info("[EventAttrAsyncOperator-{}] 关闭, newAttr={}, lockWait={}",
                getRuntimeContext().getIndexOfThisSubtask(),
                newAttrCount.get(), lockWaitCount.get());
    }
}
