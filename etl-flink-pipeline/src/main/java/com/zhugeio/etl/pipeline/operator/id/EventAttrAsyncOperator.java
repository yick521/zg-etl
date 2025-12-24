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
 * 事件属性处理算子
 * 
 * ✅ 核心功能:
 * 1. 处理事件中的自定义属性 (以 _ 开头)
 * 2. 获取或创建事件属性ID
 * 3. 处理属性黑名单过滤
 * 4. 属性类型转换和校验
 * 5. 自动创建新事件属性 (使用分布式锁防止并发冲突)
 * 6. 集成 OperatorMetrics 监控
 * 
 * ✅ 分布式锁机制:
 * - 创建新属性时使用 KVRocks 分布式锁
 * - 锁的 key: dlock:event_attr:{eventId}_{owner}_{attrName}
 * - 防止多个并行实例同时创建相同属性
 * 
 * Hash结构:
 * - event_attr:{eventId} field={owner}_{attrName} value={attrId}
 * - event_attr_column:{eventId}_{attrId} value={columnName}
 * - black_event_attr field={appId}_{eventId}_{attrName} value=1
 */
public class EventAttrAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EventAttrAsyncOperator.class);

    // Hash Key 常量
    private static final String EVENT_ATTR_HASH_PREFIX = "event_attr:";
    private static final String EVENT_ATTR_COLUMN_HASH = "event_attr_column";
    private static final String BLACK_EVENT_ATTR_HASH = "black_event_attr";
    private static final String CREATE_ATTR_FORBID_HASH = "create_attr_forbid";

    // 最大属性名长度
    private static final int MAX_ATTR_NAME_LENGTH = 100;
    // 每个事件最大属性数
    private static final int MAX_ATTR_PER_EVENT = 500;
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
    private transient Cache<String, Integer> eventAttrIdCache;
    private transient Cache<String, String> eventAttrColumnCache;
    private transient Cache<String, Boolean> blackEventAttrCache;
    private transient Cache<String, Boolean> createAttrForbidCache;
    private transient Cache<String, Integer> eventAttrCountCache;

    // 统计
    private transient AtomicLong cacheHitCount;
    private transient AtomicLong cacheMissCount;
    private transient AtomicLong newAttrCount;
    private transient AtomicLong lockWaitCount;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final int maxAttrLength;
    private final int metricsInterval;

    public EventAttrAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, MAX_ATTR_NAME_LENGTH, 60);
    }

    public EventAttrAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, 
                                   int maxAttrLength) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, maxAttrLength, 60);
    }

    public EventAttrAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, 
                                   int maxAttrLength, int metricsInterval) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.maxAttrLength = maxAttrLength;
        this.metricsInterval = metricsInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 Metrics
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "event_attr_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        // 初始化 ConfigCacheService
        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .build();
        
        configCacheService = CacheServiceFactory.getConfigService("event-attr", cacheConfig);
        kvrocksClient = configCacheService.getKvrocksClient();

        // 初始化分布式锁管理器
        lockManager = new LockManager(kvrocksClient);

        // 初始化本地缓存
        initLocalCaches();

        // 初始化数据库连接池 (从 Config 获取)
        initDataSource();

        // 初始化统计
        cacheHitCount = new AtomicLong(0);
        cacheMissCount = new AtomicLong(0);
        newAttrCount = new AtomicLong(0);
        lockWaitCount = new AtomicLong(0);

        LOG.info("[EventAttrAsyncOperator-{}] 初始化成功 (分布式锁已启用)", 
                getRuntimeContext().getIndexOfThisSubtask());
    }

    private void initLocalCaches() {
        eventAttrIdCache = Caffeine.newBuilder()
                .maximumSize(500000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .recordStats()
                .build();

        eventAttrColumnCache = Caffeine.newBuilder()
                .maximumSize(500000)
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .recordStats()
                .build();

        blackEventAttrCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();

        createAttrForbidCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        eventAttrCountCache = Caffeine.newBuilder()
                .maximumSize(50000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
    }

    private void initDataSource() {
        // 从 Config 获取数据库配置
        String url = com.zhugeio.etl.common.config.Config.getString("rdbms.url");
        if (url == null || url.isEmpty()) {
            LOG.warn("rdbms.url 未配置，跳过数据库连接池初始化");
            return;
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(com.zhugeio.etl.common.config.Config.getString("rdbms.userName"));
        config.setPassword(com.zhugeio.etl.common.config.Config.getString("rdbms.password"));
        config.setDriverClassName(com.zhugeio.etl.common.config.Config.getString("rdbms.driverClass", "com.mysql.cj.jdbc.Driver"));
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

            // 处理事件属性列表
            processEventAttrs(input, appId, owner, eventArray, resultFuture);

        } catch (Exception e) {
            LOG.error("[EventAttrAsyncOperator] 处理失败", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 处理事件属性列表
     */
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

            // 处理每个属性
            for (String attrName : new HashSet<>(pr.keySet())) {
                // 只处理自定义属性 (以 _ 开头)
                if (!attrName.startsWith("_")) {
                    continue;
                }

                String realAttrName = attrName.substring(1);  // 去掉前缀 _
                if (realAttrName.isEmpty() || realAttrName.length() > maxAttrLength) {
                    continue;
                }

                Object attrValue = pr.get(attrName);
                String attrType = getObjectType(attrValue);

                CompletableFuture<Void> future = isBlackEventAttr(appId, eventId, realAttrName)
                        .thenCompose(isBlack -> {
                            if (isBlack) {
                                pr.remove(attrName);
                                return CompletableFuture.completedFuture(null);
                            }

                            return getOrCreateEventAttrId(eventId, owner, realAttrName, attrType)
                                    .thenCompose(attrId -> {
                                        if (attrId == null) {
                                            pr.remove(attrName);
                                            return CompletableFuture.completedFuture(null);
                                        }

                                        // 获取列名
                                        return getEventAttrColumn(eventId, attrId)
                                                .thenAccept(columnName -> {
                                                    if (columnName != null) {
                                                        // 重命名属性: _xxx -> cus1
                                                        pr.remove(attrName);
                                                        pr.put(columnName, attrValue);
                                                    }
                                                });
                                    });
                        });

                futures.add(future);
            }
        }

        // 等待所有属性处理完成
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

    /**
     * 获取或创建事件属性ID
     */
    private CompletableFuture<Integer> getOrCreateEventAttrId(Integer eventId, String owner, 
                                                               String attrName, String attrType) {
        String cacheKey = eventId + "_" + owner + "_" + attrName;

        // 1. 先查本地缓存
        Integer cached = eventAttrIdCache.getIfPresent(cacheKey);
        if (cached != null) {
            cacheHitCount.incrementAndGet();
            return CompletableFuture.completedFuture(cached);
        }

        cacheMissCount.incrementAndGet();

        // 2. 查 KVRocks
        String hashKey = EVENT_ATTR_HASH_PREFIX + eventId;
        String field = owner + "_" + attrName;

        return kvrocksClient.asyncHGet(hashKey, field)
                .thenCompose(value -> {
                    if (value != null) {
                        try {
                            Integer attrId = Integer.parseInt(value);
                            eventAttrIdCache.put(cacheKey, attrId);
                            return CompletableFuture.completedFuture(attrId);
                        } catch (NumberFormatException e) {
                            LOG.warn("属性ID格式错误: {}", value);
                        }
                    }

                    // 3. 检查是否禁止创建属性
                    return isCreateAttrForbid(eventId)
                            .thenCompose(forbid -> {
                                if (forbid) {
                                    return CompletableFuture.completedFuture(null);
                                }

                                // 4. 检查属性数量限制
                                return getEventAttrCount(eventId)
                                        .thenCompose(count -> {
                                            if (count >= MAX_ATTR_PER_EVENT) {
                                                LOG.warn("事件属性数量超限: eventId={}, count={}", 
                                                        eventId, count);
                                                return CompletableFuture.completedFuture(null);
                                            }

                                            // 5. 使用分布式锁创建属性
                                            return createEventAttrWithLock(eventId, owner, attrName, attrType);
                                        });
                            });
                });
    }

    /**
     * 使用分布式锁创建事件属性
     */
    private CompletableFuture<Integer> createEventAttrWithLock(Integer eventId, String owner, 
                                                                String attrName, String attrType) {
        if (dataSource == null) {
            LOG.warn("数据库连接池未初始化，无法创建事件属性");
            return CompletableFuture.completedFuture(null);
        }

        String lockKey = LockManager.eventAttrLockKey(eventId, owner, attrName);
        lockWaitCount.incrementAndGet();

        return lockManager.executeWithLockAsync(lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS, () -> {
            // 双重检查
            String hashKey = EVENT_ATTR_HASH_PREFIX + eventId;
            String field = owner + "_" + attrName;

            return kvrocksClient.asyncHGet(hashKey, field)
                    .thenCompose(existingValue -> {
                        if (existingValue != null) {
                            try {
                                Integer existingId = Integer.parseInt(existingValue);
                                String cacheKey = eventId + "_" + owner + "_" + attrName;
                                eventAttrIdCache.put(cacheKey, existingId);
                                LOG.debug("属性已被其他实例创建: eventId={}, attrName={}, attrId={}",
                                        eventId, attrName, existingId);
                                return CompletableFuture.completedFuture(existingId);
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                        }

                        // 真正创建属性
                        return CompletableFuture.supplyAsync(() -> 
                                createEventAttrInDb(eventId, owner, attrName, attrType));
                    });
        });
    }

    /**
     * 在数据库中创建事件属性
     */
    private Integer createEventAttrInDb(Integer eventId, String owner, String attrName, String attrType) {
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
                    String cacheKey = eventId + "_" + owner + "_" + attrName;
                    eventAttrIdCache.put(cacheKey, attrId);
                    eventAttrColumnCache.put(eventId + "_" + attrId, columnName);
                    
                    // 同步到 KVRocks
                    syncAttrToKVRocks(eventId, owner, attrName, attrId, columnName);
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
                    String cacheKey = eventId + "_" + owner + "_" + attrName;
                    eventAttrIdCache.put(cacheKey, attrId);
                    eventAttrColumnCache.put(eventId + "_" + attrId, columnName);
                    
                    // 同步到 KVRocks
                    syncAttrToKVRocks(eventId, owner, attrName, attrId, columnName);
                    
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
                try {
                    conn.close();
                } catch (Exception e) {
                    LOG.error("关闭连接失败", e);
                }
            }
        }
    }

    /**
     * 获取下一个可用的列名 (cus1, cus2, ...)
     */
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
     * 同步属性到 KVRocks
     */
    private void syncAttrToKVRocks(Integer eventId, String owner, String attrName, 
                                    Integer attrId, String columnName) {
        String hashKey = EVENT_ATTR_HASH_PREFIX + eventId;
        String field = owner + "_" + attrName;
        kvrocksClient.asyncHSet(hashKey, field, String.valueOf(attrId));
        kvrocksClient.asyncHSet(EVENT_ATTR_COLUMN_HASH, eventId + "_" + attrId, columnName);
    }

    /**
     * 获取事件属性列名
     */
    private CompletableFuture<String> getEventAttrColumn(Integer eventId, Integer attrId) {
        String cacheKey = eventId + "_" + attrId;
        String cached = eventAttrColumnCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(EVENT_ATTR_COLUMN_HASH, cacheKey)
                .thenApply(value -> {
                    if (value != null) {
                        eventAttrColumnCache.put(cacheKey, value);
                    }
                    return value;
                });
    }

    /**
     * 检查是否在黑名单
     */
    private CompletableFuture<Boolean> isBlackEventAttr(Integer appId, Integer eventId, String attrName) {
        String cacheKey = appId + "_" + eventId + "_" + attrName;
        Boolean cached = blackEventAttrCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(BLACK_EVENT_ATTR_HASH, cacheKey)
                .thenApply(value -> {
                    boolean isBlack = value != null && "1".equals(value);
                    blackEventAttrCache.put(cacheKey, isBlack);
                    return isBlack;
                });
    }

    /**
     * 检查是否禁止创建属性
     */
    private CompletableFuture<Boolean> isCreateAttrForbid(Integer eventId) {
        String cacheKey = String.valueOf(eventId);
        Boolean cached = createAttrForbidCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        return kvrocksClient.asyncHGet(CREATE_ATTR_FORBID_HASH, cacheKey)
                .thenApply(value -> {
                    boolean forbid = value != null && "1".equals(value);
                    createAttrForbidCache.put(cacheKey, forbid);
                    return forbid;
                });
    }

    /**
     * 获取事件属性数量
     */
    private CompletableFuture<Integer> getEventAttrCount(Integer eventId) {
        String cacheKey = String.valueOf(eventId);
        Integer cached = eventAttrCountCache.getIfPresent(cacheKey);
        if (cached != null) {
            return CompletableFuture.completedFuture(cached);
        }

        String hashKey = EVENT_ATTR_HASH_PREFIX + eventId;
        return KvrocksClient.asyncHLen(kvrocksClient, hashKey)
                .thenApply(count -> {
                    int countInt = count != null ? count.intValue() : 0;
                    eventAttrCountCache.put(cacheKey, countInt);
                    return countInt;
                });
    }

    /**
     * 获取属性值类型
     */
    private String getObjectType(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof Number) {
            return "number";
        } else if (value instanceof String) {
            return "string";
        } else if (value instanceof Boolean) {
            return "boolean";
        } else {
            return "object";
        }
    }

    /**
     * 获取属性类型代码
     */
    private int getPropType(String attrType) {
        switch (attrType) {
            case "number":
                return 1;
            case "string":
                return 2;
            case "boolean":
                return 3;
            default:
                return 2;
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
            LOG.info("MySQL连接池已关闭");
        }

        LOG.info("[EventAttrAsyncOperator-{}] 关闭, stats: cacheHit={}, cacheMiss={}, newAttr={}, lockWait={}",
                getRuntimeContext().getIndexOfThisSubtask(),
                cacheHitCount.get(), cacheMissCount.get(), newAttrCount.get(), lockWaitCount.get());
    }
}
