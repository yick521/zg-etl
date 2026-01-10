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
import com.zhugeio.etl.common.model.AttrInfo;
import com.zhugeio.etl.common.model.AttrResult;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
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
import java.util.stream.Collectors;

/**
 * 事件属性处理算子 - 批量优化版
 * <p>
 * 优化点:
 * 1. 批量查询已存在的属性ID（减少缓存查询次数）
 * 2. 批量插入新属性到数据库（使用批处理）
 * 3. 批量同步到KV存储（使用HMSET）
 * 4. 批量更新缓存（减少内存操作）
 * 5. 按eventId和owner分组处理（优化锁粒度）
 * 6. 列号按owner独立管理（避免并发冲突）
 * 7. 写入 event_attr_platform 表（平台映射）
 */
public class EventAttrAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(EventAttrAsyncOperator.class);

    private static final long LOCK_WAIT_MS = 3000L;
    private static final long LOCK_EXPIRE_MS = 10000L;

    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient OperatorMetrics metrics;
    private transient LockManager lockManager;

    private transient AtomicLong newAttrCount;
    private transient AtomicLong lockWaitCount;

    private final CacheConfig cacheConfig;
    private final Properties dbProperties;
    private final int maxAttrLength;
    private final int metricsInterval;

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

        LOG.info("[EventAttrAsyncOperator-{}] 初始化成功",
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
            String defaultOwner = String.valueOf(data.get("owner"));
            Integer sdk = input.getSdk();  // ★ 获取 platform/sdk 信息

            if (appId == null || appId == 0) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 从 data.data 读取事件列表
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

            processEventAttrs(input, appId, defaultOwner, sdk, dataList, resultFuture);

        } catch (Exception e) {
            LOG.error("[EventAttrAsyncOperator] 处理失败", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 批量处理事件属性
     * 主流程：收集属性 -> 批量查询/创建 -> 批量检查黑名单 -> 批量更新数据
     */
    @SuppressWarnings("unchecked")
    private void processEventAttrs(ZGMessage input, Integer appId, String defaultOwner,
                                   Integer sdk, List<Map<String, Object>> dataList,
                                   ResultFuture<ZGMessage> resultFuture) {

        // 第一步：收集所有需要处理的属性
        List<AttrInfo> allAttrs = new ArrayList<>();

        for (Map<String, Object> dataItem : dataList) {
            if (dataItem == null) continue;

            // 检查数据类型，只处理事件类型
            String dt = String.valueOf(dataItem.get("dt"));
            if (!"evt".equals(dt) && !"mkt".equals(dt) && !"abp".equals(dt) && !"vtl".equals(dt)) {
                continue;
            }

            // 检查是否有错误标记
            Object errorCodeObj = dataItem.get("errorCode");
            if (errorCodeObj != null) {
                continue;
            }

            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
            if (pr == null || pr.isEmpty()) {
                continue;
            }

            // 从 pr.$zg_eid 读取事件ID
            Integer eventId = parseInteger(pr.get("$zg_eid"));
            if (eventId == null || eventId == 0) {
                LOG.debug("跳过属性处理，事件ID不存在: appId={}", appId);
                continue;
            }

            // 根据dt类型确定owner
            String owner = getOwner(dt, defaultOwner);

            // 收集自定义属性（以 _ 开头）
            for (String attrName : new HashSet<>(pr.keySet())) {
                if (!attrName.startsWith("_")) {
                    continue;
                }

                String realAttrName = attrName.substring(1);
                if (realAttrName.isEmpty() || realAttrName.length() > maxAttrLength) {
                    continue;
                }

                Object attrValue = pr.get(attrName);
                String attrType = getObjectType(attrValue);

                allAttrs.add(new AttrInfo(appId, eventId, owner, realAttrName,
                        realAttrName, attrType, pr, attrName));
            }
        }

        if (allAttrs.isEmpty()) {
            metrics.out();
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        // 第二步：批量查询已存在的属性ID，不存在则创建
        batchGetOrCreateEventAttrs(allAttrs, sdk)  // ★ 传入 sdk 参数
                .thenCompose(attrResults -> {
                    // 第三步：批量检查黑名单并更新数据
                    return batchCheckBlacklistAndUpdateData(allAttrs, attrResults);
                })
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.error("[EventAttrAsyncOperator] 批量处理事件属性失败", ex);
                        metrics.error();
                    } else {
                        metrics.out();
                    }
                    resultFuture.complete(Collections.singleton(input));
                });
    }

    /**
     * 批量获取或创建事件属性
     * 按eventId分组处理，提高并发性能
     */
    private CompletableFuture<Map<String, AttrResult>> batchGetOrCreateEventAttrs(List<AttrInfo> attrs, Integer sdk) {
        Map<String, AttrResult> results = new ConcurrentHashMap<>();

        // 按 eventId 分组，减少锁竞争
        Map<Integer, List<AttrInfo>> groupedByEvent = attrs.stream()
                .collect(Collectors.groupingBy(AttrInfo::getEventId));

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<Integer, List<AttrInfo>> entry : groupedByEvent.entrySet()) {
            Integer eventId = entry.getKey();
            List<AttrInfo> eventAttrs = entry.getValue();

            CompletableFuture<Void> future = batchGetOrCreateForEvent(eventId, eventAttrs, sdk, results);
            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> results);
    }

    /**
     * 为单个事件批量处理属性
     * 1. 批量查询缓存
     * 2. 对缓存未命中的属性，检查是否禁止创建
     * 3. 加锁批量创建
     */
    private CompletableFuture<Void> batchGetOrCreateForEvent(Integer eventId,
                                                             List<AttrInfo> eventAttrs,
                                                             Integer sdk,
                                                             Map<String, AttrResult> results) {
        // 第一步：批量查询缓存
        List<CompletableFuture<Void>> cacheFutures = new ArrayList<>();
        List<AttrInfo> needCreate = Collections.synchronizedList(new ArrayList<>());

        for (AttrInfo attr : eventAttrs) {
            CompletableFuture<Void> cf = cacheService.getEventAttrId(attr.getAppId(), attr.getEventId(),
                            attr.getOwner(), attr.getAttrName())
                    .thenCompose(attrId -> {
                        if (attrId != null) {
                            // 缓存命中，异步获取列号
                            return cacheService.getEventAttrColumn(attr.getEventId(), attrId)
                                    .thenCompose(columnName -> {
                                        results.put(attr.getKey(), new AttrResult(attrId, columnName));
                                        // ★ 检查平台匹配并异步写入 event_attr_platform
                                        return checkAndWriteAttrPlatform(attrId, sdk);
                                    });
                        } else {
                            // 缓存未命中，需要创建
                            needCreate.add(attr);
                            return CompletableFuture.completedFuture(null);
                        }
                    });
            cacheFutures.add(cf);
        }

        return CompletableFuture.allOf(cacheFutures.toArray(new CompletableFuture[0]))
                .thenCompose(v -> {
                    if (needCreate.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    // 第二步：检查是否禁止创建
                    return cacheService.isCreateAttrForbid(eventId)
                            .thenCompose(forbid -> {
                                if (forbid) {
                                    LOG.debug("事件禁止创建新属性: eventId={}", eventId);
                                    return CompletableFuture.completedFuture(null);
                                }

                                // 第三步：加锁批量创建
                                return batchCreateEventAttrsWithLock(eventId, needCreate, sdk, results);
                            });
                });
    }

    /**
     * ★ 检查平台匹配并异步写入 event_attr_platform
     */
    private CompletableFuture<Void> checkAndWriteAttrPlatform(Integer attrId, Integer sdk) {
        if (sdk == null) {
            return CompletableFuture.completedFuture(null);
        }

        return cacheService.checkEventEttrPlatform(attrId, sdk)
                .thenAccept(platformOk -> {
                    if (!platformOk) {
                        // 平台不匹配，异步写入 event_attr_platform
                        writeEventAttrPlatformToMysql(attrId, sdk);
                    }
                });
    }

    /**
     * 加锁批量创建事件属性
     * 按owner分组，每个owner使用独立的锁
     */
    private CompletableFuture<Void> batchCreateEventAttrsWithLock(Integer eventId,
                                                                  List<AttrInfo> attrs,
                                                                  Integer sdk,
                                                                  Map<String, AttrResult> results) {
        if (dataSource == null || attrs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // 按owner分组，每个owner用一个锁
        Map<String, List<AttrInfo>> groupedByOwner = attrs.stream()
                .collect(Collectors.groupingBy(AttrInfo::getOwner));

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, List<AttrInfo>> entry : groupedByOwner.entrySet()) {
            String owner = entry.getKey();
            List<AttrInfo> ownerAttrs = entry.getValue();

            String lockKey = LockManager.eventAttrLockKey(eventId, owner);
            lockWaitCount.incrementAndGet();

            CompletableFuture<Void> future = lockManager.executeWithLockAsync(
                    lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS, () -> CompletableFuture.supplyAsync(() -> {
                        batchCreateEventAttrsInDb(eventId, owner, ownerAttrs, sdk, results);
                        return null;
                    })
            );

            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * 批量在数据库中创建事件属性
     * 1. 批量查询已存在的属性（防止并发创建）
     * 2. 获取下一个可用列号（按owner独立管理）
     * 3. 批量插入新属性
     * 4. 批量同步到KV和缓存
     * 5. ★ 批量写入 event_attr_platform
     */
    private void batchCreateEventAttrsInDb(Integer eventId, String owner,
                                           List<AttrInfo> attrs,
                                           Integer sdk,
                                           Map<String, AttrResult> results) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            // 第一步：批量查询已存在的属性（防止并发创建）
            String selectSql = "SELECT attr_id, attr_name, column_name FROM event_attr " +
                    "WHERE event_id = ? AND owner = ? AND attr_name IN (" +
                    String.join(",", Collections.nCopies(attrs.size(), "?")) + ")";

            Map<String, AttrResult> existingAttrs = new HashMap<>();
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setInt(1, eventId);
                stmt.setString(2, owner);
                for (int i = 0; i < attrs.size(); i++) {
                    stmt.setString(3 + i, attrs.get(i).getAttrName());
                }

                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    int attrId = rs.getInt("attr_id");
                    String attrName = rs.getString("attr_name");
                    String columnName = rs.getString("column_name");
                    existingAttrs.put(attrName, new AttrResult(attrId, columnName));
                }
            }

            // 过滤出需要创建的属性
            List<AttrInfo> toCreate = new ArrayList<>();
            for (AttrInfo attr : attrs) {
                AttrResult existing = existingAttrs.get(attr.getAttrName());
                if (existing != null) {
                    results.put(attr.getKey(), existing);
                    LOG.debug("属性已被其他实例创建: eventId={}, owner={}, attrName={}, attrId={}",
                            eventId, owner, attr.getAttrName(), existing.getAttrId());

                    // ★ 对已存在的属性也检查并写入平台信息
                    if (sdk != null) {
                        checkAndWriteAttrPlatformAsync(existing.getAttrId(), sdk);
                    }
                } else {
                    toCreate.add(attr);
                }
            }

            if (toCreate.isEmpty()) {
                conn.commit();
                // 批量更新缓存
                batchUpdateCache(eventId, owner, existingAttrs, attrs);
                return;
            }

            // 第二步：获取下一个可用的列号（按owner独立管理，避免并发冲突）
            int nextColNum = getNextColumnNumber(conn, eventId, owner);

            // 第三步：批量插入新属性
            String insertSql = "INSERT INTO event_attr(event_id, attr_name, prop_type, owner, column_name, insert_time) " +
                    "VALUES(?, ?, ?, ?, ?,NOW())";

            Map<String, AttrResult> newAttrs = new HashMap<>();
            Map<Integer, String> idxToColumnName = new HashMap<>();

            try (PreparedStatement stmt = conn.prepareStatement(insertSql,
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {

                for (int i = 0; i < toCreate.size(); i++) {
                    AttrInfo attr = toCreate.get(i);
                    String columnName = "cus" + (nextColNum + i);

                    stmt.setInt(1, eventId);
                    stmt.setString(2, attr.getAttrName());
                    stmt.setInt(3, getPropType(attr.getAttrType()));
                    stmt.setString(4, owner);
                    stmt.setString(5, columnName);
                    stmt.addBatch();

                    idxToColumnName.put(i, columnName);
                }

                stmt.executeBatch();
                ResultSet rs = stmt.getGeneratedKeys();

                int idx = 0;
                while (rs.next() && idx < toCreate.size()) {
                    int attrId = rs.getInt(1);
                    AttrInfo attr = toCreate.get(idx);
                    String columnName = idxToColumnName.get(idx);

                    AttrResult result = new AttrResult(attrId, columnName);
                    newAttrs.put(attr.getAttrName(), result);
                    results.put(attr.getKey(), result);

                    newAttrCount.incrementAndGet();
                    LOG.info("创建事件属性成功: eventId={}, owner={}, attrName={}, attrId={}, column={}",
                            eventId, owner, attr.getAttrName(), attrId, columnName);

                    idx++;
                }
            }

            conn.commit();

            // 第四步：批量同步到KV和缓存
            batchSyncToKVAndCache(eventId, owner, toCreate, newAttrs);

            // ★ 第五步：批量写入 event_attr_platform
            if (sdk != null && !newAttrs.isEmpty()) {
                batchWriteEventAttrPlatformToMysql(newAttrs, sdk);
            }

        } catch (Exception e) {
            LOG.error("批量创建事件属性失败: eventId={}, owner={}", eventId, owner, e);
            if (conn != null) {
                try { conn.rollback(); } catch (Exception ignored) {}
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * 获取下一个可用的列编号
     * 按 owner 独立管理列号（推荐，避免并发冲突）
     */
    private int getNextColumnNumber(Connection conn, Integer eventId, String owner) throws Exception {
        String sql = "SELECT MAX(CAST(SUBSTRING(column_name, 4) AS UNSIGNED)) as max_col " +
                "FROM event_attr WHERE event_id = ? AND owner = ? AND column_name LIKE 'cus%'";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, eventId);
            stmt.setString(2, owner);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("max_col") + 1;
            }
        }
        return 1;
    }

    /**
     * 批量同步到KV和缓存
     * 使用HMSET批量写入，减少网络IO
     */
    private void batchSyncToKVAndCache(Integer eventId, String owner,
                                       List<AttrInfo> attrs,
                                       Map<String, AttrResult> attrResults) {
        if (attrs.isEmpty()) {
            return;
        }

        boolean cluster = cacheConfig.isKvrocksCluster();

        // 批量同步属性 ID 到 KV
        String attrIdKey = cluster
                ? "{" + CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP + "}"
                : CacheKeyConstants.APP_ID_EVENT_ATTR_ID_MAP;

        Map<String, String> attrIdMap = new HashMap<>();
        Map<String, String> columnMap = new HashMap<>();

        for (AttrInfo attr : attrs) {
            AttrResult result = attrResults.get(attr.getAttrName());
            if (result == null) continue;

            String attrIdField = CacheKeyConstants.eventAttrIdField(
                    attr.getAppId(), eventId, owner, attr.getAttrName());
            attrIdMap.put(attrIdField, String.valueOf(result.getAttrId()));

            String columnField = CacheKeyConstants.eventAttrColumnField(eventId, result.getAttrId());
            columnMap.put(columnField, result.getColumnName());

            // 更新内存缓存
            cacheService.putEventAttrIdCache(attr.getAppId(), eventId, owner,
                    attr.getAttrName(), result.getAttrId());
            cacheService.putEventAttrColumnCache(eventId, result.getAttrId(), result.getColumnName());
        }

        // 批量写入KV - 使用 HMSET
        if (!attrIdMap.isEmpty()) {
            kvrocksClient.asyncHMSet(attrIdKey, attrIdMap);
        }

        String columnKey = cluster
                ? "{" + CacheKeyConstants.EVENT_ATTR_COLUMN_MAP + "}"
                : CacheKeyConstants.EVENT_ATTR_COLUMN_MAP;

        if (!columnMap.isEmpty()) {
            kvrocksClient.asyncHMSet(columnKey, columnMap);
        }

        LOG.info("批量同步KV成功: eventId={}, owner={}, count={}",
                eventId, owner, attrs.size());
    }

    /**
     * 批量更新缓存（用于已存在的属性）
     */
    private void batchUpdateCache(Integer eventId, String owner,
                                  Map<String, AttrResult> attrResults,
                                  List<AttrInfo> attrs) {
        for (AttrInfo attr : attrs) {
            AttrResult result = attrResults.get(attr.getAttrName());
            if (result != null) {
                cacheService.putEventAttrIdCache(attr.getAppId(), eventId, owner,
                        attr.getAttrName(), result.getAttrId());
                cacheService.putEventAttrColumnCache(eventId, result.getAttrId(), result.getColumnName());
            }
        }
    }

    /**
     * ★ 异步写入单个 event_attr_platform 到 MySQL
     */
    private void writeEventAttrPlatformToMysql(Integer attrId, Integer platform) {
        if (dataSource == null || platform == null) {
            return;
        }

        CompletableFuture.runAsync(() -> {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();

                // 幂等写入 event_attr_platform
                String sql = "INSERT IGNORE INTO event_attr_platform(event_attr_id, platform) VALUES (?, ?)";
                LOG.debug("写入MySQL[event_attr_platform]: attrId={}, platform={}", attrId, platform);

                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setInt(1, attrId);
                    stmt.setInt(2, platform);
                    stmt.executeUpdate();
                }

                LOG.debug("成功写入MySQL[event_attr_platform]: attrId={}, platform={}", attrId, platform);

                // 写入成功后，同步到KV和缓存
                syncEventAttrPlatformToKVAndCache(attrId, platform);

            } catch (SQLException e) {
                LOG.error("写入MySQL[event_attr_platform]失败: attrId={}, platform={}", attrId, platform, e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        }).exceptionally(ex -> {
            LOG.error("异步写入event_attr_platform发生异常: attrId={}, platform={}", attrId, platform, ex);
            return null;
        });
    }

    /**
     * ★ 批量写入 event_attr_platform 到 MySQL（用于新创建的属性）
     */
    private void batchWriteEventAttrPlatformToMysql(Map<String, AttrResult> newAttrs, Integer platform) {
        if (dataSource == null || platform == null || newAttrs.isEmpty()) {
            return;
        }

        CompletableFuture.runAsync(() -> {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
                conn.setAutoCommit(false);

                String sql = "INSERT IGNORE INTO event_attr_platform(event_attr_id, platform) VALUES (?, ?)";

                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    for (AttrResult result : newAttrs.values()) {
                        stmt.setInt(1, result.getAttrId());
                        stmt.setInt(2, platform);
                        stmt.addBatch();
                    }

                    stmt.executeBatch();
                }

                conn.commit();
                LOG.info("批量写入MySQL[event_attr_platform]成功: count={}, platform={}",
                        newAttrs.size(), platform);

                // 批量同步到KV和缓存
                for (AttrResult result : newAttrs.values()) {
                    syncEventAttrPlatformToKVAndCache(result.getAttrId(), platform);
                }

            } catch (SQLException e) {
                LOG.error("批量写入MySQL[event_attr_platform]失败: platform={}", platform, e);
                if (conn != null) {
                    try { conn.rollback(); } catch (Exception ignored) {}
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true);
                        conn.close();
                    } catch (SQLException ignored) {
                    }
                }
            }
        }).exceptionally(ex -> {
            LOG.error("批量异步写入event_attr_platform发生异常: platform={}", platform, ex);
            return null;
        });
    }

    /**
     * ★ 异步检查并写入属性平台信息（用于已存在的属性）
     */
    private void checkAndWriteAttrPlatformAsync(Integer attrId, Integer platform) {
        if (platform == null) {
            return;
        }

        cacheService.checkEventEttrPlatform(attrId, platform)
                .thenAccept(platformOk -> {
                    if (!platformOk) {
                        writeEventAttrPlatformToMysql(attrId, platform);
                    }
                })
                .exceptionally(ex -> {
                    LOG.error("检查属性平台失败: attrId={}, platform={}", attrId, platform, ex);
                    return null;
                });
    }

    /**
     * ★ 同步 event_attr_platform 到 KV 和缓存
     */
    private void syncEventAttrPlatformToKVAndCache(Integer attrId, Integer platform) {
        try {
            boolean cluster = cacheConfig.isKvrocksCluster();

            // 构建 Set Key
            String key = cluster
                    ? "{" + CacheKeyConstants.EVENT_ATTR_PLATFORM + "}"
                    : CacheKeyConstants.EVENT_ATTR_PLATFORM;

            // 构建 Set Member: attrId_platform
            String member = CacheKeyConstants.eventAttrPlatformMember(attrId, platform);

            // 异步写入 KV (使用 SADD 添加到 Set)
            kvrocksClient.asyncSAdd(key, member)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            LOG.error("同步event_attr_platform到KV失败: attrId={}, platform={}",
                                    attrId, platform, ex);
                        } else {
                            LOG.info("同步event_attr_platform到KV成功: attrId={}, platform={}, key={}, member={}",
                                    attrId, platform, key, member);
                        }
                    });

            // 更新内存缓存
            cacheService.putEventAttrdPlatformCache(attrId, platform);

            LOG.info("同步event_attr_platform到KV和缓存完成: attrId={}, platform={}", attrId, platform);

        } catch (Exception e) {
            LOG.error("同步event_attr_platform到KV和缓存失败: attrId={}, platform={}", attrId, platform, e);
        }
    }

    /**
     * 批量检查黑名单并更新数据
     * 并发检查所有属性的黑名单状态，然后统一更新pr对象
     */
    private CompletableFuture<Void> batchCheckBlacklistAndUpdateData(
            List<AttrInfo> attrs, Map<String, AttrResult> attrResults) {

        // 收集所有需要检查的属性 ID
        Set<Integer> attrIds = attrResults.values().stream()
                .map(AttrResult::getAttrId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (attrIds.isEmpty()) {
            // 所有属性都创建失败，移除它们
            for (AttrInfo attr : attrs) {
                attr.getPr().remove(attr.getOriginalKey());
            }
            return CompletableFuture.completedFuture(null);
        }

        // 批量检查黑名单
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (AttrInfo attr : attrs) {
            AttrResult result = attrResults.get(attr.getKey());
            if (result == null) {
                // 属性创建失败，移除
                attr.getPr().remove(attr.getOriginalKey());
                continue;
            }

            CompletableFuture<Void> future = cacheService.isBlackEventAttr(result.getAttrId())
                    .thenAccept(isBlack -> {
                        if (isBlack) {
                            // 黑名单属性，移除
                            attr.getPr().remove(attr.getOriginalKey());
                        } else {
                            // 正常属性，写入属性ID和类型
                            attr.getPr().put("$zg_epid#" + attr.getOriginalKey(), result.getAttrId());
                            attr.getPr().put("$zg_eptp#" + attr.getOriginalKey(), attr.getAttrType());
                        }
                    })
                    .exceptionally(ex -> {
                        LOG.error("检查黑名单失败: attrId={}", result.getAttrId(), ex);
                        // 出错时保守处理，移除该属性
                        attr.getPr().remove(attr.getOriginalKey());
                        return null;
                    });

            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * 根据数据类型获取 owner
     * 与旧工程保持一致
     */
    private String getOwner(String dt, String defaultOwner) {
        // abp 类型固定使用 "zg" 作为 owner
        if ("abp".equals(dt)) {
            return "zg";
        }
        return defaultOwner;
    }

    private Integer parseInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Integer) return (Integer) value;
        if (value instanceof Number) return ((Number) value).intValue();
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String getObjectType(Object value) {
        if (value == null) {
            return "null";
        }else if (value instanceof Number) {
            return "number";
        }else if (value instanceof String) {
            return "string";
        }else {
            return "object";
        }
    }

    private int getPropType(String type) {
        switch (type) {
            case "string": return 1;
            case "number": return 2;
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