package com.zhugeio.etl.pipeline.operator.id;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheKeyConstants;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.lock.LockManager;
import com.zhugeio.etl.common.model.PropInfo;
import com.zhugeio.etl.common.model.PropResult;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 用户属性处理算子 - 批量优化版
 * <p>
 * 优化点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 3. 黑名单查询从 Hash 改为 Set (asyncSIsMember)
 * 4. 批量查询已存在的属性ID（减少缓存查询次数）
 * 5. 批量插入新属性到数据库（使用批处理）
 * 6. 批量同步到KV存储（使用HMSET）
 * 7. 批量更新缓存（减少内存操作）
 * 8. 按 appId + zgid 分组处理（用户级别锁，避免同一用户并发创建冲突）
 * <p>
 * 锁粒度说明:
 * - 锁Key格式: user_prop:appId_zgid_batch
 * - zgid: 设备ID/用户ID，从 dt=usr 的数据中提取
 * - 同一用户的多个属性在一个事务中批量创建
 * <p>
 * KVRocks Key 对照 (与同步服务一致):
 * - appIdPropIdMap (Hash): field=appId_owner_propName(大写)
 * - appIdPropIdOriginalMap (Hash): field=appId_owner_propId
 * - blackUserPropSet (Set): member=propId
 */
public class UserPropAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(UserPropAsyncOperator.class);

    private static final long LOCK_WAIT_MS = 3000L;
    private static final long LOCK_EXPIRE_MS = 10000L;

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient LockManager lockManager;

    private transient AtomicLong newPropCount;
    private transient AtomicLong lockWaitCount;

    private final CacheConfig cacheConfig;
    private final Properties dbProperties;
    private final int maxPropLength;

    public UserPropAsyncOperator(CacheConfig cacheConfig, Properties dbProperties, int maxPropLength) {
        this.cacheConfig = cacheConfig;
        this.dbProperties = dbProperties;
        this.maxPropLength = maxPropLength;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);
        kvrocksClient = cacheService.getKvrocksClient();

        lockManager = new LockManager(kvrocksClient);

        initializeConnectionPool();

        newPropCount = new AtomicLong(0);
        lockWaitCount = new AtomicLong(0);

        LOG.info("[UserPropAsyncOperator-{}] 初始化成功 (使用统一缓存服务)",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    private void initializeConnectionPool() {
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
        config.setPoolName("UserProp-MySQL-Pool-" + getRuntimeContext().getIndexOfThisSubtask());

        dataSource = new HikariDataSource(config);
        LOG.info("UserProp MySQL连接池初始化成功");
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        try {
            List<PropInfo> propInfos = extractUserProps(input);

            if (propInfos.isEmpty()) {
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            processUserProps(input, propInfos)
                    .whenComplete((output, throwable) -> {
                        if (throwable != null) {
                            LOG.error("处理用户属性失败", throwable);
                            resultFuture.completeExceptionally(throwable);
                        } else {
                            resultFuture.complete(Collections.singleton(output));
                        }
                    });

        } catch (Exception e) {
            LOG.error("asyncInvoke 异常", e);
            resultFuture.completeExceptionally(e);
        }
    }

    /**
     * 提取用户属性信息
     */
    @SuppressWarnings("unchecked")
    private List<PropInfo> extractUserProps(ZGMessage msg) {
        List<PropInfo> propInfos = new ArrayList<>();

        if (msg.getResult() == -1) {
            return propInfos;
        }

        Integer appId = msg.getAppId();
        String owner = String.valueOf(msg.getData().get("owner"));

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) msg.getData().get("data");

        if (dataList == null) {
            return propInfos;
        }

        for (Map<String, Object> dataItem : dataList) {
            String dt = (String) dataItem.get("dt");
            if (!"usr".equals(dt)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
            if (pr == null) {
                continue;
            }

            // 从 pr 中获取 zgid
            String zgZgid = String.valueOf(pr.get("zg_zgid"));
            if (zgZgid == null || "null".equals(zgZgid) || zgZgid.isEmpty()) {
                LOG.warn("usr类型数据缺少 zgZgid 字段，跳过属性处理: appId={}", appId);
                continue;
            }

            for (String key : new HashSet<>(pr.keySet())) {
                if (key.startsWith("_")) {
                    String propName = key.substring(1);
                    Object propValue = pr.get(key);
                    int propType = getObjectType(propValue);

                    // 过滤空值和过长属性
                    if ("null".equals(String.valueOf(propType)) || propName.length() > maxPropLength) {
                        pr.remove(key);
                        continue;
                    }

                    propInfos.add(new PropInfo(appId, owner, zgZgid, propName, propType, pr, key));
                }
            }
        }

        return propInfos;
    }

    /**
     * 批量处理用户属性
     * 主流程：收集属性 -> 批量查询/创建 -> 批量检查黑名单 -> 批量更新数据
     */
    private CompletableFuture<ZGMessage> processUserProps(ZGMessage input, List<PropInfo> propInfos) {
        if (propInfos.isEmpty()) {
            return CompletableFuture.completedFuture(input);
        }

        // 第一步：批量获取或创建用户属性ID
        return batchGetOrCreateUserProps(propInfos)
                .thenCompose(propResults -> {
                    // 第二步：批量检查黑名单并更新数据
                    return batchCheckBlacklistAndUpdateData(propInfos, propResults);
                })
                .thenApply(v -> input);
    }

    /**
     * 批量获取或创建用户属性
     * 按 appId + zgid 分组处理（用户级别），提高并发性能
     */
    private CompletableFuture<Map<String, PropResult>> batchGetOrCreateUserProps(List<PropInfo> propInfos) {
        Map<String, PropResult> results = new ConcurrentHashMap<>();

        // 按 appId + zgid 分组（用户级别），减少锁竞争
        Map<String, List<PropInfo>> groupedByUser = propInfos.stream()
                .collect(Collectors.groupingBy(prop -> prop.getAppId() + "_" + prop.getZgZgid()));

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, List<PropInfo>> entry : groupedByUser.entrySet()) {
            List<PropInfo> userProps = entry.getValue();

            CompletableFuture<Void> future = batchGetOrCreateForUser(userProps, results);
            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> results);
    }

    /**
     * 为单个用户批量处理属性
     * 1. 批量查询缓存
     * 2. 对缓存未命中的属性，加锁批量创建
     */
    private CompletableFuture<Void> batchGetOrCreateForUser(List<PropInfo> userProps,
                                                            Map<String, PropResult> results) {
        // 第一步：批量查询缓存
        List<CompletableFuture<Void>> cacheFutures = new ArrayList<>();
        List<PropInfo> needCreate = Collections.synchronizedList(new ArrayList<>());

        for (PropInfo prop : userProps) {
            CompletableFuture<Void> cf = cacheService.getUserPropId(
                            prop.getAppId(), prop.getOwner(), prop.getPropName())
                    .thenCompose(propId -> {
                        if (propId != null) {
                            // 缓存命中，异步获取原始名称
                            return cacheService.getUserPropOriginalName(
                                            prop.getAppId(), prop.getOwner(), propId)
                                    .thenAccept(originalName -> results.put(prop.getKey(), new PropResult(propId, originalName)));
                        } else {
                            // 缓存未命中，需要创建
                            needCreate.add(prop);
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

                    // 第二步：加锁批量创建
                    return batchCreateUserPropsWithLock(needCreate, results);
                });
    }

    /**
     * 加锁批量创建用户属性
     * 按 appId + zgid（用户级别）加锁，避免同一用户并发创建冲突
     */
    private CompletableFuture<Void> batchCreateUserPropsWithLock(List<PropInfo> props,
                                                                 Map<String, PropResult> results) {
        if (dataSource == null || props.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // 使用第一个属性的 appId 和 zgid（同一批次都相同）
        PropInfo first = props.get(0);
        // 锁Key格式: user_prop:appId_zgid_batch
        String lockKey = "user_prop:" + first.getAppId() + "_" + first.getZgZgid() + "_batch";
        lockWaitCount.incrementAndGet();

        return lockManager.executeWithLockAsync(
                lockKey, LOCK_WAIT_MS, LOCK_EXPIRE_MS,
                () -> CompletableFuture.supplyAsync(() -> {
                    batchCreateUserPropsInDb(props, results);
                    return null;
                })
        );
    }

    /**
     * 批量在数据库中创建用户属性
     * 1. 批量查询已存在的属性（防止并发创建）
     * 2. 批量插入新属性
     * 3. 批量同步到KV和缓存
     */
    private void batchCreateUserPropsInDb(List<PropInfo> props, Map<String, PropResult> results) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            PropInfo first = props.get(0);
            Integer appId = first.getAppId();
            String owner = first.getOwner();
            String ZgZgid = first.getZgZgid();

            // 第一步：批量查询已存在的属性（防止并发创建）
            String selectSql = "SELECT id, name FROM user_prop_meta " +
                    "WHERE app_id = ? AND owner = ? AND name IN (" +
                    String.join(",", Collections.nCopies(props.size(), "?")) + ")";

            Map<String, PropResult> existingProps = new HashMap<>();
            try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                stmt.setInt(1, appId);
                stmt.setString(2, owner);
                for (int i = 0; i < props.size(); i++) {
                    stmt.setString(3 + i, props.get(i).getPropName());
                }

                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    int propId = rs.getInt("id");
                    String propName = rs.getString("name");
                    existingProps.put(propName, new PropResult(propId, propName));
                }
            }

            // 过滤出需要创建的属性
            List<PropInfo> toCreate = new ArrayList<>();
            for (PropInfo prop : props) {
                PropResult existing = existingProps.get(prop.getPropName());
                if (existing != null) {
                    results.put(prop.getKey(), existing);
                    LOG.debug("属性已被其他实例创建: appId={}, ZgZgid={}, owner={}, propName={}, propId={}",
                            appId, ZgZgid, owner, prop.getPropName(), existing.getPropId());
                } else {
                    toCreate.add(prop);
                }
            }

            if (toCreate.isEmpty()) {
                conn.commit();
                // 批量更新缓存
                batchUpdateCache(appId, owner, existingProps, props);
                return;
            }

            // 第二步：批量插入新属性
            String insertSql = "INSERT IGNORE INTO user_prop_meta(app_id, owner, name, type) VALUES(?, ?, ?, ?)";
            Map<String, PropResult> newProps = new HashMap<>();

            try (PreparedStatement stmt = conn.prepareStatement(insertSql,
                    java.sql.Statement.RETURN_GENERATED_KEYS)) {

                for (PropInfo prop : toCreate) {
                    stmt.setInt(1, appId);
                    stmt.setString(2, owner);
                    stmt.setString(3, prop.getPropName());
                    stmt.setInt(4, prop.getPropType());
                    stmt.addBatch();
                }

                stmt.executeBatch();
                ResultSet rs = stmt.getGeneratedKeys();

                int idx = 0;
                while (rs.next() && idx < toCreate.size()) {
                    int propId = rs.getInt(1);
                    PropInfo prop = toCreate.get(idx);

                    PropResult result = new PropResult(propId, prop.getPropName());
                    newProps.put(prop.getPropName(), result);
                    results.put(prop.getKey(), result);

                    newPropCount.incrementAndGet();
                    LOG.info("创建用户属性成功: appId={}, ZgZgid={}, owner={}, propName={}, propId={}",
                            appId, ZgZgid, owner, prop.getPropName(), propId);

                    idx++;
                }
            }

            conn.commit();

            // 第三步：批量同步到KV和缓存
            batchSyncToKVAndCache(appId, owner, toCreate, newProps);

        } catch (Exception e) {
            LOG.error("批量创建用户属性失败: props={}", props.size(), e);
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
     * 批量同步到KV和缓存
     * 使用HMSET批量写入，减少网络IO
     */
    private void batchSyncToKVAndCache(Integer appId, String owner,
                                       List<PropInfo> props,
                                       Map<String, PropResult> propResults) {
        if (props.isEmpty()) {
            return;
        }

        boolean cluster = cacheConfig.isKvrocksCluster();
        String ZgZgid = props.get(0).getZgZgid();

        // 批量同步属性 ID 到 KV
        String propIdKey = cluster
                ? "{" + CacheKeyConstants.APP_ID_PROP_ID_MAP + "}"
                : CacheKeyConstants.APP_ID_PROP_ID_MAP;

        Map<String, String> propIdMap = new HashMap<>();
        Map<String, String> originalMap = new HashMap<>();

        for (PropInfo prop : props) {
            PropResult result = propResults.get(prop.getPropName());
            if (result == null) continue;

            String propIdField = CacheKeyConstants.userPropIdField(appId, owner, prop.getPropName());
            propIdMap.put(propIdField, String.valueOf(result.getPropId()));

            String originalField = CacheKeyConstants.userPropOriginalField(appId, owner, result.getPropId());
            originalMap.put(originalField, prop.getPropName());

            // 更新内存缓存
            cacheService.putUserPropIdCache(appId, owner, prop.getPropName(), result.getPropId());
        }

        // 批量写入KV - 使用HMSET
        if (!propIdMap.isEmpty()) {
            kvrocksClient.asyncHMSet(propIdKey, propIdMap);
        }

        String originalKey = cluster
                ? "{" + CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP + "}"
                : CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP;

        if (!originalMap.isEmpty()) {
            kvrocksClient.asyncHMSet(originalKey, originalMap);
        }

        LOG.info("批量同步KV成功: appId={}, ZgZgid={}, owner={}, count={}",
                appId, ZgZgid, owner, props.size());
    }

    /**
     * 批量更新缓存（用于已存在的属性）
     */
    private void batchUpdateCache(Integer appId, String owner,
                                  Map<String, PropResult> propResults,
                                  List<PropInfo> props) {
        for (PropInfo prop : props) {
            PropResult result = propResults.get(prop.getPropName());
            if (result != null) {
                cacheService.putUserPropIdCache(appId, owner, prop.getPropName(), result.getPropId());
            }
        }
    }

    /**
     * 批量检查黑名单并更新数据
     * 并发检查所有属性的黑名单状态，然后统一更新pr对象
     */
    private CompletableFuture<Void> batchCheckBlacklistAndUpdateData(
            List<PropInfo> props, Map<String, PropResult> propResults) {

        // 收集所有需要检查的属性 ID
        Set<Integer> propIds = propResults.values().stream()
                .map(PropResult::getPropId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (propIds.isEmpty()) {
            // 所有属性都创建失败，移除它们
            for (PropInfo prop : props) {
                prop.getPr().remove(prop.getOriginalKey());
            }
            return CompletableFuture.completedFuture(null);
        }

        // 批量检查黑名单
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (PropInfo prop : props) {
            PropResult result = propResults.get(prop.getKey());
            if (result == null) {
                // 属性创建失败，移除
                prop.getPr().remove(prop.getOriginalKey());
                continue;
            }

            CompletableFuture<Void> future = cacheService.isBlackUserProp(result.getPropId())
                    .thenAccept(isBlack -> {
                        if (isBlack) {
                            // 黑名单属性，移除
                            prop.getPr().remove(prop.getOriginalKey());
                        } else {
                            // 正常属性，写入属性ID和原始名称
                            enrichPropMap(prop, result.getPropId(), result.getOriginalName());
                        }
                    })
                    .exceptionally(ex -> {
                        LOG.error("检查黑名单失败: propId={}", result.getPropId(), ex);
                        // 出错时保守处理，移除该属性
                        prop.getPr().remove(prop.getOriginalKey());
                        return null;
                    });

            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private void enrichPropMap(PropInfo prop, Integer propId, String originalName) {
        Object value = prop.getPr().get(prop.getOriginalKey());
        prop.getPr().remove(prop.getOriginalKey());

        // 使用原始名称（保持大小写）
        String newKey = originalName != null ? "_" + originalName : prop.getOriginalKey();
        prop.getPr().put(newKey, value);
        prop.getPr().put("$zg_upid#" + newKey, propId);
    }

    private int getObjectType(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return 1;
        return 0;
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[UserPropAsyncOperator] 处理超时");
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
        LOG.info("[UserPropAsyncOperator-{}] 关闭, newProp={}, lockWait={}",
                getRuntimeContext().getIndexOfThisSubtask(),
                newPropCount.get(), lockWaitCount.get());
    }
}