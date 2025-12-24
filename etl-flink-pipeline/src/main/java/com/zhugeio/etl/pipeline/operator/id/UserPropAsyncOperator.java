package com.zhugeio.etl.pipeline.operator.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheKeyConstants;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.model.UserPropMeta;
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

/**
 * 用户属性处理算子 - 改造版
 * 
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 3. 黑名单查询从 Hash 改为 Set (asyncSIsMember)
 * 
 * KVRocks Key 对照 (与同步服务一致):
 * - appIdPropIdMap (Hash): field=appId_owner_propName(大写)
 * - appIdPropIdOriginalMap (Hash): field=appId_owner_propId
 * - blackUserPropSet (Set): member=propId
 */
public class UserPropAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(UserPropAsyncOperator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;

    private CacheConfig cacheConfig;
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

        initializeConnectionPool();

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
            List<UserPropRequest> propRequests = extractUserProps(input);

            if (propRequests.isEmpty()) {
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            processUserProps(input, propRequests)
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

    @SuppressWarnings("unchecked")
    private List<UserPropRequest> extractUserProps(ZGMessage msg) {
        List<UserPropRequest> requests = new ArrayList<>();

        if (msg.getResult() == -1) {
            return requests;
        }

        Integer appId = msg.getAppId();
        String owner = String.valueOf(msg.getData().get("owner"));

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) msg.getData().get("data");

        if (dataList == null) {
            return requests;
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

            for (String key : new HashSet<>(pr.keySet())) {
                if (key.startsWith("_")) {
                    String propName = key.substring(1);
                    Object propValue = pr.get(key);
                    int propType = getObjectType(propValue);

                    requests.add(new UserPropRequest(appId, owner, propName, propType, pr, key));
                }
            }
        }

        return requests;
    }

    private CompletableFuture<ZGMessage> processUserProps(ZGMessage input, List<UserPropRequest> requests) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (UserPropRequest req : requests) {
            CompletableFuture<Void> future = processOneProp(req);
            futures.add(future);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> input);
    }

    private CompletableFuture<Void> processOneProp(UserPropRequest req) {
        if ("null".equals(req.propTypeStr) || req.propName.length() > maxPropLength) {
            req.prMap.remove(req.originalKey);
            return CompletableFuture.completedFuture(null);
        }

        // 使用统一缓存服务获取用户属性ID
        return cacheService.getUserPropId(req.appId, req.owner, req.propName)
                .thenCompose(propId -> {
                    if (propId != null) {
                        // 检查黑名单
                        return cacheService.isBlackUserProp(propId)
                                .thenCompose(isBlack -> {
                                    if (isBlack) {
                                        req.prMap.remove(req.originalKey);
                                        return CompletableFuture.completedFuture(null);
                                    }

                                    // 获取原始名称
                                    return cacheService.getUserPropOriginalName(req.appId, req.owner, propId)
                                            .thenAccept(originalName -> {
                                                enrichPropMap(req, propId, originalName);
                                            });
                                });
                    } else {
                        // 需要插入数据库
                        return insertUserPropToDB(req)
                                .thenCompose(newPropId -> {
                                    if (newPropId != null) {
                                        return cacheService.isBlackUserProp(newPropId)
                                                .thenAccept(isBlack -> {
                                                    if (isBlack) {
                                                        req.prMap.remove(req.originalKey);
                                                    } else {
                                                        enrichPropMap(req, newPropId, req.propName);
                                                    }
                                                });
                                    } else {
                                        req.prMap.remove(req.originalKey);
                                        return CompletableFuture.completedFuture(null);
                                    }
                                });
                    }
                });
    }

    private CompletableFuture<Integer> insertUserPropToDB(UserPropRequest req) {
        return CompletableFuture.supplyAsync(() -> {
            Connection conn = null;
            try {
                conn = dataSource.getConnection();

                // 插入用户属性
                String insertSql = "INSERT IGNORE INTO user_prop_meta(app_id, owner, name, type) VALUES(?, ?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                    stmt.setInt(1, req.appId);
                    stmt.setString(2, req.owner);
                    stmt.setString(3, req.propName);
                    stmt.setInt(4, req.propType);
                    stmt.executeUpdate();
                }

                // 查询刚插入的记录
                String selectSql = "SELECT id FROM user_prop_meta WHERE app_id = ? AND owner = ? AND name = ?";
                try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                    stmt.setInt(1, req.appId);
                    stmt.setString(2, req.owner);
                    stmt.setString(3, req.propName);

                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        int propId = rs.getInt("id");

                        // 更新统一缓存
                        cacheService.putUserPropIdCache(req.appId, req.owner, req.propName, propId);

                        // 同步到 KVRocks
                        syncUserPropToKVRocks(req.appId, req.owner, req.propName, propId);

                        LOG.info("创建用户属性成功: appId={}, propName={}, propId={}", 
                                req.appId, req.propName, propId);
                        return propId;
                    }
                }

                return null;

            } catch (Exception e) {
                LOG.error("插入用户属性失败: appId={}, propName={}", req.appId, req.propName, e);
                return null;
            } finally {
                if (conn != null) {
                    try { conn.close(); } catch (Exception ignored) {}
                }
            }
        });
    }

    /**
     * 同步到 KVRocks (使用与同步服务一致的 Key 格式)
     */
    private void syncUserPropToKVRocks(Integer appId, String owner, String propName, Integer propId) {
        boolean cluster = cacheConfig.isKvrocksCluster();

        // Key: appIdPropIdMap, Field: appId_owner_propName(大写)
        String propIdKey = cluster
                ? "{" + CacheKeyConstants.APP_ID_PROP_ID_MAP + "}"
                : CacheKeyConstants.APP_ID_PROP_ID_MAP;
        String propIdField = CacheKeyConstants.userPropIdField(appId, owner, propName);
        kvrocksClient.asyncHSet(propIdKey, propIdField, String.valueOf(propId));

        // Key: appIdPropIdOriginalMap, Field: appId_owner_propId
        String originalKey = cluster
                ? "{" + CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP + "}"
                : CacheKeyConstants.APP_ID_PROP_ID_ORIGINAL_MAP;
        String originalField = CacheKeyConstants.userPropOriginalField(appId, owner, propId);
        kvrocksClient.asyncHSet(originalKey, originalField, propName);
    }

    private void enrichPropMap(UserPropRequest req, Integer propId, String originalName) {
        Object value = req.prMap.get(req.originalKey);
        req.prMap.remove(req.originalKey);

        // 使用原始名称（保持大小写）
        String newKey = originalName != null ? "_" + originalName : req.originalKey;
        req.prMap.put(newKey, value);
        req.prMap.put("$zg_upid#" + newKey, propId);
    }

    private int getObjectType(Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Number) return 1;
        if (obj instanceof String) return 0;
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
        LOG.info("[UserPropAsyncOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }

    // 内部请求类
    private static class UserPropRequest {
        final Integer appId;
        final String owner;
        final String propName;
        final int propType;
        final String propTypeStr;
        final Map<String, Object> prMap;
        final String originalKey;

        UserPropRequest(Integer appId, String owner, String propName, int propType,
                        Map<String, Object> prMap, String originalKey) {
            this.appId = appId;
            this.owner = owner;
            this.propName = propName;
            this.propType = propType;
            this.propTypeStr = String.valueOf(propType);
            this.prMap = prMap;
            this.originalKey = originalKey;
        }
    }
}
