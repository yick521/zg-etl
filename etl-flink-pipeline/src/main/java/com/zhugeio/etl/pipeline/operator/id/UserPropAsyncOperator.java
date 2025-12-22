package com.zhugeio.etl.pipeline.operator.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import java.util.concurrent.TimeUnit;

/**
 * 用户自定义属性异步处理算子
 * <p>
 * 核心功能：
 * 1. 从 KV 中查询用户属性ID
 * 2. 如果不存在则异步插入到 MySQL
 * 3. 检查黑名单过滤
 * 4. 使用本地缓存加速
 */
public class UserPropAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(UserPropAsyncOperator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final int maxPropLength;

    // KV 相关
    private transient KvrocksClient kvrocks;
    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;


    // 缓存相关
    private transient Cache<String, UserPropMeta> propCache;  // 正常属性缓存
    private transient Cache<String, Boolean> blacklistCache;  // 黑名单缓存
    private transient Cache<String, String> originalNameCache; // 原始名称缓存（大小写映射）

    // MySQL 连接池
    private transient HikariDataSource dataSource;
    private final Properties dbProperties;


    public UserPropAsyncOperator(String kvrocksHost, int kvrocksPort,
                                 boolean kvrocksCluster, Properties dbProperties,
                                 int maxPropLength) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.dbProperties = dbProperties;
        this.maxPropLength = maxPropLength;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        // 初始化 KVRocks 客户端
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        if (!kvrocks.testConnection()) {
            throw new RuntimeException("KVRocks连接失败!");
        }

        // 初始化本地缓存
        propCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();

        blacklistCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(30, TimeUnit.MINUTES)
                .build();

        originalNameCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        // 初始化 MySQL 连接池
        initializeConnectionPool();

        LOG.info("UserPropAsyncOperator 初始化完成");
    }

    private void initializeConnectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbProperties.getProperty("rdbms.url"));
        config.setUsername(dbProperties.getProperty("rdbms.userName"));
        config.setPassword(dbProperties.getProperty("rdbms.password"));
        config.setDriverClassName(dbProperties.getProperty("rdbms.driverClass"));

        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000L);
        config.setIdleTimeout(600000L);
        config.setMaxLifetime(1800000L);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("UserProp-MySQL-Pool-" + getRuntimeContext().getIndexOfThisSubtask());

        dataSource = new HikariDataSource(config);
        LOG.info("MySQL连接池初始化成功");
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        try {
            // 提取用户属性信息
            List<UserPropRequest> propRequests = extractUserProps(input);

            if (propRequests.isEmpty()) {
                // 没有用户属性需要处理
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 异步处理所有用户属性
            CompletableFuture<ZGMessage> future = processUserProps(input, propRequests);

            future.whenComplete((output, throwable) -> {
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
     * 从消息中提取用户属性
     */
    private List<UserPropRequest> extractUserProps(ZGMessage msg) {
        List<UserPropRequest> requests = new ArrayList<>();

        if (msg.getResult() == -1) {
            return requests;
        }

        Integer appId = msg.getAppId();
        String owner = String.valueOf(msg.getData().get("owner"));

        @SuppressWarnings("unchecked")
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

            // 提取自定义属性（以 _ 开头的）
            for (String key : new HashSet<>(pr.keySet())) {
                if (key.startsWith("_")) {
                    String propName = key.substring(1);
                    Object propValue = pr.get(key);
                    int propType = getObjectType(propValue);

                    requests.add(new UserPropRequest(
                            appId, owner, propName, propType, pr, key
                    ));
                }
            }
        }

        return requests;
    }

    /**
     * 处理所有用户属性
     */
    private CompletableFuture<ZGMessage> processUserProps(ZGMessage input,
                                                          List<UserPropRequest> requests) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (UserPropRequest req : requests) {
            CompletableFuture<Void> future = processOneProp(req);
            futures.add(future);
        }

        // 等待所有属性处理完成
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> input);
    }

    /**
     * 处理单个用户属性
     */
    private CompletableFuture<Void> processOneProp(UserPropRequest req) {
        // 1. 校验属性名
        if ("null".equals(req.propTypeStr) || req.propName.length() > maxPropLength) {
            req.prMap.remove(req.originalKey);
            return CompletableFuture.completedFuture(null);
        }

        // 2. 生成缓存 key
        String cacheKey = buildCacheKey(req.appId, req.owner, req.propName);

        // 3. 检查本地缓存
        UserPropMeta cached = propCache.getIfPresent(cacheKey);
        if (cached != null) {
            // 缓存命中
            if (isInBlacklist(cached.getId())) {
                req.prMap.remove(req.originalKey);
                return CompletableFuture.completedFuture(null);
            }
            enrichPropMap(req, cached);
            return CompletableFuture.completedFuture(null);
        }

        // 4. 从 KV 异步查询
        return getUserPropFromKV(req.appId, req.owner, req.propName)
                .thenCompose(propMeta -> {
                    if (propMeta != null) {
                        // KV 中找到了
                        propCache.put(cacheKey, propMeta);

                        if (isInBlacklist(propMeta.getId())) {
                            req.prMap.remove(req.originalKey);
                            return CompletableFuture.completedFuture(null);
                        }

                        enrichPropMap(req, propMeta);
                        return CompletableFuture.completedFuture(null);
                    } else {
                        // KV 中没有，需要插入数据库
                        return insertUserPropToDB(req)
                                .thenCompose(newPropMeta -> {
                                    if (newPropMeta != null) {
                                        propCache.put(cacheKey, newPropMeta);

                                        if (isInBlacklist(newPropMeta.getId())) {
                                            req.prMap.remove(req.originalKey);
                                            return CompletableFuture.completedFuture(null);
                                        }

                                        enrichPropMap(req, newPropMeta);
                                    } else {
                                        req.prMap.remove(req.originalKey);
                                    }
                                    return CompletableFuture.completedFuture(null);
                                });
                    }
                });
    }

    /**
     * 从 KV 查询用户属性
     */
    private CompletableFuture<UserPropMeta> getUserPropFromKV(Integer appId, String owner, String propName) {
        String hashKey = "user_prop_meta";

        // 使用 appId_owner_propName(大写) 作为复合键
        String upperPropName = propName.toUpperCase();

        return kvrocks.asyncHGetAll(hashKey)
                .thenApply(map -> {
                    if (map == null || map.isEmpty()) {
                        return null;
                    }

                    // 遍历所有记录，找到匹配的
                    for (Map.Entry<String, String> entry : map.entrySet()) {
                        try {
                            UserPropMeta meta = OBJECT_MAPPER.readValue(entry.getValue(), UserPropMeta.class);

                            if (meta.getAppId().equals(appId) &&
                                    meta.getOwner().equals(owner) &&
                                    meta.getName().equalsIgnoreCase(propName)) {

                                // 记录原始名称（大小写）
                                String origKey = buildCacheKey(appId, owner, meta.getId());
                                originalNameCache.put(origKey, meta.getName());

                                return meta;
                            }
                        } catch (Exception e) {
                            LOG.warn("解析 UserPropMeta JSON 失败: {}", entry.getValue(), e);
                        }
                    }

                    return null;
                })
                .exceptionally(throwable -> {
                    LOG.error("从 KV 查询用户属性失败", throwable);
                    return null;
                });
    }

    /**
     * 插入用户属性到数据库（异步）
     */
    private CompletableFuture<UserPropMeta> insertUserPropToDB(UserPropRequest req) {
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
                String selectSql = "SELECT id, app_id, owner, name, type, is_delete FROM user_prop_meta " +
                        "WHERE app_id = ? AND owner = ? AND name = ?";
                try (PreparedStatement stmt = conn.prepareStatement(selectSql)) {
                    stmt.setInt(1, req.appId);
                    stmt.setString(2, req.owner);
                    stmt.setString(3, req.propName);

                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        UserPropMeta meta = new UserPropMeta();
                        meta.setId(rs.getInt("id"));
                        meta.setAppId(rs.getInt("app_id"));
                        meta.setOwner(rs.getString("owner"));
                        meta.setName(rs.getString("name"));
                        meta.setType(rs.getInt("type"));
                        meta.setIsDelete(rs.getInt("is_delete"));

                        LOG.debug("成功插入用户属性: appId={}, owner={}, propName={}, id={}",
                                req.appId, req.owner, req.propName, meta.getId());

                        return meta;
                    }
                }

                return null;

            } catch (Exception e) {
                LOG.error("插入用户属性到数据库失败: appId={}, owner={}, propName={}",
                        req.appId, req.owner, req.propName, e);
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
        });
    }

    /**
     * 检查是否在黑名单中
     */
    private boolean isInBlacklist(Integer propId) {
        String blackKey = "blacklist_" + propId;
        Boolean cached = blacklistCache.getIfPresent(blackKey);

        if (cached != null) {
            return cached;
        }

        // 从 KV 查询黑名单（同步查询，因为这个数据量小）
        try {
            String hashKey = "user_prop_meta";
            String value = kvrocks.hGet(hashKey, String.valueOf(propId));

            if (value != null) {
                UserPropMeta meta = OBJECT_MAPPER.readValue(value, UserPropMeta.class);
                boolean isBlack = meta.getIsDelete() == 1;
                blacklistCache.put(blackKey, isBlack);
                return isBlack;
            }
        } catch (Exception e) {
            LOG.warn("查询黑名单失败: propId={}", propId, e);
        }

        return false;
    }

    /**
     * 填充属性信息到 map
     */
    private void enrichPropMap(UserPropRequest req, UserPropMeta meta) {
        Map<String, Object> pr = req.prMap;
        String propName = req.propName;

        // 检查是否需要替换为原始名称（大小写）
        String origKey = buildCacheKey(req.appId, req.owner, meta.getId());
        String originalName = originalNameCache.getIfPresent(origKey);

        if (originalName != null && !propName.equals(originalName)) {
            // 替换为原始名称
            Object value = pr.remove("_" + propName);
            pr.put("_" + originalName, value);
            propName = originalName;
        }

        // 添加属性 ID 和类型
        pr.put("$zg_upid#_" + propName, meta.getId());
        pr.put("$zg_uptp#_" + propName, req.propTypeStr);
    }

    /**
     * 获取对象类型
     */
    private int getObjectType(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number) {
            return 1;
        }
        if (value instanceof String) {
            return 2;
        }
        if (value instanceof Boolean) {
            return 3;
        }
        return 0;
    }

    /**
     * 构建缓存 key
     */
    private String buildCacheKey(Object... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append("_");
            sb.append(parts[i]);
        }
        return sb.toString();
    }

    @Override
    public void close() {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("MySQL连接池已关闭");
        }

        if (propCache != null) {
            LOG.info("属性缓存统计: {}", propCache.stats());
        }
    }

    /**
     * 用户属性请求
     */
    private static class UserPropRequest {
        Integer appId;
        String owner;
        String propName;
        int propType;
        String propTypeStr;
        Map<String, Object> prMap;
        String originalKey;

        UserPropRequest(Integer appId, String owner, String propName,
                        int propType, Map<String, Object> prMap, String originalKey) {
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