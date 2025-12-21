package com.zhugeio.etl.common.client.kvrocks;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * KVRocks客户端 - Lettuce真异步版本 + 分布式原子操作支持
 *
 *  优势:
 * 1. 真正的异步IO,基于Netty
 * 2. 无需线程池,不会阻塞
 * 3. 支持百万级并发
 * 4.  新增: SETNX/HSETNX 原子操作,解决分布式并发问题
 * 
 *  修复 (相比原版):
 * 1. 集群模式下连接也复用，不再每次 init() 都创建新连接
 * 2. 单机模式下也使用共享连接
 * 3. 添加连接状态检查
 */
public class KvrocksClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(KvrocksClient.class);
    private static final long serialVersionUID = 1L;

    private final String host;
    private final int port;
    private final boolean isCluster;

    // Lettuce客户端 (transient, 在每个TaskManager上重新初始化)
    private transient RedisClusterClient clusterClient;
    private transient RedisClient standaloneClient;
    private transient StatefulRedisClusterConnection<String, String> clusterConnection;
    private transient StatefulRedisConnection<String, String> standaloneConnection;

    public KvrocksClient(String host, int port, boolean isCluster) {
        this.host = host;
        this.port = port;
        this.isCluster = isCluster;
    }

    //  修复: 静态共享连接 (集群模式)
    private static volatile RedisClusterClient sharedClusterClient;
    private static volatile StatefulRedisClusterConnection<String, String> sharedClusterConnection;
    
    //  修复: 静态共享连接 (单机模式)
    private static volatile RedisClient sharedStandaloneClient;
    private static volatile StatefulRedisConnection<String, String> sharedStandaloneConnection;
    
    private static final Object CLUSTER_LOCK = new Object();
    private static final Object STANDALONE_LOCK = new Object();
    
    //  修复: 连接状态标记
    private static final AtomicBoolean clusterInitialized = new AtomicBoolean(false);
    private static final AtomicBoolean standaloneInitialized = new AtomicBoolean(false);

    /**
     * 初始化Lettuce连接
     * 
     *  修复: 集群和单机模式都使用共享连接
     */
    public void init() {
        if (isCluster) {
            initClusterConnection();
        } else {
            initStandaloneConnection();
        }
    }

    /**
     *  修复: 集群模式连接初始化 - 真正复用连接
     */
    private void initClusterConnection() {
        // 快速检查 (无锁)
        if (clusterInitialized.get() && sharedClusterConnection != null && sharedClusterConnection.isOpen()) {
            this.clusterConnection = sharedClusterConnection;
            this.clusterClient = sharedClusterClient;
            LOG.debug("复用已有集群连接: {}:{}", host, port);
            return;
        }

        synchronized (CLUSTER_LOCK) {
            // 双重检查
            if (clusterInitialized.get() && sharedClusterConnection != null && sharedClusterConnection.isOpen()) {
                this.clusterConnection = sharedClusterConnection;
                this.clusterClient = sharedClusterClient;
                return;
            }

            // 创建新连接
            LOG.info("创建新的集群连接: {}:{}", host, port);
            
            sharedClusterClient = RedisClusterClient.create(
                    RedisURI.Builder
                            .redis(host, port)
                            .withTimeout(Duration.ofSeconds(60))
                            .build()
            );

            sharedClusterClient.setOptions(ClusterClientOptions.builder()
                    .autoReconnect(true)
                    .maxRedirects(8)
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(60000)))
                    .build());

            sharedClusterConnection = sharedClusterClient.connect();
            
            this.clusterConnection = sharedClusterConnection;
            this.clusterClient = sharedClusterClient;
            
            clusterInitialized.set(true);
            LOG.info(" Lettuce集群连接初始化成功: {}:{} (共享复用模式)", host, port);
        }
    }

    /**
     *  修复: 单机模式连接初始化 - 也复用连接
     */
    private void initStandaloneConnection() {
        // 快速检查 (无锁)
        if (standaloneInitialized.get() && sharedStandaloneConnection != null && sharedStandaloneConnection.isOpen()) {
            this.standaloneConnection = sharedStandaloneConnection;
            this.standaloneClient = sharedStandaloneClient;
            LOG.debug("复用已有单机连接: {}:{}", host, port);
            return;
        }

        synchronized (STANDALONE_LOCK) {
            // 双重检查
            if (standaloneInitialized.get() && sharedStandaloneConnection != null && sharedStandaloneConnection.isOpen()) {
                this.standaloneConnection = sharedStandaloneConnection;
                this.standaloneClient = sharedStandaloneClient;
                return;
            }

            // 创建新连接
            LOG.info("创建新的单机连接: {}:{}", host, port);
            
            sharedStandaloneClient = RedisClient.create(
                    RedisURI.Builder
                            .redis(host, port)
                            .withTimeout(Duration.ofSeconds(60))
                            .build()
            );

            sharedStandaloneClient.setOptions(ClientOptions.builder()
                    .autoReconnect(true)
                    .pingBeforeActivateConnection(true)
                    .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(60000)))
                    .build());

            sharedStandaloneConnection = sharedStandaloneClient.connect();
            
            this.standaloneConnection = sharedStandaloneConnection;
            this.standaloneClient = sharedStandaloneClient;
            
            standaloneInitialized.set(true);
            LOG.info(" Lettuce单机连接初始化成功: {}:{} (共享复用模式)", host, port);
        }
    }

    // ==================== 以下方法保持原版不变 ====================

    /**
     *  真正的异步Get操作 (基于Netty,无阻塞)
     */
    public CompletableFuture<String> asyncGet(String key) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks GET失败", ex);
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks GET失败", ex);
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     *  真正的异步Set操作 (修复类型转换)
     */
    public CompletableFuture<Void> asyncSet(String key, String value) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks SET失败", ex);
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            throw new RuntimeException("KVRocks SET失败", ex);
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     *  异步 SETEX (带过期时间的SET)
     */
    public CompletableFuture<Void> asyncSetEx(String key, long seconds, String value) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.setex(key, seconds, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SETEX失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.setex(key, seconds, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SETEX失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     *  原子性的 SETNX (SET if Not eXists)
     * 只在key不存在时设置值
     *
     * @return true表示设置成功(之前不存在), false表示key已存在
     */
    public CompletableFuture<Boolean> asyncSetIfAbsent(String key, String value) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                return async.setnx(key, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SETNX失败: {}, {}", key, ex.getMessage());
                            return false;  // 失败时返回false
                        });
            } else {
                RedisAsyncCommands<String, String> async =
                        standaloneConnection.async();
                return async.setnx(key, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SETNX失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks SETNX异常: {}, {}", key, e.getMessage());
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     *  异步Hash Get
     */
    public CompletableFuture<String> asyncHGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     *  异步Hash Set (修复类型转换)
     */
    public CompletableFuture<Void> asyncHSet(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> null);
            } else {
                return standaloneConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     *  异步Hash GetAll
     */
    public CompletableFuture<Map<String, String>> asyncHGetAll(String key) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async = 
                        clusterConnection.async();
                return async.hgetall(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGETALL失败: {}, {}", key, ex.getMessage());
                            return new HashMap<>();
                        });
            } else {
                RedisAsyncCommands<String, String> async = 
                        standaloneConnection.async();
                return async.hgetall(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGETALL失败: {}, {}", key, ex.getMessage());
                            return new HashMap<>();
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks HGETALL异常: {}, {}", key, e.getMessage());
            return CompletableFuture.completedFuture(new HashMap<>());
        }
    }

    /**
     *  原子性的 HSETNX (Hash SET if Not eXists)
     * 只在hash的field不存在时设置值
     *
     * @return true表示设置成功(field之前不存在), false表示field已存在
     */
    public CompletableFuture<Boolean> asyncHSetIfAbsent(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().hsetnx(key, field, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSETNX失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            } else {
                return standaloneConnection.async().hsetnx(key, field, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSETNX失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks HSETNX异常: {}:{}, {}", key, field, e.getMessage());
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     *  批量Hash Get (真正的Pipeline)
     */
    public CompletableFuture<Map<String, String>> asyncBatchHGet(String key, List<String> fields) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();

                // Pipeline多个HGET命令
                List<RedisFuture<String>> futures = fields.stream()
                        .map(field -> async.hget(key, field))
                        .collect(Collectors.toList());

                // 等待所有完成
                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        try {
                            String value = futures.get(i).get();
                            if (value != null) {
                                result.put(fields.get(i), value);
                            }
                        } catch (Exception ignored) {}
                    }
                    return result;
                });

            } else {
                // 单机模式使用真正的Pipeline
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);  // 开启Pipeline

                List<RedisFuture<String>> futures = fields.stream()
                        .map(field -> async.hget(key, field))
                        .collect(Collectors.toList());

                async.flushCommands();  // 一次性发送
                async.setAutoFlushCommands(true);

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> {
                    Map<String, String> result = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        try {
                            String value = futures.get(i).get();
                            if (value != null) {
                                result.put(fields.get(i), value);
                            }
                        } catch (Exception ignored) {}
                    }
                    return result;
                });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(new HashMap<>());
        }
    }

    /**
     *  批量Hash Set (修复类型转换)
     */
    public CompletableFuture<Void> asyncBatchHSet(String key, Map<String, String> fieldValues) {
        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();

                List<RedisFuture<Boolean>> futures = fieldValues.entrySet().stream()
                        .map(entry -> async.hset(key, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> (Void) null);

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = fieldValues.entrySet().stream()
                        .map(entry -> async.hset(key, entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList());

                async.flushCommands();
                async.setAutoFlushCommands(true);

                return CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).thenApply(v -> (Void) null);
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 测试连接
     */
    public boolean testConnection() {
        try {
            if (isCluster) {
                String pong = clusterConnection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            } else {
                String pong = standaloneConnection.sync().ping();
                return "PONG".equalsIgnoreCase(pong);
            }
        } catch (Exception e) {
            LOG.error("KVRocks连接测试失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     *  同步Hash Get
     */
    public String hGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.sync().hget(key, field);
            } else {
                return standaloneConnection.sync().hget(key, field);
            }
        } catch (Exception e) {
            LOG.error("KVRocks HGET失败: {}:{}", key, field, e.getMessage());
            return null;
        }
    }

    /**
     *  同步批量管道查询 (Window算子专用)
     */
    public Map<String, String> syncBatchHGet(String hashKey, List<String> fields, long timeoutMs) {
        Map<String, String> results = new HashMap<>();

        if (fields == null || fields.isEmpty()) {
            return results;
        }

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<String>> futures = new ArrayList<>();
                for (String field : fields) {
                    futures.add(async.hget(hashKey, field));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                for (int i = 0; i < fields.size(); i++) {
                    try {
                        String value = futures.get(i).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (value != null) {
                            results.put(hashKey + ":" + fields.get(i), value);
                        }
                    } catch (Exception e) {
                        // 单个查询失败不影响其他
                    }
                }

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<String>> futures = new ArrayList<>();
                for (String field : fields) {
                    futures.add(async.hget(hashKey, field));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                for (int i = 0; i < fields.size(); i++) {
                    try {
                        String value = futures.get(i).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
                        if (value != null) {
                            results.put(hashKey + ":" + fields.get(i), value);
                        }
                    } catch (Exception e) {
                        // 继续
                    }
                }
            }

        } catch (Exception e) {
            LOG.error("批量查询失败: {}, {}", hashKey, e.getMessage());
        }

        return results;
    }

    /**
     *  同步批量管道写入 (Window算子专用)
     */
    public void syncBatchHSet(String hashKey, Map<String, String> fieldValues, long timeoutMs) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            return;
        }

        try {
            if (isCluster) {
                RedisAdvancedClusterAsyncCommands<String, String> async =
                        clusterConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
                    futures.add(async.hset(hashKey, entry.getKey(), entry.getValue()));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            } else {
                RedisAsyncCommands<String, String> async = standaloneConnection.async();
                async.setAutoFlushCommands(false);

                List<RedisFuture<Boolean>> futures = new ArrayList<>();
                for (Map.Entry<String, String> entry : fieldValues.entrySet()) {
                    futures.add(async.hset(hashKey, entry.getKey(), entry.getValue()));
                }

                async.flushCommands();
                async.setAutoFlushCommands(true);

                CompletableFuture.allOf(
                        futures.stream()
                                .map(RedisFuture::toCompletableFuture)
                                .toArray(CompletableFuture[]::new)
                ).get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

        } catch (Exception e) {
            LOG.error("批量写入失败: {}, {}", hashKey, e.getMessage());
        }
    }

    // ==================== Set 操作 ====================

    /**
     *  异步 SISMEMBER (检查成员是否在集合中)
     */
    public CompletableFuture<Boolean> asyncSIsMember(String key, String member) {
        try {
            if (isCluster) {
                return clusterConnection.async().sismember(key, member)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SISMEMBER失败: {}:{}, {}", key, member, ex.getMessage());
                            return false;
                        });
            } else {
                return standaloneConnection.async().sismember(key, member)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SISMEMBER失败: {}:{}, {}", key, member, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     *  异步 SADD (添加成员到集合)
     */
    public CompletableFuture<Long> asyncSAdd(String key, String... members) {
        try {
            if (isCluster) {
                return clusterConnection.async().sadd(key, members)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SADD失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            } else {
                return standaloneConnection.async().sadd(key, members)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SADD失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(0L);
        }
    }

    /**
     *  异步 SMEMBERS (获取集合所有成员)
     */
    public CompletableFuture<Set<String>> asyncSMembers(String key) {
        try {
            if (isCluster) {
                return clusterConnection.async().smembers(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SMEMBERS失败: {}, {}", key, ex.getMessage());
                            return Collections.emptySet();
                        });
            } else {
                return standaloneConnection.async().smembers(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SMEMBERS失败: {}, {}", key, ex.getMessage());
                            return Collections.emptySet();
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    /**
     * 关闭连接
     * 
     *  修复: 实例关闭时不关闭共享连接，共享连接由 shutdownAll() 管理
     */
    public void shutdown() {
        // 实例级别的关闭不再关闭共享连接
        LOG.debug("KvrocksClient 实例关闭 (共享连接保持)");
    }

    /**
     *  新增: 关闭所有共享连接 (JVM 关闭时调用)
     */
    public static void shutdownAll() {
        LOG.info("关闭所有 KVRocks 共享连接...");
        
        synchronized (CLUSTER_LOCK) {
            if (sharedClusterConnection != null) {
                try {
                    sharedClusterConnection.close();
                } catch (Exception e) {
                    LOG.warn("关闭集群连接失败", e);
                }
                sharedClusterConnection = null;
            }
            if (sharedClusterClient != null) {
                try {
                    sharedClusterClient.shutdown();
                } catch (Exception e) {
                    LOG.warn("关闭集群客户端失败", e);
                }
                sharedClusterClient = null;
            }
            clusterInitialized.set(false);
        }

        synchronized (STANDALONE_LOCK) {
            if (sharedStandaloneConnection != null) {
                try {
                    sharedStandaloneConnection.close();
                } catch (Exception e) {
                    LOG.warn("关闭单机连接失败", e);
                }
                sharedStandaloneConnection = null;
            }
            if (sharedStandaloneClient != null) {
                try {
                    sharedStandaloneClient.shutdown();
                } catch (Exception e) {
                    LOG.warn("关闭单机客户端失败", e);
                }
                sharedStandaloneClient = null;
            }
            standaloneInitialized.set(false);
        }
        
        LOG.info("所有 KVRocks 共享连接已关闭");
    }

    // 注册 ShutdownHook
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("JVM 关闭，清理 KVRocks 连接...");
            shutdownAll();
        }, "kvrocks-shutdown-hook"));
    }
}
