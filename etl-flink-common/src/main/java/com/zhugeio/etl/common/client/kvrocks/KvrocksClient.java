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

/**
 * KVRocks客户端 - Lettuce真异步版本 + 分布式原子操作支持
 *
 * 优势:
 * 1. 真正的异步IO,基于Netty
 * 2. 无需线程池,不会阻塞
 * 3. 支持百万级并发
 * 4. 新增: SETNX/HSETNX 原子操作,解决分布式并发问题
 * 5. 新增: SET NX PX / EVAL 支持分布式锁
 * 6. 集群模式下连接也复用，不再每次 init() 都创建新连接
 * 7. 单机模式下也使用共享连接
 * 8. 添加连接状态检查
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

    // 静态共享连接 (集群模式)
    private static volatile RedisClusterClient sharedClusterClient;
    private static volatile StatefulRedisClusterConnection<String, String> sharedClusterConnection;

    // 静态共享连接 (单机模式)
    private static volatile RedisClient sharedStandaloneClient;
    private static volatile StatefulRedisConnection<String, String> sharedStandaloneConnection;

    private static final Object CLUSTER_LOCK = new Object();
    private static final Object STANDALONE_LOCK = new Object();

    // 连接状态标记
    private static final AtomicBoolean clusterInitialized = new AtomicBoolean(false);
    private static final AtomicBoolean standaloneInitialized = new AtomicBoolean(false);

    /**
     * 初始化Lettuce连接
     */
    public void init() {
        if (isCluster) {
            initClusterConnection();
        } else {
            initStandaloneConnection();
        }
    }

    /**
     * 集群模式连接初始化 - 真正复用连接
     */
    private void initClusterConnection() {
        if (clusterInitialized.get() && sharedClusterConnection != null && sharedClusterConnection.isOpen()) {
            this.clusterConnection = sharedClusterConnection;
            this.clusterClient = sharedClusterClient;
            LOG.debug("复用已有集群连接: {}:{}", host, port);
            return;
        }

        synchronized (CLUSTER_LOCK) {
            if (clusterInitialized.get() && sharedClusterConnection != null && sharedClusterConnection.isOpen()) {
                this.clusterConnection = sharedClusterConnection;
                this.clusterClient = sharedClusterClient;
                return;
            }

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
            LOG.info("Lettuce集群连接初始化成功: {}:{} (共享复用模式)", host, port);
        }
    }

    /**
     * 单机模式连接初始化 - 也复用连接
     */
    private void initStandaloneConnection() {
        if (standaloneInitialized.get() && sharedStandaloneConnection != null && sharedStandaloneConnection.isOpen()) {
            this.standaloneConnection = sharedStandaloneConnection;
            this.standaloneClient = sharedStandaloneClient;
            LOG.debug("复用已有单机连接: {}:{}", host, port);
            return;
        }

        synchronized (STANDALONE_LOCK) {
            if (standaloneInitialized.get() && sharedStandaloneConnection != null && sharedStandaloneConnection.isOpen()) {
                this.standaloneConnection = sharedStandaloneConnection;
                this.standaloneClient = sharedStandaloneClient;
                return;
            }

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
            LOG.info("Lettuce单机连接初始化成功: {}:{} (共享复用模式)", host, port);
        }
    }

    /**
     * 测试连接
     */
    public boolean testConnection() {
        try {
            String result;
            if (isCluster) {
                result = clusterConnection.sync().ping();
            } else {
                result = standaloneConnection.sync().ping();
            }
            return "PONG".equalsIgnoreCase(result);
        } catch (Exception e) {
            LOG.error("KVRocks 连接测试失败: {}", e.getMessage());
            return false;
        }
    }

    // ==================== 基础 String 操作 ====================

    /**
     * 异步 GET
     */
    public CompletableFuture<String> asyncGet(String key) {
        try {
            if (isCluster) {
                return clusterConnection.async().get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                return standaloneConnection.async().get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 异步 SET
     */
    public CompletableFuture<Void> asyncSet(String key, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                return standaloneConnection.async().set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    // ==================== 分布式锁相关操作 (新增) ====================

    /**
     * 异步 SET NX PX (分布式锁核心操作)
     *
     * 原子操作: 仅当 key 不存在时设置值，并设置过期时间
     *
     * @param key      锁的 key
     * @param value    锁的值 (通常是唯一标识，用于安全释放)
     * @param expireMs 过期时间 (毫秒)
     * @return 是否设置成功
     */
    public CompletableFuture<Boolean> asyncSetNxPx(String key, String value, long expireMs) {
        try {
            SetArgs args = SetArgs.Builder.nx().px(expireMs);

            if (isCluster) {
                return clusterConnection.async().set(key, value, args)
                        .toCompletableFuture()
                        .thenApply(result -> "OK".equals(result))
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET NX PX失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            } else {
                return standaloneConnection.async().set(key, value, args)
                        .toCompletableFuture()
                        .thenApply(result -> "OK".equals(result))
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET NX PX失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks SET NX PX异常: {}", key, e);
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * 异步 EVAL (执行 Lua 脚本)
     *
     * 用于分布式锁的安全释放等原子操作
     *
     * @param script Lua 脚本
     * @param key    key
     * @param value  value (ARGV[1])
     * @return 脚本执行结果
     */
    public CompletableFuture<Object> asyncEval(String script, String key, String value) {
        try {
            String[] keys = new String[]{key};
            String[] args = new String[]{value};

            if (isCluster) {
                return clusterConnection.async().eval(script, ScriptOutputType.INTEGER, keys, args)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks EVAL失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                return standaloneConnection.async().eval(script, ScriptOutputType.INTEGER, keys, args)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks EVAL失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks EVAL异常: {}", key, e);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 异步 DEL (删除 key)
     *
     * @param key 要删除的 key
     * @return 删除的 key 数量
     */
    public CompletableFuture<Long> asyncDel(String key) {
        try {
            if (isCluster) {
                return clusterConnection.async().del(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks DEL失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            } else {
                return standaloneConnection.async().del(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks DEL失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks DEL异常: {}", key, e);
            return CompletableFuture.completedFuture(0L);
        }
    }

    /**
     * 异步 EXPIRE (设置过期时间)
     *
     * @param key     key
     * @param seconds 过期时间 (秒)
     * @return 是否成功
     */
    public CompletableFuture<Boolean> asyncExpire(String key, long seconds) {
        try {
            if (isCluster) {
                return clusterConnection.async().expire(key, seconds)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks EXPIRE失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            } else {
                return standaloneConnection.async().expire(key, seconds)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks EXPIRE失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            LOG.error("KVRocks EXPIRE异常: {}", key, e);
            return CompletableFuture.completedFuture(false);
        }
    }

    // ==================== Hash 操作 ====================

    /**
     * 异步 HGET
     */
    public CompletableFuture<String> asyncHGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGET失败: {}:{}, {}", key, field, ex.getMessage());
                            return null;
                        });
            } else {
                return standaloneConnection.async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGET失败: {}:{}, {}", key, field, ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 同步 HGET
     *
     * @param key   Hash key
     * @param field Hash field
     * @return 字段值，不存在返回 null
     */
    public String hGet(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.sync().hget(key, field);
            } else {
                return standaloneConnection.sync().hget(key, field);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync HGET失败: {}:{}, {}", key, field, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 HSET
     *
     * @param key   Hash key
     * @param field Hash field
     * @param value 值
     * @return true=新增字段, false=更新字段
     */
    public boolean hSet(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.sync().hset(key, field, value);
            } else {
                return standaloneConnection.sync().hset(key, field, value);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync HSET失败: {}:{}, {}", key, field, e.getMessage());
            return false;
        }
    }

    /**
     * 同步 HGETALL
     *
     * @param key Hash key
     * @return 所有字段和值的 Map
     */
    public Map<String, String> hGetAll(String key) {
        try {
            if (isCluster) {
                return clusterConnection.sync().hgetall(key);
            } else {
                return standaloneConnection.sync().hgetall(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync HGETALL失败: {}, {}", key, e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * 同步 GET
     *
     * @param key Key
     * @return 值，不存在返回 null
     */
    public String get(String key) {
        try {
            if (isCluster) {
                return clusterConnection.sync().get(key);
            } else {
                return standaloneConnection.sync().get(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync GET失败: {}, {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 SET
     *
     * @param key   Key
     * @param value 值
     * @return "OK" 表示成功
     */
    public String set(String key, String value) {
        try {
            if (isCluster) {
                return clusterConnection.sync().set(key, value);
            } else {
                return standaloneConnection.sync().set(key, value);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync SET失败: {}, {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 DEL
     *
     * @param key Key
     * @return 删除的 key 数量
     */
    public long del(String key) {
        try {
            if (isCluster) {
                return clusterConnection.sync().del(key);
            } else {
                return standaloneConnection.sync().del(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync DEL失败: {}, {}", key, e.getMessage());
            return 0;
        }
    }

    /**
     * 同步 EXISTS
     *
     * @param key Key
     * @return 存在的 key 数量
     */
    public long exists(String key) {
        try {
            if (isCluster) {
                return clusterConnection.sync().exists(key);
            } else {
                return standaloneConnection.sync().exists(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync EXISTS失败: {}, {}", key, e.getMessage());
            return 0;
        }
    }

    /**
     * 异步 HSET
     */
    public CompletableFuture<Void> asyncHSet(String key, String field, String value) {
        try {
            if (isCluster) {
                return clusterConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSET失败: {}:{}, {}", key, field, ex.getMessage());
                            return null;
                        });
            } else {
                return standaloneConnection.async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSET失败: {}:{}, {}", key, field, ex.getMessage());
                            return null;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 异步 HSETNX (原子操作: 仅当 field 不存在时设置)
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
            return CompletableFuture.completedFuture(false);
        }
    }

    /**
     * 异步 HGETALL
     */
    public CompletableFuture<Map<String, String>> asyncHGetAll(String key) {
        try {
            if (isCluster) {
                return clusterConnection.async().hgetall(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGETALL失败: {}, {}", key, ex.getMessage());
                            return Collections.emptyMap();
                        });
            } else {
                return standaloneConnection.async().hgetall(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGETALL失败: {}, {}", key, ex.getMessage());
                            return Collections.emptyMap();
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
    }

    /**
     * 异步 HLEN (获取 Hash 的 field 数量)
     */
    public CompletableFuture<Long> asyncHLen(String key) {
        try {
            if (isCluster) {
                return clusterConnection.async().hlen(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HLEN失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            } else {
                return standaloneConnection.async().hlen(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HLEN失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(0L);
        }
    }

    /**
     * 异步 HEXISTS (检查 field 是否存在)
     */
    public CompletableFuture<Boolean> asyncHExists(String key, String field) {
        try {
            if (isCluster) {
                return clusterConnection.async().hexists(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HEXISTS失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            } else {
                return standaloneConnection.async().hexists(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HEXISTS失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    // ==================== Set 操作 ====================

    /**
     * 异步 SISMEMBER
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
     * 异步 SADD
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
     * 异步 SMEMBERS
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

    // ==================== 静态辅助方法 (兼容旧代码) ====================

    /**
     * 异步获取 Hash 的 field 数量 (静态方法，兼容旧代码)
     */
    public static CompletableFuture<Long> asyncHLen(KvrocksClient client, String key) {
        return client.asyncHLen(key);
    }

    /**
     * 异步检查 Hash field 是否存在 (静态方法，兼容旧代码)
     */
    public static CompletableFuture<Boolean> asyncHExists(KvrocksClient client, String key, String field) {
        return client.asyncHExists(key, field);
    }

    // ==================== 连接管理 ====================

    /**
     * 关闭连接
     */
    public void shutdown() {
        LOG.debug("KvrocksClient 实例关闭 (共享连接保持)");
    }

    /**
     * 关闭所有共享连接 (JVM 关闭时调用)
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