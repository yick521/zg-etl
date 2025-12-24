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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KVRocks客户端 - Lettuce异步连接池版本
 *
 * 优势:
 * 1. 真正的异步IO,基于Netty
 * 2. 连接池: 4个连接轮询使用，提升并发能力
 * 3. 支持百万级并发
 * 4. 支持 SETNX/HSETNX 原子操作
 * 5. 支持 SET NX PX / EVAL 分布式锁
 * 6. 集群/单机模式共享连接池
 *
 */
public class KvrocksClient implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(KvrocksClient.class);
    private static final long serialVersionUID = 2L;

    // 连接池大小
    private static final int POOL_SIZE = 4;

    private final String host;
    private final int port;
    private final boolean isCluster;

    // Lettuce客户端 (transient, 在每个TaskManager上重新初始化)
    private transient RedisClusterClient clusterClient;
    private transient RedisClient standaloneClient;
    private transient StatefulRedisClusterConnection<String, String>[] clusterConnections;
    private transient StatefulRedisConnection<String, String>[] standaloneConnections;

    public KvrocksClient(String host, int port, boolean isCluster) {
        this.host = host;
        this.port = port;
        this.isCluster = isCluster;
    }

    // ==================== 静态共享连接池 ====================

    // 集群模式
    private static volatile RedisClusterClient sharedClusterClient;
    private static volatile StatefulRedisClusterConnection<String, String>[] sharedClusterPool;
    private static final Object CLUSTER_LOCK = new Object();
    private static final AtomicBoolean clusterInitialized = new AtomicBoolean(false);

    // 单机模式
    private static volatile RedisClient sharedStandaloneClient;
    private static volatile StatefulRedisConnection<String, String>[] sharedStandalonePool;
    private static final Object STANDALONE_LOCK = new Object();
    private static final AtomicBoolean standaloneInitialized = new AtomicBoolean(false);

    // 全局轮询计数器
    private static final AtomicInteger roundRobin = new AtomicInteger(0);

    /**
     * 初始化Lettuce连接池
     */
    public void init() {
        if (isCluster) {
            initClusterPool();
        } else {
            initStandalonePool();
        }
    }

    /**
     * 集群模式连接池初始化
     */
    @SuppressWarnings("unchecked")
    private void initClusterPool() {
        if (clusterInitialized.get() && isPoolHealthy(sharedClusterPool)) {
            this.clusterConnections = sharedClusterPool;
            this.clusterClient = sharedClusterClient;
            LOG.debug("复用已有集群连接池: {}:{}, 连接数: {}", host, port, POOL_SIZE);
            return;
        }

        synchronized (CLUSTER_LOCK) {
            if (clusterInitialized.get() && isPoolHealthy(sharedClusterPool)) {
                this.clusterConnections = sharedClusterPool;
                this.clusterClient = sharedClusterClient;
                return;
            }

            LOG.info("创建集群连接池: {}:{}, 连接数: {}", host, port, POOL_SIZE);

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

            sharedClusterPool = new StatefulRedisClusterConnection[POOL_SIZE];
            for (int i = 0; i < POOL_SIZE; i++) {
                sharedClusterPool[i] = sharedClusterClient.connect();
            }

            this.clusterConnections = sharedClusterPool;
            this.clusterClient = sharedClusterClient;

            clusterInitialized.set(true);
            LOG.info("Lettuce集群连接池初始化成功: {}:{} (共享复用模式, 连接数: {})", host, port, POOL_SIZE);
        }
    }

    /**
     * 单机模式连接池初始化
     */
    @SuppressWarnings("unchecked")
    private void initStandalonePool() {
        if (standaloneInitialized.get() && isPoolHealthy(sharedStandalonePool)) {
            this.standaloneConnections = sharedStandalonePool;
            this.standaloneClient = sharedStandaloneClient;
            LOG.debug("复用已有单机连接池: {}:{}, 连接数: {}", host, port, POOL_SIZE);
            return;
        }

        synchronized (STANDALONE_LOCK) {
            if (standaloneInitialized.get() && isPoolHealthy(sharedStandalonePool)) {
                this.standaloneConnections = sharedStandalonePool;
                this.standaloneClient = sharedStandaloneClient;
                return;
            }

            LOG.info("创建单机连接池: {}:{}, 连接数: {}", host, port, POOL_SIZE);

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

            sharedStandalonePool = new StatefulRedisConnection[POOL_SIZE];
            for (int i = 0; i < POOL_SIZE; i++) {
                sharedStandalonePool[i] = sharedStandaloneClient.connect();
            }

            this.standaloneConnections = sharedStandalonePool;
            this.standaloneClient = sharedStandaloneClient;

            standaloneInitialized.set(true);
            LOG.info("Lettuce单机连接池初始化成功: {}:{} (共享复用模式, 连接数: {})", host, port, POOL_SIZE);
        }
    }

    private boolean isPoolHealthy(Object[] pool) {
        if (pool == null || pool.length == 0) {
            return false;
        }
        for (Object conn : pool) {
            if (conn == null) {
                return false;
            }
            if (conn instanceof StatefulRedisClusterConnection) {
                if (!((StatefulRedisClusterConnection<?, ?>) conn).isOpen()) {
                    return false;
                }
            } else if (conn instanceof StatefulRedisConnection) {
                if (!((StatefulRedisConnection<?, ?>) conn).isOpen()) {
                    return false;
                }
            }
        }
        return true;
    }

    // ==================== 连接获取 (轮询) ====================

    private int getNextIndex() {
        return Math.abs(roundRobin.getAndIncrement() % POOL_SIZE);
    }

    private StatefulRedisClusterConnection<String, String> getClusterConnection() {
        return clusterConnections[getNextIndex()];
    }

    private StatefulRedisConnection<String, String> getStandaloneConnection() {
        return standaloneConnections[getNextIndex()];
    }

    /**
     * 测试连接
     */
    public boolean testConnection() {
        try {
            String result;
            if (isCluster) {
                result = getClusterConnection().sync().ping();
            } else {
                result = getStandaloneConnection().sync().ping();
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
                return getClusterConnection().async().get(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks GET失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                return getStandaloneConnection().async().get(key)
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
                return getClusterConnection().async().set(key, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                return getStandaloneConnection().async().set(key, value)
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

    // ==================== 分布式锁相关操作 ====================

    /**
     * 异步 SET NX PX (分布式锁核心操作)
     */
    public CompletableFuture<Boolean> asyncSetNxPx(String key, String value, long expireMs) {
        try {
            SetArgs args = SetArgs.Builder.nx().px(expireMs);
            if (isCluster) {
                return getClusterConnection().async().set(key, value, args)
                        .toCompletableFuture()
                        .thenApply(result -> "OK".equals(result))
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SET NX PX失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            } else {
                return getStandaloneConnection().async().set(key, value, args)
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
     */
    public CompletableFuture<Object> asyncEval(String script, String key, String value) {
        try {
            String[] keys = new String[]{key};
            String[] args = new String[]{value};
            if (isCluster) {
                return getClusterConnection().async().eval(script, ScriptOutputType.INTEGER, keys, args)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks EVAL失败: {}, {}", key, ex.getMessage());
                            return null;
                        });
            } else {
                return getStandaloneConnection().async().eval(script, ScriptOutputType.INTEGER, keys, args)
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
     * 异步 DEL
     */
    public CompletableFuture<Long> asyncDel(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().async().del(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks DEL失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            } else {
                return getStandaloneConnection().async().del(key)
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
     * 异步 EXPIRE
     */
    public CompletableFuture<Boolean> asyncExpire(String key, long seconds) {
        try {
            if (isCluster) {
                return getClusterConnection().async().expire(key, seconds)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks EXPIRE失败: {}, {}", key, ex.getMessage());
                            return false;
                        });
            } else {
                return getStandaloneConnection().async().expire(key, seconds)
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
                return getClusterConnection().async().hget(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGET失败: {}:{}, {}", key, field, ex.getMessage());
                            return null;
                        });
            } else {
                return getStandaloneConnection().async().hget(key, field)
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
     */
    public String hGet(String key, String field) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().hget(key, field);
            } else {
                return getStandaloneConnection().sync().hget(key, field);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync HGET失败: {}:{}, {}", key, field, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 HSET
     */
    public boolean hSet(String key, String field, String value) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().hset(key, field, value);
            } else {
                return getStandaloneConnection().sync().hset(key, field, value);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync HSET失败: {}:{}, {}", key, field, e.getMessage());
            return false;
        }
    }

    /**
     * 同步 HGETALL
     */
    public Map<String, String> hGetAll(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().hgetall(key);
            } else {
                return getStandaloneConnection().sync().hgetall(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync HGETALL失败: {}, {}", key, e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * 同步 GET
     */
    public String get(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().get(key);
            } else {
                return getStandaloneConnection().sync().get(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync GET失败: {}, {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 SET
     */
    public String set(String key, String value) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().set(key, value);
            } else {
                return getStandaloneConnection().sync().set(key, value);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync SET失败: {}, {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 DEL
     */
    public long del(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().del(key);
            } else {
                return getStandaloneConnection().sync().del(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync DEL失败: {}, {}", key, e.getMessage());
            return 0;
        }
    }

    /**
     * 同步 EXISTS
     */
    public long exists(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().exists(key);
            } else {
                return getStandaloneConnection().sync().exists(key);
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
                return getClusterConnection().async().hset(key, field, value)
                        .toCompletableFuture()
                        .thenApply(result -> (Void) null)
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSET失败: {}:{}, {}", key, field, ex.getMessage());
                            return null;
                        });
            } else {
                return getStandaloneConnection().async().hset(key, field, value)
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
                return getClusterConnection().async().hsetnx(key, field, value)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HSETNX失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            } else {
                return getStandaloneConnection().async().hsetnx(key, field, value)
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
                return getClusterConnection().async().hgetall(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HGETALL失败: {}, {}", key, ex.getMessage());
                            return Collections.emptyMap();
                        });
            } else {
                return getStandaloneConnection().async().hgetall(key)
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
     * 异步 HLEN
     */
    public CompletableFuture<Long> asyncHLen(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().async().hlen(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HLEN失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            } else {
                return getStandaloneConnection().async().hlen(key)
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
     * 同步 INCRBY
     */
    public Long incrBy(String key, long amount) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().incrby(key, amount);
            } else {
                return getStandaloneConnection().sync().incrby(key, amount);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync INCRBY失败: {}, {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 同步 INCR
     */
    public Long incr(String key) {
        try {
            if (isCluster) {
                return getClusterConnection().sync().incr(key);
            } else {
                return getStandaloneConnection().sync().incr(key);
            }
        } catch (Exception e) {
            LOG.error("KVRocks sync INCR失败: {}, {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * 异步 HEXISTS
     */
    public CompletableFuture<Boolean> asyncHExists(String key, String field) {
        try {
            if (isCluster) {
                return getClusterConnection().async().hexists(key, field)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks HEXISTS失败: {}:{}, {}", key, field, ex.getMessage());
                            return false;
                        });
            } else {
                return getStandaloneConnection().async().hexists(key, field)
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
                return getClusterConnection().async().sismember(key, member)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SISMEMBER失败: {}:{}, {}", key, member, ex.getMessage());
                            return false;
                        });
            } else {
                return getStandaloneConnection().async().sismember(key, member)
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
                return getClusterConnection().async().sadd(key, members)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SADD失败: {}, {}", key, ex.getMessage());
                            return 0L;
                        });
            } else {
                return getStandaloneConnection().async().sadd(key, members)
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
                return getClusterConnection().async().smembers(key)
                        .toCompletableFuture()
                        .exceptionally(ex -> {
                            LOG.error("KVRocks SMEMBERS失败: {}, {}", key, ex.getMessage());
                            return Collections.emptySet();
                        });
            } else {
                return getStandaloneConnection().async().smembers(key)
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

    public static CompletableFuture<Long> asyncHLen(KvrocksClient client, String key) {
        return client.asyncHLen(key);
    }

    public static CompletableFuture<Boolean> asyncHExists(KvrocksClient client, String key, String field) {
        return client.asyncHExists(key, field);
    }

    // ==================== 连接管理 ====================

    /**
     * 关闭连接 (实例不关闭共享连接池)
     */
    public void shutdown() {
        LOG.debug("KvrocksClient 实例关闭 (连接池保持)");
    }

    /**
     * 关闭所有共享连接池 (JVM 关闭时调用)
     */
    public static void shutdownAll() {
        LOG.info("关闭所有 KVRocks 连接池...");

        synchronized (CLUSTER_LOCK) {
            if (sharedClusterPool != null) {
                for (StatefulRedisClusterConnection<String, String> conn : sharedClusterPool) {
                    if (conn != null) {
                        try { conn.close(); } catch (Exception e) { LOG.warn("关闭集群连接失败", e); }
                    }
                }
                sharedClusterPool = null;
            }
            if (sharedClusterClient != null) {
                try { sharedClusterClient.shutdown(); } catch (Exception e) { LOG.warn("关闭集群客户端失败", e); }
                sharedClusterClient = null;
            }
            clusterInitialized.set(false);
        }

        synchronized (STANDALONE_LOCK) {
            if (sharedStandalonePool != null) {
                for (StatefulRedisConnection<String, String> conn : sharedStandalonePool) {
                    if (conn != null) {
                        try { conn.close(); } catch (Exception e) { LOG.warn("关闭单机连接失败", e); }
                    }
                }
                sharedStandalonePool = null;
            }
            if (sharedStandaloneClient != null) {
                try { sharedStandaloneClient.shutdown(); } catch (Exception e) { LOG.warn("关闭单机客户端失败", e); }
                sharedStandaloneClient = null;
            }
            standaloneInitialized.set(false);
        }

        LOG.info("所有 KVRocks 连接池已关闭");
    }

    /**
     * 获取连接池大小
     */
    public static int getPoolSize() {
        return POOL_SIZE;
    }

    // ShutdownHook
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("JVM 关闭，清理 KVRocks 连接池...");
            shutdownAll();
        }, "kvrocks-pool-shutdown-hook"));
    }
}