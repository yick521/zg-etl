package com.zhugeio.etl.common.lock;

import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * KVRocks 分布式锁
 * 
 * 基于 Redis SETNX + EXPIRE 实现
 * 
 * 使用方式:
 * <pre>
 * DistributedLock lock = new DistributedLock(kvrocksClient, "lock:event:123_test");
 * lock.tryLockAsync(5000, 30000).thenCompose(acquired -> {
 *     if (acquired) {
 *         try {
 *             // 执行业务逻辑
 *             return doBusinessLogic();
 *         } finally {
 *             lock.unlockAsync();
 *         }
 *     }
 *     return CompletableFuture.completedFuture(null);
 * });
 * </pre>
 */
public class DistributedLock {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

    private static final String LOCK_PREFIX = "dlock:";
    private static final long DEFAULT_EXPIRE_MS = 30000L;  // 默认30秒过期
    private static final long DEFAULT_WAIT_MS = 5000L;     // 默认等待5秒
    private static final long RETRY_INTERVAL_MS = 50L;     // 重试间隔50ms

    private final KvrocksClient kvrocksClient;
    private final String lockKey;
    private final String lockValue;  // 唯一标识，用于安全释放锁

    public DistributedLock(KvrocksClient kvrocksClient, String lockKey) {
        this.kvrocksClient = kvrocksClient;
        this.lockKey = LOCK_PREFIX + lockKey;
        this.lockValue = UUID.randomUUID().toString();
    }

    /**
     * 异步尝试获取锁
     * 
     * @param waitMs   等待时间 (毫秒)
     * @param expireMs 锁过期时间 (毫秒)
     * @return 是否获取成功
     */
    public CompletableFuture<Boolean> tryLockAsync(long waitMs, long expireMs) {
        long startTime = System.currentTimeMillis();
        return tryAcquire(startTime, waitMs, expireMs);
    }

    /**
     * 异步尝试获取锁 (使用默认参数)
     */
    public CompletableFuture<Boolean> tryLockAsync() {
        return tryLockAsync(DEFAULT_WAIT_MS, DEFAULT_EXPIRE_MS);
    }

    /**
     * 递归尝试获取锁
     */
    private CompletableFuture<Boolean> tryAcquire(long startTime, long waitMs, long expireMs) {
        // 检查是否超时
        if (System.currentTimeMillis() - startTime > waitMs) {
            return CompletableFuture.completedFuture(false);
        }

        // 使用 SET NX PX 命令 (原子操作)
        return kvrocksClient.asyncSetNxPx(lockKey, lockValue, expireMs)
                .thenCompose(success -> {
                    if (success != null && success) {
                        LOG.debug("获取锁成功: key={}", lockKey);
                        return CompletableFuture.completedFuture(true);
                    }

                    // 未获取到锁，等待后重试
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(RETRY_INTERVAL_MS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    }).thenCompose(v -> tryAcquire(startTime, waitMs, expireMs));
                })
                .exceptionally(ex -> {
                    LOG.warn("获取锁异常: key={}", lockKey, ex);
                    return false;
                });
    }

    /**
     * 异步释放锁
     * 只有锁的持有者才能释放
     */
    public CompletableFuture<Boolean> unlockAsync() {
        // 使用 Lua 脚本保证原子性: 只有值匹配才删除
        String luaScript = 
                "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                "    return redis.call('del', KEYS[1]) " +
                "else " +
                "    return 0 " +
                "end";

        return kvrocksClient.asyncEval(luaScript, lockKey, lockValue)
                .thenApply(result -> {
                    boolean success = result != null && "1".equals(result.toString());
                    if (success) {
                        LOG.debug("释放锁成功: key={}", lockKey);
                    } else {
                        LOG.warn("释放锁失败(可能已过期): key={}", lockKey);
                    }
                    return success;
                })
                .exceptionally(ex -> {
                    LOG.error("释放锁异常: key={}", lockKey, ex);
                    return false;
                });
    }

    /**
     * 同步尝试获取锁
     */
    public boolean tryLock(long waitMs, long expireMs) {
        try {
            return tryLockAsync(waitMs, expireMs).get(waitMs + 1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("同步获取锁失败: key={}", lockKey, e);
            return false;
        }
    }

    /**
     * 同步释放锁
     */
    public boolean unlock() {
        try {
            return unlockAsync().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("同步释放锁失败: key={}", lockKey, e);
            return false;
        }
    }

    public String getLockKey() {
        return lockKey;
    }

    public String getLockValue() {
        return lockValue;
    }
}
