package com.zhugeio.etl.common.lock;

import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * 分布式锁管理器
 * 
 * 提供更便捷的锁操作，自动处理锁的获取和释放
 * 
 * 使用方式:
 * <pre>
 * LockManager lockManager = new LockManager(kvrocksClient);
 * 
 * // 异步方式
 * lockManager.executeWithLockAsync("event:123_test", () -> {
 *     // 业务逻辑
 *     return createEvent(appId, eventName);
 * }).thenAccept(result -> {
 *     // 处理结果
 * });
 * 
 * // 同步方式
 * Integer eventId = lockManager.executeWithLock("event:123_test", () -> {
 *     return createEvent(appId, eventName);
 * });
 * </pre>
 */
public class LockManager {

    private static final Logger LOG = LoggerFactory.getLogger(LockManager.class);

    private static final long DEFAULT_WAIT_MS = 5000L;
    private static final long DEFAULT_EXPIRE_MS = 30000L;

    private final KvrocksClient kvrocksClient;

    public LockManager(KvrocksClient kvrocksClient) {
        this.kvrocksClient = kvrocksClient;
    }

    /**
     * 使用分布式锁执行异步操作
     * 
     * @param lockKey  锁的key
     * @param supplier 需要执行的操作
     * @param <T>      返回值类型
     * @return 操作结果
     */
    public <T> CompletableFuture<T> executeWithLockAsync(String lockKey, 
                                                          Supplier<CompletableFuture<T>> supplier) {
        return executeWithLockAsync(lockKey, DEFAULT_WAIT_MS, DEFAULT_EXPIRE_MS, supplier);
    }

    /**
     * 使用分布式锁执行异步操作
     * 
     * @param lockKey  锁的key
     * @param waitMs   等待时间
     * @param expireMs 锁过期时间
     * @param supplier 需要执行的操作
     * @param <T>      返回值类型
     * @return 操作结果
     */
    public <T> CompletableFuture<T> executeWithLockAsync(String lockKey, 
                                                          long waitMs, 
                                                          long expireMs,
                                                          Supplier<CompletableFuture<T>> supplier) {
        DistributedLock lock = new DistributedLock(kvrocksClient, lockKey);

        return lock.tryLockAsync(waitMs, expireMs)
                .thenCompose(acquired -> {
                    if (!acquired) {
                        LOG.warn("获取锁超时: key={}", lockKey);
                        return CompletableFuture.completedFuture(null);
                    }

                    return supplier.get()
                            .whenComplete((result, ex) -> {
                                // 无论成功失败都释放锁
                                lock.unlockAsync()
                                        .exceptionally(unlockEx -> {
                                            LOG.error("释放锁失败: key={}", lockKey, unlockEx);
                                            return false;
                                        });
                            });
                });
    }

    /**
     * 使用分布式锁执行同步操作
     * 
     * @param lockKey  锁的key
     * @param supplier 需要执行的操作
     * @param <T>      返回值类型
     * @return 操作结果
     */
    public <T> T executeWithLock(String lockKey, Supplier<T> supplier) {
        return executeWithLock(lockKey, DEFAULT_WAIT_MS, DEFAULT_EXPIRE_MS, supplier);
    }

    /**
     * 使用分布式锁执行同步操作
     * 
     * @param lockKey  锁的key
     * @param waitMs   等待时间
     * @param expireMs 锁过期时间
     * @param supplier 需要执行的操作
     * @param <T>      返回值类型
     * @return 操作结果
     */
    public <T> T executeWithLock(String lockKey, 
                                  long waitMs, 
                                  long expireMs,
                                  Supplier<T> supplier) {
        DistributedLock lock = new DistributedLock(kvrocksClient, lockKey);

        boolean acquired = false;
        try {
            acquired = lock.tryLock(waitMs, expireMs);
            if (!acquired) {
                LOG.warn("获取锁超时: key={}", lockKey);
                return null;
            }

            return supplier.get();

        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    /**
     * 带重试的锁执行
     * 如果获取锁失败，会进行重试
     * 
     * @param lockKey   锁的key
     * @param maxRetry  最大重试次数
     * @param supplier  需要执行的操作
     * @param <T>       返回值类型
     * @return 操作结果
     */
    public <T> CompletableFuture<T> executeWithLockAndRetry(String lockKey,
                                                             int maxRetry,
                                                             Supplier<CompletableFuture<T>> supplier) {
        return executeWithLockAndRetryInternal(lockKey, maxRetry, 0, supplier);
    }

    private <T> CompletableFuture<T> executeWithLockAndRetryInternal(String lockKey,
                                                                      int maxRetry,
                                                                      int currentRetry,
                                                                      Supplier<CompletableFuture<T>> supplier) {
        return executeWithLockAsync(lockKey, supplier)
                .thenCompose(result -> {
                    if (result != null) {
                        return CompletableFuture.completedFuture(result);
                    }

                    // 结果为null且还有重试次数
                    if (currentRetry < maxRetry) {
                        LOG.info("锁执行失败，重试 {}/{}: key={}", currentRetry + 1, maxRetry, lockKey);
                        return executeWithLockAndRetryInternal(lockKey, maxRetry, currentRetry + 1, supplier);
                    }

                    return CompletableFuture.completedFuture(null);
                });
    }

    /**
     * 创建事件锁的key
     */
    public static String eventLockKey(Integer appId, String owner, String eventName) {
        return "event:" + appId + "_" + owner + "_" + eventName;
    }

    /**
     * 创建事件属性锁的key
     */
    public static String eventAttrLockKey(Integer eventId, String owner) {
        return "event_attr:" + eventId + "_" + owner;
    }

    /**
     * 创建AppId和平台的锁key
     */
    public String appSdkLockKey(Integer appId, Integer platform) {
        return "app_sdk:" + appId + "_" + platform;
    }
}
