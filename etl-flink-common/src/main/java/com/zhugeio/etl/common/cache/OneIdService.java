package com.zhugeio.etl.common.cache;

import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.zhugeio.etl.common.constants.CacheKeyConstants.*;

/**
 * OneId服务 - 统一ID管理
 * 
 * 处理设备ID、用户ID、诸葛ID的生成和映射
 * 完整实现原 ZgIdService.scala 的逻辑，使用 String 结构存储
 * 
 * Key结构:
 * - d:{appId}:{deviceMd5}   -> zgDeviceId 设备MD5→设备ID
 * - u:{appId}:{cuid}        -> zgUserId   用户标识→用户ID
 * - dz:{appId}:{zgDeviceId} -> zgId       设备→诸葛ID
 * - uz:{appId}:{zgUserId}   -> zgId       用户→诸葛ID
 * - zu:{appId}:{zgId}       -> zgUserId   诸葛ID→用户 (反向映射)
 * 
 * ⚠️ 重要: 这些ID映射不能使用本地缓存！
 * 
 * 原因: Flink 并行任务可能同时处理同一设备/用户的数据，
 *       如果使用本地缓存，会导致同一设备/用户生成多个不同的ID
 * 
 * 解决方案: 使用 "先读后写 + SETNX" 模式
 *   1. GET 查询是否存在
 *   2. 存在 → 直接返回
 *   3. 不存在 → 生成新ID，SETNX 原子写入
 *   4. SETNX 成功 → 返回新ID
 *   5. SETNX 失败(被其他任务抢先) → GET 返回已有ID
 */
public class OneIdService {

    private static final Logger LOG = LoggerFactory.getLogger(OneIdService.class);

    private KvrocksClient kvrocksClient;
    private SnowflakeIdGenerator idGenerator;

    // 统计计数器
    private final AtomicLong existZgidCount = new AtomicLong(0);
    private final AtomicLong newZgidCount = new AtomicLong(0);
    private final AtomicLong conflictCount = new AtomicLong(0);

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final int workerId;

    public OneIdService(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, int workerId) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.workerId = workerId;
    }

    public void init() {
        kvrocksClient = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocksClient.init();

        if (!kvrocksClient.testConnection()) {
            throw new RuntimeException("KVRocks连接失败: " + kvrocksHost + ":" + kvrocksPort);
        }

        idGenerator = new SnowflakeIdGenerator(workerId);

        LOG.info("OneIdService initialized: host={}, port={}, workerId={}",
                kvrocksHost, kvrocksPort, workerId);
    }
    // ==================== 设备ID ====================

    /**
     * 获取或创建设备ID (先读后写模式)
     * 
     * Key: d:{appId}:{deviceMd5} (String)
     * Value: {zgDeviceId}
     * 
     * 流程:
     * 1. GET 查询是否存在
     * 2. 存在 → 直接返回
     * 3. 不存在 → 生成新ID，SETNX 写入
     * 4. SETNX 成功 → 返回新ID
     * 5. SETNX 失败(被其他任务抢先) → GET 返回已有ID
     */
    public CompletableFuture<Long> getOrCreateDeviceId(Integer appId, String deviceMd5) {
        if (appId == null || deviceMd5 == null || deviceMd5.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String key = DEVICE_ID_PREFIX + appId + ":" + deviceMd5;

        return kvrocksClient.asyncGet(key)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        // 已存在，直接返回
                        return CompletableFuture.completedFuture(Long.parseLong(existingId));
                    }
                    
                    // 不存在，生成新ID并尝试写入
                    Long newId = idGenerator.nextId();
                    String newIdStr = String.valueOf(newId);
                    
                    return kvrocksClient.asyncSetIfAbsent(key, newIdStr)
                            .thenCompose(success -> {
                                if (success) {
                                    // 写入成功
                                    return CompletableFuture.completedFuture(newId);
                                } else {
                                    // 写入失败，被其他任务抢先，读取已有ID
                                    return kvrocksClient.asyncGet(key)
                                            .thenApply(id -> id != null ? Long.parseLong(id) : null);
                                }
                            });
                });
    }

    // ==================== 用户ID ====================

    /**
     * 获取或创建用户ID (先读后写模式)
     * 
     * Key: u:{appId}:{cuid} (String)
     * Value: {zgUserId}
     * 
     * 流程:
     * 1. GET 查询是否存在
     * 2. 存在 → 直接返回
     * 3. 不存在 → 生成新ID，SETNX 写入
     * 4. SETNX 成功 → 返回新ID
     * 5. SETNX 失败(被其他任务抢先) → GET 返回已有ID
     */
    public CompletableFuture<Long> getOrCreateUserId(Integer appId, String cuid) {
        if (appId == null || cuid == null || cuid.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String key = USER_ID_PREFIX + appId + ":" + cuid;

        return kvrocksClient.asyncGet(key)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        // 已存在，直接返回
                        return CompletableFuture.completedFuture(Long.parseLong(existingId));
                    }
                    
                    // 不存在，生成新ID并尝试写入
                    Long newId = idGenerator.nextId();
                    String newIdStr = String.valueOf(newId);
                    
                    return kvrocksClient.asyncSetIfAbsent(key, newIdStr)
                            .thenCompose(success -> {
                                if (success) {
                                    // 写入成功
                                    return CompletableFuture.completedFuture(newId);
                                } else {
                                    // 写入失败，被其他任务抢先，读取已有ID
                                    return kvrocksClient.asyncGet(key)
                                            .thenApply(id -> id != null ? Long.parseLong(id) : null);
                                }
                            });
                });
    }

    // ==================== 诸葛ID (核心逻辑) ====================

    /**
     * 获取或创建诸葛ID - 完整实现原 ZgIdService.scala 逻辑
     * 
     * Key结构:
     * - uz:{appId}:{zgUserId}   -> zgId     用户→诸葛ID
     * - dz:{appId}:{zgDeviceId} -> zgId     设备→诸葛ID
     * - zu:{appId}:{zgId}       -> zgUserId 诸葛ID→用户 (反向映射)
     * 
     * 绑定逻辑 (与原 ZgIdService.scala 完全一致):
     * 
     * Case 1: 实名用户 (zgUserId != null)
     *   1.1 用户已有zgid (uz有值):
     *       - 使用用户的zgid
     *       - 如果设备zgid不同，更新设备绑定到用户的zgid
     *   1.2 用户没有zgid (uz无值):
     *       1.2.1 设备有zgid且该zgid未绑定其他用户 (dz有值 && zu无值):
     *             - 使用设备的zgid，绑定用户
     *             - 写入 uz 和 zu
     *       1.2.2 设备有zgid但该zgid已绑定其他用户 (dz有值 && zu有值):
     *             - 创建新zgid
     *             - 写入 dz, uz, zu
     *       1.2.3 设备没有zgid (dz无值):
     *             - 创建新zgid
     *             - 写入 dz, uz, zu
     * 
     * Case 2: 匿名用户 (zgUserId == null)
     *   2.1 设备有zgid: 使用设备的zgid
     *   2.2 设备没有zgid: 创建新zgid，仅写入 dz
     * 
     * @param appId 应用ID
     * @param zgUserId 诸葛用户ID (可为null表示匿名用户)
     * @param zgDeviceId 诸葛设备ID (必须)
     * @return ZgidResult
     */
    public CompletableFuture<ZgidResult> getOrCreateZgid(Integer appId, Long zgUserId, Long zgDeviceId) {
        if (appId == null || zgDeviceId == null) {
            return CompletableFuture.completedFuture(ZgidResult.invalid());
        }

        String appIdStr = String.valueOf(appId);
        
        // uz:{appId}:{zgUserId} -> zgId
        String userZgidKey = zgUserId != null 
                ? USER_ZGID_PREFIX + appIdStr + ":" + zgUserId 
                : null;
        // dz:{appId}:{zgDeviceId} -> zgId
        String deviceZgidKey = ZGID_DEVICE_PREFIX + appIdStr + ":" + zgDeviceId;

        // 并行查询 uz 和 dz
        CompletableFuture<String> userZgidFuture = userZgidKey != null
                ? kvrocksClient.asyncGet(userZgidKey)
                : CompletableFuture.completedFuture(null);
        CompletableFuture<String> deviceZgidFuture = kvrocksClient.asyncGet(deviceZgidKey);

        return CompletableFuture.allOf(userZgidFuture, deviceZgidFuture)
                .thenCompose(v -> {
                    String userZgidStr = userZgidFuture.join();
                    String deviceZgidStr = deviceZgidFuture.join();

                    // Case 1: 实名用户
                    if (zgUserId != null) {
                        return handleIdentifiedUser(appIdStr, zgUserId, zgDeviceId,
                                userZgidKey, deviceZgidKey, userZgidStr, deviceZgidStr);
                    } else {
                        // Case 2: 匿名用户
                        return handleAnonymousUser(deviceZgidKey, deviceZgidStr);
                    }
                })
                .exceptionally(ex -> {
                    LOG.error("获取诸葛ID失败: appId={}, userId={}, deviceId={}", 
                            appId, zgUserId, zgDeviceId, ex);
                    return ZgidResult.error(ex.getMessage());
                });
    }

    /**
     * 处理实名用户的zgid获取/创建
     */
    private CompletableFuture<ZgidResult> handleIdentifiedUser(
            String appIdStr, Long zgUserId, Long zgDeviceId,
            String userZgidKey, String deviceZgidKey,
            String userZgidStr, String deviceZgidStr) {
        
        if (userZgidStr != null) {
            // Case 1.1: 用户已有zgid
            Long zgid = Long.parseLong(userZgidStr);
            
            // 如果设备zgid不同，更新设备绑定到用户的zgid
            if (deviceZgidStr == null || !deviceZgidStr.equals(userZgidStr)) {
                kvrocksClient.asyncSet(deviceZgidKey, userZgidStr);
            }
            
            existZgidCount.incrementAndGet();
            return CompletableFuture.completedFuture(
                    ZgidResult.existing(zgid, false, true));
        } else {
            // Case 1.2: 用户没有zgid
            if (deviceZgidStr != null) {
                // 设备有zgid，检查该zgid是否已绑定其他用户
                String reverseKey = ZGID_USER_PREFIX + appIdStr + ":" + deviceZgidStr;
                
                return kvrocksClient.asyncGet(reverseKey)
                        .thenCompose(existingUserId -> {
                            if (existingUserId == null) {
                                // Case 1.2.1: zgid未绑定其他用户，绑定当前用户
                                Long zgid = Long.parseLong(deviceZgidStr);
                                
                                // 写入 uz 和 zu
                                kvrocksClient.asyncSet(userZgidKey, deviceZgidStr);
                                kvrocksClient.asyncSet(reverseKey, String.valueOf(zgUserId));
                                
                                existZgidCount.incrementAndGet();
                                return CompletableFuture.completedFuture(
                                        ZgidResult.existing(zgid, true, false));
                            } else {
                                // Case 1.2.2: zgid已绑定其他用户，创建新zgid
                                return createNewZgidForIdentifiedUser(
                                        appIdStr, zgUserId, userZgidKey, deviceZgidKey);
                            }
                        });
            } else {
                // Case 1.2.3: 设备没有zgid，创建新zgid
                return createNewZgidForIdentifiedUser(
                        appIdStr, zgUserId, userZgidKey, deviceZgidKey);
            }
        }
    }

    /**
     * 为实名用户创建新zgid
     */
    private CompletableFuture<ZgidResult> createNewZgidForIdentifiedUser(
            String appIdStr, Long zgUserId, String userZgidKey, String deviceZgidKey) {
        
        Long newZgid = idGenerator.nextId();
        String zgidStr = String.valueOf(newZgid);
        String reverseKey = ZGID_USER_PREFIX + appIdStr + ":" + zgidStr;

        // 使用 SETNX 确保原子性
        return kvrocksClient.asyncSetIfAbsent(deviceZgidKey, zgidStr)
                .thenCompose(deviceSuccess -> {
                    if (deviceSuccess) {
                        // 设备绑定成功，写入 uz 和 zu
                        kvrocksClient.asyncSet(userZgidKey, zgidStr);
                        kvrocksClient.asyncSet(reverseKey, String.valueOf(zgUserId));
                        
                        newZgidCount.incrementAndGet();
                        return CompletableFuture.completedFuture(ZgidResult.created(newZgid));
                    } else {
                        // 设备绑定失败，被其他任务抢先，重新获取
                        conflictCount.incrementAndGet();
                        return kvrocksClient.asyncGet(deviceZgidKey)
                                .thenApply(existingZgid -> {
                                    if (existingZgid != null) {
                                        Long zgid = Long.parseLong(existingZgid);
                                        // 仍然绑定用户
                                        kvrocksClient.asyncSet(userZgidKey, existingZgid);
                                        String rKey = ZGID_USER_PREFIX + appIdStr + ":" + existingZgid;
                                        kvrocksClient.asyncSet(rKey, String.valueOf(zgUserId));
                                        
                                        existZgidCount.incrementAndGet();
                                        return ZgidResult.existing(zgid, true, false);
                                    } else {
                                        return ZgidResult.invalid();
                                    }
                                });
                    }
                });
    }

    /**
     * 处理匿名用户的zgid获取/创建
     */
    private CompletableFuture<ZgidResult> handleAnonymousUser(
            String deviceZgidKey, String deviceZgidStr) {
        
        if (deviceZgidStr != null) {
            // Case 2.1: 设备有zgid
            existZgidCount.incrementAndGet();
            return CompletableFuture.completedFuture(
                    ZgidResult.existing(Long.parseLong(deviceZgidStr), false, false));
        } else {
            // Case 2.2: 设备没有zgid，创建新zgid
            Long newZgid = idGenerator.nextId();
            String zgidStr = String.valueOf(newZgid);

            return kvrocksClient.asyncSetIfAbsent(deviceZgidKey, zgidStr)
                    .thenCompose(success -> {
                        if (success) {
                            newZgidCount.incrementAndGet();
                            return CompletableFuture.completedFuture(ZgidResult.created(newZgid));
                        } else {
                            conflictCount.incrementAndGet();
                            return kvrocksClient.asyncGet(deviceZgidKey)
                                    .thenApply(existingZgid -> {
                                        if (existingZgid != null) {
                                            existZgidCount.incrementAndGet();
                                            return ZgidResult.existing(
                                                    Long.parseLong(existingZgid), false, false);
                                        } else {
                                            return ZgidResult.invalid();
                                        }
                                    });
                        }
                    });
        }
    }

    // ==================== 反向查询 ====================

    /**
     * 根据zgId查询绑定的userId
     * 
     * Key: zu:{appId}:{zgId} -> zgUserId
     */
    public CompletableFuture<Long> getUserIdByZgid(Integer appId, Long zgId) {
        if (appId == null || zgId == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String key = ZGID_USER_PREFIX + appId + ":" + zgId;
        return kvrocksClient.asyncGet(key)
                .thenApply(value -> value != null ? Long.parseLong(value) : null);
    }

    /**
     * 判断zgId是否已绑定用户
     */
    public CompletableFuture<Boolean> isZgidBoundToUser(Integer appId, Long zgId) {
        if (appId == null || zgId == null) {
            return CompletableFuture.completedFuture(false);
        }
        
        String key = ZGID_USER_PREFIX + appId + ":" + zgId;
        return kvrocksClient.asyncGet(key)
                .thenApply(value -> value != null);
    }

    /**
     * 绑定zgId到userId (反向映射)
     * 
     * Key: zu:{appId}:{zgId} -> zgUserId
     */
    public CompletableFuture<Void> bindZgidToUser(Integer appId, Long zgId, Long zgUserId) {
        if (appId == null || zgId == null || zgUserId == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String key = ZGID_USER_PREFIX + appId + ":" + zgId;
        return kvrocksClient.asyncSet(key, String.valueOf(zgUserId))
                .thenApply(v -> null);
    }

    public void close() {
        if (kvrocksClient != null) {
            try {
                kvrocksClient.shutdown();
            } catch (Exception e) {
                LOG.warn("关闭KvrocksClient时出错", e);
            }
        }
        LOG.info("OneIdService closed. Final stats: {}", getStats());
    }

    // ==================== 统计信息 ====================

    public long getExistZgidCount() {
        return existZgidCount.get();
    }

    public long getNewZgidCount() {
        return newZgidCount.get();
    }

    public long getConflictCount() {
        return conflictCount.get();
    }

    public void resetCounters() {
        existZgidCount.set(0);
        newZgidCount.set(0);
        conflictCount.set(0);
    }

    public String getStats() {
        return String.format("OneIdService Stats - exist: %d, new: %d, conflict: %d",
                existZgidCount.get(), newZgidCount.get(), conflictCount.get());
    }

    // ==================== 结果类 ====================

    /**
     * Zgid操作结果
     */
    public static class ZgidResult {
        private final Long zgid;
        private final boolean created;
        private final boolean userBound;      // 本次是否绑定了用户
        private final boolean existedByUser;  // 是否通过用户维度找到的
        private final boolean valid;
        private final String errorMsg;

        private ZgidResult(Long zgid, boolean created, boolean userBound, 
                          boolean existedByUser, boolean valid, String errorMsg) {
            this.zgid = zgid;
            this.created = created;
            this.userBound = userBound;
            this.existedByUser = existedByUser;
            this.valid = valid;
            this.errorMsg = errorMsg;
        }

        public static ZgidResult created(Long zgid) {
            return new ZgidResult(zgid, true, false, false, true, null);
        }

        public static ZgidResult existing(Long zgid, boolean userBound, boolean existedByUser) {
            return new ZgidResult(zgid, false, userBound, existedByUser, true, null);
        }

        public static ZgidResult invalid() {
            return new ZgidResult(null, false, false, false, false, "Invalid parameters");
        }

        public static ZgidResult error(String msg) {
            return new ZgidResult(null, false, false, false, false, msg);
        }

        public Long getZgid() { return zgid; }
        public boolean isCreated() { return created; }
        public boolean isUserBound() { return userBound; }
        public boolean isExistedByUser() { return existedByUser; }
        public boolean isValid() { return valid; }
        public String getErrorMsg() { return errorMsg; }

        @Override
        public String toString() {
            return String.format("ZgidResult{zgid=%d, created=%s, userBound=%s, existedByUser=%s, valid=%s}",
                    zgid, created, userBound, existedByUser, valid);
        }
    }

    // ==================== 雪花ID生成器 ====================

    /**
     * 简化版雪花算法ID生成器
     */
    public static class SnowflakeIdGenerator {
        private final long workerId;
        private final long epoch = 1609459200000L; // 2021-01-01
        private final long workerIdBits = 10L;
        private final long sequenceBits = 12L;
        private final long maxWorkerId = ~(-1L << workerIdBits);
        private final long sequenceMask = ~(-1L << sequenceBits);
        private final long workerIdShift = sequenceBits;
        private final long timestampShift = sequenceBits + workerIdBits;

        private long sequence = 0L;
        private long lastTimestamp = -1L;

        public SnowflakeIdGenerator(long workerId) {
            if (workerId > maxWorkerId || workerId < 0) {
                throw new IllegalArgumentException("Worker ID must be between 0 and " + maxWorkerId);
            }
            this.workerId = workerId;
        }

        public synchronized long nextId() {
            long timestamp = System.currentTimeMillis();

            if (timestamp < lastTimestamp) {
                throw new RuntimeException("Clock moved backwards");
            }

            if (timestamp == lastTimestamp) {
                sequence = (sequence + 1) & sequenceMask;
                if (sequence == 0) {
                    timestamp = tilNextMillis(lastTimestamp);
                }
            } else {
                sequence = 0L;
            }

            lastTimestamp = timestamp;

            return ((timestamp - epoch) << timestampShift) |
                    (workerId << workerIdShift) |
                    sequence;
        }

        private long tilNextMillis(long lastTimestamp) {
            long timestamp = System.currentTimeMillis();
            while (timestamp <= lastTimestamp) {
                timestamp = System.currentTimeMillis();
            }
            return timestamp;
        }
    }
}
