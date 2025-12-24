package com.zhugeio.etl.common.cache;

import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.util.SnowflakeIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OneId服务 - 统一ID管理
 *
 * ✅ 使用外部 SnowflakeIdGenerator (支持时钟回拨等待)
 * ✅ 使用 Hash 结构存储，与原 Scala FrontCache 完全一致
 *
 * Hash结构 (与原Scala一致):
 * - device_id:{appId}  field={deviceMd5} value={zgDeviceId}   设备MD5→设备ID
 * - user_id:{appId}    field={cuid}      value={zgUserId}     用户标识→用户ID
 * - device_zgid:{appId} field={zgDeviceId} value={zgId}       设备→诸葛ID
 * - user_zgid:{appId}   field={zgUserId}   value={zgId}       用户→诸葛ID
 * - zgid_user:{appId}   field={zgId}       value={zgUserId}   诸葛ID→用户(反向)
 *
 * ⚠️ 重要: 这些ID映射不使用本地缓存！
 *    使用 "先读后写 + HSETNX" 模式保证分布式唯一性
 */
public class OneIdService {

    private static final Logger LOG = LoggerFactory.getLogger(OneIdService.class);
    
    // Hash Key 前缀 (与原Scala一致)
    private static final String DEVICE_ID_HASH = "device_id:";      // device_id:{appId}
    private static final String USER_ID_HASH = "user_id:";          // user_id:{appId}
    private static final String DEVICE_ZGID_HASH = "device_zgid:";  // device_zgid:{appId}
    private static final String USER_ZGID_HASH = "user_zgid:";      // user_zgid:{appId}
    private static final String ZGID_USER_HASH = "zgid_user:";      // zgid_user:{appId}

    private KvrocksClient kvrocksClient;
    private SnowflakeIdGenerator idGenerator;

    private final AtomicLong existIdCount = new AtomicLong(0);
    private final AtomicLong newIdCount = new AtomicLong(0);
    private final AtomicLong conflictCount = new AtomicLong(0);
    private final AtomicLong bindingCount = new AtomicLong(0);

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final Integer externalWorkerId;

    /**
     * 构造函数 (自动生成 workerId)
     */
    public OneIdService(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, null);
    }

    /**
     * 构造函数 (指定 workerId)
     */
    public OneIdService(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, Integer workerId) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.externalWorkerId = workerId;
    }

    public void init() {
        kvrocksClient = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocksClient.init();

        if (!kvrocksClient.testConnection()) {
            throw new RuntimeException("KVRocks连接失败: " + kvrocksHost + ":" + kvrocksPort);
        }

        int workerId;
        if (externalWorkerId != null) {
            workerId = externalWorkerId;
            LOG.info("使用外部指定的 workerId: {}", workerId);
        } else {
            workerId = generateWorkerId();
            LOG.info("自动生成的 workerId: {}", workerId);
        }

        // 使用外部的 SnowflakeIdGenerator
        idGenerator = new SnowflakeIdGenerator(workerId);

        LOG.info("OneIdService initialized: host={}, port={}, workerId={}",
                kvrocksHost, kvrocksPort, workerId);
    }

    // ==================== 设备ID映射 ====================

    /**
     * 获取或创建设备ID
     * Hash: device_id:{appId} field={deviceMd5} value={zgDeviceId}
     */
    public CompletableFuture<Long> getOrCreateDeviceId(Integer appId, String deviceMd5) {
        if (appId == null || deviceMd5 == null || deviceMd5.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String hashKey = DEVICE_ID_HASH + appId;

        return kvrocksClient.asyncHGet(hashKey, deviceMd5)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        existIdCount.incrementAndGet();
                        try {
                            return CompletableFuture.completedFuture(Long.parseLong(existingId));
                        } catch (NumberFormatException e) {
                            LOG.warn("设备ID格式错误: {}", existingId);
                        }
                    }
                    
                    // 生成新ID
                    Long newId = idGenerator.nextId();
                    String newIdStr = String.valueOf(newId);
                    
                    // 使用 HSETNX 原子写入
                    return kvrocksClient.asyncHSetIfAbsent(hashKey, deviceMd5, newIdStr)
                            .thenCompose(success -> {
                                if (success) {
                                    newIdCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(newId);
                                } else {
                                    // 发生冲突，重新获取
                                    conflictCount.incrementAndGet();
                                    return kvrocksClient.asyncHGet(hashKey, deviceMd5)
                                            .thenApply(id -> {
                                                try {
                                                    return id != null ? Long.parseLong(id) : null;
                                                } catch (NumberFormatException e) {
                                                    return null;
                                                }
                                            });
                                }
                            });
                });
    }

    // ==================== 用户ID映射 ====================

    /**
     * 获取或创建用户ID
     * Hash: user_id:{appId} field={cuid} value={zgUserId}
     */
    public CompletableFuture<Long> getOrCreateUserId(Integer appId, String cuid) {
        if (appId == null || cuid == null || cuid.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String hashKey = USER_ID_HASH + appId;

        return kvrocksClient.asyncHGet(hashKey, cuid)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        existIdCount.incrementAndGet();
                        try {
                            return CompletableFuture.completedFuture(Long.parseLong(existingId));
                        } catch (NumberFormatException e) {
                            LOG.warn("用户ID格式错误: {}", existingId);
                        }
                    }
                    
                    Long newId = idGenerator.nextId();
                    String newIdStr = String.valueOf(newId);
                    
                    return kvrocksClient.asyncHSetIfAbsent(hashKey, cuid, newIdStr)
                            .thenCompose(success -> {
                                if (success) {
                                    newIdCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(newId);
                                } else {
                                    conflictCount.incrementAndGet();
                                    return kvrocksClient.asyncHGet(hashKey, cuid)
                                            .thenApply(id -> {
                                                try {
                                                    return id != null ? Long.parseLong(id) : null;
                                                } catch (NumberFormatException e) {
                                                    return null;
                                                }
                                            });
                                }
                            });
                });
    }

    // ==================== 诸葛ID映射 (核心逻辑) ====================

    /**
     * 获取或创建诸葛ID - 完整实现用户设备绑定逻辑
     * 
     * 逻辑流程:
     * 1. 如果有 zgUserId:
     *    - 查询 user_zgid:{appId} → zgId
     *    - 如果用户已有zgId → 绑定设备到此zgId → 返回zgId
     *    - 如果用户无zgId:
     *      - 查询 device_zgid:{appId} → zgId
     *      - 如果设备有zgId → 将用户绑定到此zgId → 返回zgId
     *      - 如果设备无zgId → 生成新zgId → 绑定用户和设备 → 返回新zgId
     * 2. 如果无 zgUserId (匿名用户):
     *    - 查询 device_zgid:{appId} → zgId
     *    - 如果设备有zgId → 返回zgId
     *    - 如果设备无zgId → 生成新zgId → 绑定设备 → 返回新zgId
     */
    public CompletableFuture<Long> getOrCreateZgid(Integer appId, Long zgDeviceId, Long zgUserId) {
        if (appId == null || zgDeviceId == null) {
            return CompletableFuture.completedFuture(null);
        }

        String deviceZgidHash = DEVICE_ZGID_HASH + appId;
        String deviceField = String.valueOf(zgDeviceId);

        // 有实名用户
        if (zgUserId != null) {
            String userZgidHash = USER_ZGID_HASH + appId;
            String userField = String.valueOf(zgUserId);
            String zgidUserHash = ZGID_USER_HASH + appId;

            // 先查用户的zgid
            return kvrocksClient.asyncHGet(userZgidHash, userField)
                    .thenCompose(userZgid -> {
                        if (userZgid != null) {
                            existIdCount.incrementAndGet();
                            // 用户已有zgid，绑定设备到此zgid (幂等操作)
                            kvrocksClient.asyncHSet(deviceZgidHash, deviceField, userZgid);
                            bindingCount.incrementAndGet();
                            try {
                                return CompletableFuture.completedFuture(Long.parseLong(userZgid));
                            } catch (NumberFormatException e) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }

                        // 用户无zgid，查设备的zgid
                        return kvrocksClient.asyncHGet(deviceZgidHash, deviceField)
                                .thenCompose(deviceZgid -> {
                                    if (deviceZgid != null) {
                                        existIdCount.incrementAndGet();
                                        // 设备有zgid，将用户绑定到此zgid
                                        kvrocksClient.asyncHSet(userZgidHash, userField, deviceZgid);
                                        kvrocksClient.asyncHSet(zgidUserHash, deviceZgid, userField);
                                        bindingCount.incrementAndGet();
                                        try {
                                            return CompletableFuture.completedFuture(Long.parseLong(deviceZgid));
                                        } catch (NumberFormatException e) {
                                            return CompletableFuture.completedFuture(null);
                                        }
                                    }

                                    // 都没有，生成新zgId
                                    Long newZgid = idGenerator.nextId();
                                    String newZgidStr = String.valueOf(newZgid);
                                    newIdCount.incrementAndGet();

                                    // 使用 HSETNX 原子写入用户映射
                                    return kvrocksClient.asyncHSetIfAbsent(userZgidHash, userField, newZgidStr)
                                            .thenCompose(success -> {
                                                if (success) {
                                                    // 写入成功，同时绑定设备和反向映射
                                                    kvrocksClient.asyncHSet(deviceZgidHash, deviceField, newZgidStr);
                                                    kvrocksClient.asyncHSet(zgidUserHash, newZgidStr, userField);
                                                    return CompletableFuture.completedFuture(newZgid);
                                                } else {
                                                    // 发生冲突，获取实际的zgId
                                                    conflictCount.incrementAndGet();
                                                    return kvrocksClient.asyncHGet(userZgidHash, userField)
                                                            .thenApply(id -> {
                                                                if (id != null) {
                                                                    // 绑定设备到获取的zgId
                                                                    kvrocksClient.asyncHSet(deviceZgidHash, deviceField, id);
                                                                    try {
                                                                        return Long.parseLong(id);
                                                                    } catch (NumberFormatException e) {
                                                                        return null;
                                                                    }
                                                                }
                                                                return null;
                                                            });
                                                }
                                            });
                                });
                    });
        } else {
            // 匿名用户，只处理设备
            return kvrocksClient.asyncHGet(deviceZgidHash, deviceField)
                    .thenCompose(existingZgid -> {
                        if (existingZgid != null) {
                            existIdCount.incrementAndGet();
                            try {
                                return CompletableFuture.completedFuture(Long.parseLong(existingZgid));
                            } catch (NumberFormatException e) {
                                return CompletableFuture.completedFuture(null);
                            }
                        }

                        Long newZgid = idGenerator.nextId();
                        String newZgidStr = String.valueOf(newZgid);

                        return kvrocksClient.asyncHSetIfAbsent(deviceZgidHash, deviceField, newZgidStr)
                                .thenCompose(success -> {
                                    if (success) {
                                        newIdCount.incrementAndGet();
                                        return CompletableFuture.completedFuture(newZgid);
                                    } else {
                                        conflictCount.incrementAndGet();
                                        return kvrocksClient.asyncHGet(deviceZgidHash, deviceField)
                                                .thenApply(id -> {
                                                    try {
                                                        return id != null ? Long.parseLong(id) : null;
                                                    } catch (NumberFormatException e) {
                                                        return null;
                                                    }
                                                });
                                    }
                                });
                    });
        }
    }

    // ==================== 辅助方法 ====================

    /**
     * 自动生成 workerId
     * 
     * 基于 MAC地址 + PID + 线程ID + 时间戳 生成唯一的 workerId
     */
    private int generateWorkerId() {
        try {
            long workerId = 0;
            long macPart = getMacAddressPart();
            workerId ^= macPart;
            long pidPart = getProcessIdPart();
            workerId ^= (pidPart << 4);
            long threadPart = Thread.currentThread().getId();
            workerId ^= (threadPart << 8);
            long timePart = System.currentTimeMillis() & 0xFF;
            workerId ^= (timePart << 2);
            
            int result = (int) (Math.abs(workerId) % 1024);
            LOG.debug("workerId 生成: mac={}, pid={}, thread={}, time={}, result={}",
                    macPart, pidPart, threadPart, timePart, result);
            return result;
        } catch (Exception e) {
            LOG.warn("自动生成 workerId 失败，使用随机值", e);
            return (int) (Math.random() * 1024);
        }
    }

    private long getMacAddressPart() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface network = interfaces.nextElement();
                byte[] mac = network.getHardwareAddress();
                if (mac != null && mac.length >= 6) {
                    return ((mac[3] & 0xFFL) << 16) | ((mac[4] & 0xFFL) << 8) | (mac[5] & 0xFFL);
                }
            }
        } catch (Exception e) {
            LOG.debug("获取 MAC 地址失败", e);
        }
        try {
            return Math.abs(InetAddress.getLocalHost().getHostName().hashCode()) & 0xFFFFFF;
        } catch (Exception e) {
            return System.nanoTime() & 0xFFFFFF;
        }
    }

    private long getProcessIdPart() {
        try {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            String pidStr = jvmName.split("@")[0];
            return Long.parseLong(pidStr) & 0xFFFF;
        } catch (Exception e) {
            LOG.debug("获取 PID 失败", e);
            return System.nanoTime() & 0xFFFF;
        }
    }

    public KvrocksClient getKvrocksClient() {
        return kvrocksClient;
    }

    public SnowflakeIdGenerator getIdGenerator() {
        return idGenerator;
    }

    public void close() {
        if (kvrocksClient != null) {
            try {
                kvrocksClient.shutdown();
            } catch (Exception e) {
                LOG.warn("关闭KvrocksClient时出错", e);
            }
        }
        LOG.info("OneIdService closed. Stats: {}", getStats());
        if (idGenerator != null) {
            LOG.info("SnowflakeIdGenerator stats: {}", idGenerator.getStats());
        }
    }

    public String getStats() {
        return String.format("OneIdService - exist: %d, new: %d, conflict: %d, binding: %d",
                existIdCount.get(), newIdCount.get(), conflictCount.get(), bindingCount.get());
    }
}
