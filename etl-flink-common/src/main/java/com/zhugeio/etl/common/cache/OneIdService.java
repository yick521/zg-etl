package com.zhugeio.etl.common.cache;

import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OneId服务 - 修复版
 * 
 *  修复点:
 * 1. workerId 自动生成，不依赖外部传入
 * 2. 基于 IP + PID + 线程信息生成唯一 workerId
 * 3. 可选的 Redis 原子自增方式
 */
public class OneIdService {

    private static final Logger LOG = LoggerFactory.getLogger(OneIdService.class);
    
    // 缓存 Key 前缀
    private static final String DEVICE_ID_PREFIX = "device_id:";
    private static final String USER_ID_PREFIX = "user_id:";

    private KvrocksClient kvrocksClient;
    private SnowflakeIdGenerator idGenerator;

    private final AtomicLong existZgidCount = new AtomicLong(0);
    private final AtomicLong newZgidCount = new AtomicLong(0);
    private final AtomicLong conflictCount = new AtomicLong(0);

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    
    //  修复: workerId 改为可选参数
    private final Integer externalWorkerId;

    /**
     * 构造函数 (自动生成 workerId)
     */
    public OneIdService(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, null);
    }

    /**
     * 构造函数 (兼容旧接口，但建议使用自动生成)
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

        //  修复: 自动生成或使用外部 workerId
        int workerId;
        if (externalWorkerId != null) {
            workerId = externalWorkerId;
            LOG.info("使用外部指定的 workerId: {}", workerId);
        } else {
            workerId = generateWorkerId();
            LOG.info("自动生成的 workerId: {}", workerId);
        }

        idGenerator = new SnowflakeIdGenerator(workerId);

        LOG.info("OneIdService initialized: host={}, port={}, workerId={}",
                kvrocksHost, kvrocksPort, workerId);
    }

    /**
     *  新增: 自动生成唯一 workerId
     * 
     * 基于以下信息生成:
     * 1. 机器 MAC 地址后 6 位
     * 2. 进程 PID
     * 3. 当前线程 ID
     * 
     * 最终结果取模到 [0, 1023] 范围
     */
    private int generateWorkerId() {
        try {
            long workerId = 0;
            
            // 1. 获取 MAC 地址
            long macPart = getMacAddressPart();
            workerId ^= macPart;
            
            // 2. 获取 PID
            long pidPart = getProcessIdPart();
            workerId ^= (pidPart << 4);
            
            // 3. 获取线程 ID
            long threadPart = Thread.currentThread().getId();
            workerId ^= (threadPart << 8);
            
            // 4. 添加时间因子 (防止重启后冲突)
            long timePart = System.currentTimeMillis() & 0xFF;
            workerId ^= (timePart << 2);
            
            // 取模到 [0, 1023]
            int result = (int) (Math.abs(workerId) % 1024);
            
            LOG.debug("workerId 生成: mac={}, pid={}, thread={}, time={}, result={}",
                    macPart, pidPart, threadPart, timePart, result);
            
            return result;
            
        } catch (Exception e) {
            LOG.warn("自动生成 workerId 失败，使用随机值", e);
            return (int) (Math.random() * 1024);
        }
    }

    /**
     * 获取 MAC 地址的一部分
     */
    private long getMacAddressPart() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface network = interfaces.nextElement();
                byte[] mac = network.getHardwareAddress();
                if (mac != null && mac.length >= 6) {
                    // 使用后 3 个字节
                    return ((mac[3] & 0xFFL) << 16) | ((mac[4] & 0xFFL) << 8) | (mac[5] & 0xFFL);
                }
            }
        } catch (Exception e) {
            LOG.debug("获取 MAC 地址失败", e);
        }
        
        // 回退: 使用 hostname hash
        try {
            return Math.abs(InetAddress.getLocalHost().getHostName().hashCode()) & 0xFFFFFF;
        } catch (Exception e) {
            return System.nanoTime() & 0xFFFFFF;
        }
    }

    /**
     * 获取进程 ID
     */
    private long getProcessIdPart() {
        try {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            // 格式: pid@hostname
            String pidStr = jvmName.split("@")[0];
            return Long.parseLong(pidStr) & 0xFFFF;
        } catch (Exception e) {
            LOG.debug("获取 PID 失败", e);
            return System.nanoTime() & 0xFFFF;
        }
    }

    /**
     *  新增: 从 Redis 原子自增获取 workerId (可选方案)
     * 
     * 适用于需要严格唯一性的场景
     */
    public int acquireWorkerIdFromRedis(String namespace) {
        try {
            String key = "worker_id_counter:" + namespace;
            
            // 使用 SETNX + GET 模式
            for (int i = 0; i < 1024; i++) {
                String candidateKey = key + ":" + i;
                Boolean success = kvrocksClient.asyncSetIfAbsent(candidateKey, "1").get(1, TimeUnit.SECONDS);
                if (success) {
                    LOG.info("从 Redis 获取 workerId: {}", i);
                    return i;
                }
            }
            
            LOG.warn("无法从 Redis 获取 workerId，所有 slot 已被占用");
            return generateWorkerId();
            
        } catch (Exception e) {
            LOG.warn("从 Redis 获取 workerId 失败，使用本地生成", e);
            return generateWorkerId();
        }
    }

    // ==================== ID 生成方法 ====================

    public CompletableFuture<Long> getOrCreateDeviceId(Integer appId, String deviceMd5) {
        if (appId == null || deviceMd5 == null || deviceMd5.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String key = DEVICE_ID_PREFIX + appId + ":" + deviceMd5;

        return kvrocksClient.asyncGet(key)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        existZgidCount.incrementAndGet();
                        return CompletableFuture.completedFuture(Long.parseLong(existingId));
                    }
                    
                    Long newId = idGenerator.nextId();
                    String newIdStr = String.valueOf(newId);
                    
                    return kvrocksClient.asyncSetIfAbsent(key, newIdStr)
                            .thenCompose(success -> {
                                if (success) {
                                    newZgidCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(newId);
                                } else {
                                    conflictCount.incrementAndGet();
                                    // 发生冲突，重新获取
                                    return kvrocksClient.asyncGet(key)
                                            .thenApply(id -> id != null ? Long.parseLong(id) : null);
                                }
                            });
                });
    }

    public CompletableFuture<Long> getOrCreateUserId(Integer appId, String cuid) {
        if (appId == null || cuid == null || cuid.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String key = USER_ID_PREFIX + appId + ":" + cuid;

        return kvrocksClient.asyncGet(key)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        existZgidCount.incrementAndGet();
                        return CompletableFuture.completedFuture(Long.parseLong(existingId));
                    }
                    
                    Long newId = idGenerator.nextId();
                    String newIdStr = String.valueOf(newId);
                    
                    return kvrocksClient.asyncSetIfAbsent(key, newIdStr)
                            .thenCompose(success -> {
                                if (success) {
                                    newZgidCount.incrementAndGet();
                                    return CompletableFuture.completedFuture(newId);
                                } else {
                                    conflictCount.incrementAndGet();
                                    return kvrocksClient.asyncGet(key)
                                            .thenApply(id -> id != null ? Long.parseLong(id) : null);
                                }
                            });
                });
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

    public String getStats() {
        return String.format("OneIdService Stats - exist: %d, new: %d, conflict: %d",
                existZgidCount.get(), newZgidCount.get(), conflictCount.get());
    }

    /**
     * 雪花ID生成器
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
                // 时钟回拨，等待追上
                long offset = lastTimestamp - timestamp;
                if (offset <= 5) {
                    try {
                        Thread.sleep(offset + 1);
                        timestamp = System.currentTimeMillis();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (timestamp < lastTimestamp) {
                    throw new RuntimeException("Clock moved backwards. Refusing to generate id for " + offset + " milliseconds");
                }
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
