package com.zhugeio.etl.pipeline.service;

import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.util.SnowflakeIdGenerator;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OneId服务 - 统一ID管理 (静态单例版本，适用于Flink TaskManager)
 *
 *  使用外部 SnowflakeIdGenerator (支持时钟回拨等待)
 *  使用 Hash 结构存储，与原 Scala FrontCache 完全一致
 *  采用双重检查锁定(DCL)实现线程安全的懒加载单例
 *
 * Hash结构 (与原Scala一致):
 * - device_id:{appId}  field={deviceMd5} value={zgDeviceId}   设备MD5→设备ID
 * - user_id:{appId}    field={cuid}      value={zgUserId}     用户标识→用户ID
 * - device_zgid:{appId} field={zgDeviceId} value={zgId}       设备→诸葛ID
 * - user_zgid:{appId}   field={zgUserId}   value={zgId}       用户→诸葛ID
 * - zgid_user:{appId}   field={zgId}       value={zgUserId}   诸葛ID→用户(反向)
 *
 * ⚠ 重要: 这些ID映射不使用本地缓存！
 *    使用 "先读后写 + HSETNX" 模式保证分布式唯一性
 *
 * 使用方式:
 *   // 首次初始化 (在Flink作业启动时调用一次)
 *   OneIdService.initialize("kvrocks-host", 6379, true);
 *
 *   // 之后在任意地方获取实例
 *   OneIdService service = OneIdService.getInstance();
 */
public class OneIdService {

    private static final Logger LOG = LoggerFactory.getLogger(OneIdService.class);

    // ==================== 单例相关 ====================

    /** 单例实例 (使用 volatile 保证可见性) */
    private static volatile OneIdService INSTANCE;

    /** 初始化锁 */
    private static final Object INIT_LOCK = new Object();

    /** 是否已初始化 */
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

    // ==================== 配置参数 (静态存储，用于延迟初始化) ====================

    private static volatile String staticKvrocksHost;
    private static volatile int staticKvrocksPort;
    private static volatile boolean staticKvrocksCluster;
    private static volatile Integer staticWorkerId;

    // ==================== Hash Key 前缀 (与原Scala一致) ====================

    private static final String DEVICE_ID_HASH = "device_id:";      // device_id:{appId}
    private static final String USER_ID_HASH = "user_id:";          // user_id:{appId}
    private static final String DEVICE_ZGID_HASH = "device_zgid:";  // device_zgid:{appId}
    private static final String USER_ZGID_HASH = "user_zgid:";      // user_zgid:{appId}
    private static final String ZGID_USER_HASH = "zgid_user:";      // zgid_user:{appId}

    // ==================== 实例成员 ====================

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

    // ==================== 单例方法 ====================

    /**
     * 配置并初始化单例 (自动生成 workerId)
     *
     * 线程安全，可多次调用，只有首次调用生效
     * 建议在 Flink 作业启动时调用
     *
     * @param kvrocksHost KVRocks 主机地址
     * @param kvrocksPort KVRocks 端口
     * @param kvrocksCluster 是否集群模式
     */
    public static void initialize(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        initialize(kvrocksHost, kvrocksPort, kvrocksCluster, null);
    }

    /**
     * 配置并初始化单例 (指定 workerId)
     *
     * 线程安全，可多次调用，只有首次调用生效
     *
     * @param kvrocksHost KVRocks 主机地址
     * @param kvrocksPort KVRocks 端口
     * @param kvrocksCluster 是否集群模式
     * @param workerId 指定的 workerId (null 表示自动生成)
     */
    public static void initialize(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, Integer workerId) {
        if (INITIALIZED.get()) {
            LOG.warn("OneIdService 已经初始化，忽略重复调用");
            return;
        }

        synchronized (INIT_LOCK) {
            if (INITIALIZED.get()) {
                LOG.warn("OneIdService 已经初始化，忽略重复调用");
                return;
            }

            // 保存配置
            staticKvrocksHost = kvrocksHost;
            staticKvrocksPort = kvrocksPort;
            staticKvrocksCluster = kvrocksCluster;
            staticWorkerId = workerId;

            // 创建并初始化实例
            INSTANCE = new OneIdService(kvrocksHost, kvrocksPort, kvrocksCluster, workerId);
            INSTANCE.init();

            INITIALIZED.set(true);
            LOG.info("OneIdService 单例初始化完成: host={}, port={}, cluster={}",
                    kvrocksHost, kvrocksPort, kvrocksCluster);
        }
    }

    /**
     * 获取单例实例
     *
     * @return OneIdService 实例
     * @throws IllegalStateException 如果尚未初始化
     */
    public static OneIdService getInstance() {
        if (!INITIALIZED.get()) {
            throw new IllegalStateException(
                    "OneIdService 尚未初始化，请先调用 OneIdService.initialize(host, port, cluster)");
        }
        return INSTANCE;
    }

    /**
     * 获取单例实例，如果未初始化则使用给定参数初始化
     *
     * 适用于 Flink 算子中的懒加载场景
     *
     * @param kvrocksHost KVRocks 主机地址
     * @param kvrocksPort KVRocks 端口
     * @param kvrocksCluster 是否集群模式
     * @return OneIdService 实例
     */
    public static OneIdService getOrCreate(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        return getOrCreate(kvrocksHost, kvrocksPort, kvrocksCluster, null);
    }

    /**
     * 获取单例实例，如果未初始化则使用给定参数初始化
     *
     * 适用于 Flink 算子中的懒加载场景
     *
     * @param kvrocksHost KVRocks 主机地址
     * @param kvrocksPort KVRocks 端口
     * @param kvrocksCluster 是否集群模式
     * @param workerId 指定的 workerId (null 表示自动生成)
     * @return OneIdService 实例
     */
    public static OneIdService getOrCreate(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, Integer workerId) {
        if (!INITIALIZED.get()) {
            initialize(kvrocksHost, kvrocksPort, kvrocksCluster, workerId);
        }
        return INSTANCE;
    }

    /**
     * 检查是否已初始化
     */
    public static boolean isInitialized() {
        return INITIALIZED.get();
    }

    /**
     * 关闭并重置单例 (谨慎使用，主要用于测试)
     */
    public static void shutdown() {
        synchronized (INIT_LOCK) {
            if (INSTANCE != null) {
                INSTANCE.close();
                INSTANCE = null;
            }
            INITIALIZED.set(false);
            staticKvrocksHost = null;
            staticKvrocksPort = 0;
            staticKvrocksCluster = false;
            staticWorkerId = null;
            LOG.info("OneIdService 单例已关闭并重置");
        }
    }

    // ==================== 构造函数 (私有) ====================

    /**
     * 私有构造函数 - 防止外部直接实例化
     */
    private OneIdService(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, Integer workerId) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.externalWorkerId = workerId;
    }

    /**
     * 初始化内部组件
     */
    private void init() {
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
     * 用于本类返回值的封装
     */
    @Data
    public static class OneIdResult implements Serializable {
        /**
         * 是否是新创建的id
         * 查询获取到的id为 false
         */
        private Boolean isNew;
        private Long id;

        public static OneIdResult build(Long id) {
            return build(false, id);
        }
        public static OneIdResult build(String id) {
            return build(false, id);
        }
        public static OneIdResult build(Boolean isNew,String id) {
            return build(isNew, Long.parseLong(id));
        }
        public static OneIdResult build(Boolean isNew,Long id) {
            OneIdResult result = new OneIdResult();
            result.setIsNew(isNew);
            result.setId(id);
            return result;
        }
    }

    /**
     * 获取或创建设备ID
     * Hash: device_id:{appId} field={deviceMd5} value={zgDeviceId}
     * @param appId
     * @param deviceMd5
     * @return OneIdResult (id, isNew)
     */
    public CompletableFuture<OneIdResult> getOrCreateDeviceId(Integer appId, String deviceMd5) {
        if (appId == null || deviceMd5 == null || deviceMd5.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String hashKey = DEVICE_ID_HASH + appId;

        return kvrocksClient.asyncHGet(hashKey, deviceMd5)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        existIdCount.incrementAndGet();
                        try {
                            return CompletableFuture.completedFuture(OneIdResult.build(existingId));
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
                                    return CompletableFuture.completedFuture(OneIdResult.build(true,newId));
                                } else {
                                    // 发生冲突，重新获取
                                    conflictCount.incrementAndGet();
                                    return kvrocksClient.asyncHGet(hashKey, deviceMd5)
                                            .thenApply(id -> {
                                                try {
                                                    return OneIdResult.build(id);
                                                } catch (NumberFormatException e) {
                                                    LOG.warn("设备ID格式错误: {}", id);
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
    public CompletableFuture<OneIdResult> getOrCreateUserId(Integer appId, String cuid) {
        if (appId == null || cuid == null || cuid.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        String hashKey = USER_ID_HASH + appId;

        return kvrocksClient.asyncHGet(hashKey, cuid)
                .thenCompose(existingId -> {
                    if (existingId != null) {
                        existIdCount.incrementAndGet();
                        try {
                            return CompletableFuture.completedFuture(OneIdResult.build(existingId));
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
                                    return CompletableFuture.completedFuture(OneIdResult.build(true,newId));
                                } else {
                                    conflictCount.incrementAndGet();
                                    return kvrocksClient.asyncHGet(hashKey, cuid)
                                            .thenApply(id -> {
                                                try {
                                                    return OneIdResult.build(id);
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

    /** WorkerId 分配的 Redis Key */
    private static final String WORKER_ID_KEY = "oneid:worker_id_seq";

    /** WorkerId 最大值 (Snowflake 通常支持 0-1023) */
    private static final int MAX_WORKER_ID = 1023;

    /**
     * 自动生成 workerId
     *
     * 使用 Kvrocks INCR 原子操作分配全局唯一的 workerId
     * 取模确保在有效范围内 (0-1023)
     */
    private int generateWorkerId() {
        try {
            // 使用同步 incr 方法获取全局唯一序号
            Long seq = kvrocksClient.incr("oneid:worker_id_seq");

            if (seq == null) {
                LOG.warn("Kvrocks INCR 返回 null，使用备用方案");
                return generateFallbackWorkerId();
            }

            int workerId = (int) (seq % (MAX_WORKER_ID + 1));
            LOG.info("从 Kvrocks 分配 workerId: seq={}, workerId={}", seq, workerId);
            return workerId;

        } catch (Exception e) {
            LOG.warn("从 Kvrocks 获取 workerId 失败，使用备用方案", e);
            return generateFallbackWorkerId();
        }
    }

    /**
     * 备用 workerId 生成方案
     *
     * 基于 MAC地址 + PID + 随机数，尽量减少冲突
     */
    private int generateFallbackWorkerId() {
        try {
            long workerId = 0;

            // MAC 地址部分
            long macPart = getMacAddressPart();
            workerId ^= macPart;

            // PID 部分
            long pidPart = getProcessIdPart();
            workerId ^= (pidPart << 4);

            // 使用 SecureRandom 增加随机性，避免同一机器上的冲突
            java.security.SecureRandom random = new java.security.SecureRandom();
            long randomPart = random.nextInt(1024);
            workerId ^= (randomPart << 8);

            int result = (int) (Math.abs(workerId) % (MAX_WORKER_ID + 1));
            LOG.warn("使用备用方案生成 workerId: mac={}, pid={}, random={}, result={}",
                    macPart, pidPart, randomPart, result);
            return result;
        } catch (Exception e) {
            LOG.error("备用 workerId 生成也失败，使用纯随机值", e);
            return new java.security.SecureRandom().nextInt(MAX_WORKER_ID + 1);
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

    /**
     * 关闭实例资源 (内部使用)
     */
    private void close() {
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