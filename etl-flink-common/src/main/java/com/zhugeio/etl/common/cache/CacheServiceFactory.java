package com.zhugeio.etl.common.cache;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConfigCacheService 工厂类 - 静态单例
 *
 * 设计说明:
 * - 一个 TaskManager (JVM) 内只有一个 ConfigCacheService 实例
 * - 多个 Slot 共享同一个实例，节省内存和 KVRocks 连接
 * - 完全依赖 L1 缓存的 TTL 过期机制，不做版本检查
 *
 * 使用方式:
 * 1. Job 启动时初始化: CacheServiceFactory.getInstance(config)
 * 2. 算子中直接获取: CacheServiceFactory.getInstance()
 */
public class CacheServiceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CacheServiceFactory.class);

    // JVM 级别单例
    private static volatile ConfigCacheService INSTANCE;
    private static final Object LOCK = new Object();

    private CacheServiceFactory() {}

    // ===================== 获取单例 =====================

    /**
     * 初始化并获取单例 (首次调用时使用)
     */
    public static ConfigCacheService getInstance(CacheConfig config) {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    LOG.info("Creating ConfigCacheService singleton: {}:{}",
                            config.getKvrocksHost(), config.getKvrocksPort());
                    INSTANCE = new ConfigCacheService(config);
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 获取已存在的单例
     *
     * @throws IllegalStateException 如果未初始化
     */
    public static ConfigCacheService getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException(
                    "ConfigCacheService not initialized. Call getInstance(config) first.");
        }
        return INSTANCE;
    }

    /**
     * 尝试获取单例，未初始化时返回 null
     */
    public static ConfigCacheService getInstanceOrNull() {
        return INSTANCE;
    }

    // ===================== 生命周期 =====================

    /**
     * 关闭单例
     */
    public static void close() {
        synchronized (LOCK) {
            if (INSTANCE != null) {
                try {
                    INSTANCE.close();
                    LOG.info("ConfigCacheService singleton closed");
                } catch (Exception e) {
                    LOG.error("Error closing ConfigCacheService", e);
                }
                INSTANCE = null;
            }
        }
    }

    /**
     * 检查是否已初始化
     */
    public static boolean isInitialized() {
        return INSTANCE != null;
    }

    // ===================== 配置创建辅助方法 =====================

    /**
     * 从 ParameterTool 创建配置
     */
    public static CacheConfig createConfig(ParameterTool params) {
        return CacheConfig.builder()
                .kvrocksHost(params.get("kvrocks.host", "localhost"))
                .kvrocksPort(params.getInt("kvrocks.port", 6379))
                .kvrocksCluster(params.getBoolean("kvrocks.cluster", false))
                .kvrocksTimeout(params.getInt("kvrocks.timeout", 60))
                .kvrocksPassword(params.get("kvrocks.password", null))
                .build();
    }

    /**
     * 从环境变量创建配置
     */
    public static CacheConfig createConfigFromEnv() {
        return CacheConfig.builder()
                .kvrocksHost(getEnvOrDefault("KVROCKS_HOST", "localhost"))
                .kvrocksPort(Integer.parseInt(getEnvOrDefault("KVROCKS_PORT", "6379")))
                .kvrocksCluster(Boolean.parseBoolean(getEnvOrDefault("KVROCKS_CLUSTER", "false")))
                .kvrocksTimeout(Integer.parseInt(getEnvOrDefault("KVROCKS_TIMEOUT", "60")))
                .kvrocksPassword(System.getenv("KVROCKS_PASSWORD"))
                .build();
    }

    private static String getEnvOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }
}