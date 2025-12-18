package com.zhugeio.etl.common.cache;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缓存服务工厂
 * 
 * 管理 ConfigCacheService 和 OneIdService 的单例
 * 同一 TaskManager 内的所有算子共享实例
 * 
 * 使用示例:
 * ```java
 * // 在算子 open() 中
 * CacheConfig config = CacheServiceFactory.createConfigFromFlink(parameters);
 * ConfigCacheService configCache = CacheServiceFactory.getConfigService(config);
 * OneIdService oneIdService = CacheServiceFactory.getOneIdService(config, workerId);
 * ```
 */
public class CacheServiceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CacheServiceFactory.class);

    // 配置缓存服务单例 (按名称区分)
    private static final Map<String, ConfigCacheService> CONFIG_SERVICES = new ConcurrentHashMap<>();

    // OneId 服务单例 (按名称区分)
    private static final Map<String, OneIdService> ONEID_SERVICES = new ConcurrentHashMap<>();

    // 锁对象
    private static final Object CONFIG_LOCK = new Object();
    private static final Object ONEID_LOCK = new Object();

    private CacheServiceFactory() {}

    // ===================== ConfigCacheService =====================

    /**
     * 获取或创建配置缓存服务 (使用默认名称)
     */
    public static ConfigCacheService getConfigService(CacheConfig config) {
        return getConfigService("default", config);
    }

    /**
     * 获取或创建配置缓存服务 (指定名称)
     */
    public static ConfigCacheService getConfigService(String name, CacheConfig config) {
        ConfigCacheService service = CONFIG_SERVICES.get(name);
        if (service != null) {
            return service;
        }

        synchronized (CONFIG_LOCK) {
            service = CONFIG_SERVICES.get(name);
            if (service != null) {
                return service;
            }

            service = new ConfigCacheService(config);
            service.init();
            CONFIG_SERVICES.put(name, service);
            LOG.info("Created ConfigCacheService instance: {}", name);
            return service;
        }
    }

    // ===================== OneIdService =====================

    /**
     * 获取或创建 OneId 服务 (使用默认名称)
     */
    public static OneIdService getOneIdService(CacheConfig config, int workerId) {
        return getOneIdService("default", config, workerId);
    }

    /**
     * 获取或创建 OneId 服务 (指定名称)
     */
    public static OneIdService getOneIdService(String name, CacheConfig config, int workerId) {
        OneIdService service = ONEID_SERVICES.get(name);
        if (service != null) {
            return service;
        }

        synchronized (ONEID_LOCK) {
            service = ONEID_SERVICES.get(name);
            if (service != null) {
                return service;
            }

            service = new OneIdService(
                    config.getKvrocksHost(),
                    config.getKvrocksPort(),
                    config.isKvrocksCluster(),
                    workerId
            );
            service.init();
            ONEID_SERVICES.put(name, service);
            LOG.info("Created OneIdService instance: {}, workerId={}", name, workerId);
            return service;
        }
    }

    // ===================== 配置构建 =====================

    /**
     * 从 Flink Configuration 创建 CacheConfig
     */
    public static CacheConfig createConfigFromFlink(Configuration parameters) {
        return CacheConfig.builder()
                .kvrocksHost(parameters.getString("kvrocks.host", "localhost"))
                .kvrocksPort(parameters.getInteger("kvrocks.port", 6379))
                .kvrocksCluster(parameters.getBoolean("kvrocks.cluster", false))
                .build();
    }

    /**
     * 从环境变量创建 CacheConfig
     */
    public static CacheConfig createConfigFromEnv() {
        return CacheConfig.builder()
                .kvrocksHost(System.getenv().getOrDefault("KVROCKS_HOST", "localhost"))
                .kvrocksPort(Integer.parseInt(System.getenv().getOrDefault("KVROCKS_PORT", "6379")))
                .kvrocksCluster(Boolean.parseBoolean(System.getenv().getOrDefault("KVROCKS_CLUSTER", "false")))
                .build();
    }

    // ===================== 关闭管理 =====================

    /**
     * 关闭指定的配置缓存服务
     */
    public static void closeConfigService(String name) {
        ConfigCacheService service = CONFIG_SERVICES.remove(name);
        if (service != null) {
            service.close();
            LOG.info("Closed ConfigCacheService: {}", name);
        }
    }

    /**
     * 关闭指定的 OneId 服务
     */
    public static void closeOneIdService(String name) {
        OneIdService service = ONEID_SERVICES.remove(name);
        if (service != null) {
            service.close();
            LOG.info("Closed OneIdService: {}", name);
        }
    }

    /**
     * 关闭所有服务
     */
    public static void closeAll() {
        LOG.info("Closing all cache services...");

        for (Map.Entry<String, ConfigCacheService> entry : CONFIG_SERVICES.entrySet()) {
            try {
                entry.getValue().close();
                LOG.info("Closed ConfigCacheService: {}", entry.getKey());
            } catch (Exception e) {
                LOG.error("Error closing ConfigCacheService: {}", entry.getKey(), e);
            }
        }
        CONFIG_SERVICES.clear();

        for (Map.Entry<String, OneIdService> entry : ONEID_SERVICES.entrySet()) {
            try {
                entry.getValue().close();
                LOG.info("Closed OneIdService: {}", entry.getKey());
            } catch (Exception e) {
                LOG.error("Error closing OneIdService: {}", entry.getKey(), e);
            }
        }
        ONEID_SERVICES.clear();

        LOG.info("All cache services closed");
    }

    // ===================== 状态查询 =====================

    /**
     * 获取所有配置服务的统计信息
     */
    public static String getAllConfigServiceStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("ConfigCacheService Stats:\n");
        for (Map.Entry<String, ConfigCacheService> entry : CONFIG_SERVICES.entrySet()) {
            sb.append("  [").append(entry.getKey()).append("] ")
              .append(entry.getValue().getStats()).append("\n");
        }
        return sb.toString();
    }

    /**
     * 获取所有 OneId 服务的统计信息
     */
    public static String getAllOneIdServiceStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("OneIdService Stats:\n");
        for (Map.Entry<String, OneIdService> entry : ONEID_SERVICES.entrySet()) {
            sb.append("  [").append(entry.getKey()).append("] ")
              .append(entry.getValue().getStats()).append("\n");
        }
        return sb.toString();
    }

    /**
     * 获取服务实例数量
     */
    public static int getConfigServiceCount() {
        return CONFIG_SERVICES.size();
    }

    public static int getOneIdServiceCount() {
        return ONEID_SERVICES.size();
    }
}
