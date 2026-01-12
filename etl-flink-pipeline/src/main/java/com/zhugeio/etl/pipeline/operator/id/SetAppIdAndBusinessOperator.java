package com.zhugeio.etl.pipeline.operator.id;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheKeyConstants;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.Dims;
import com.zhugeio.etl.common.lock.LockManager;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * 设置AppId和Business算子 - 改造版
 * <p>
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 3. 写入MySQL后同步更新KV和缓存
 * <p>
 * KVRocks Key 对照 (与同步服务一致):
 * - appKeyAppIdMap (Hash): field=appKey
 * - cidByAidMap (Hash): field=appId
 * - appIdSdkHasDataMap (Hash): field=appId_platform
 */
public class SetAppIdAndBusinessOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(SetAppIdAndBusinessOperator.class);

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private transient KvrocksClient kvrocksClient;
    private transient HikariDataSource dataSource;
    private transient LockManager lockManager;

    private final CacheConfig cacheConfig;
    private final Properties dbProperties;

    public SetAppIdAndBusinessOperator(CacheConfig cacheConfig, Properties dbProperties) {
        this.cacheConfig = cacheConfig;
        this.dbProperties = dbProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);
        kvrocksClient = cacheService.getKvrocksClient();
        lockManager = new LockManager(kvrocksClient);

        if (dbProperties != null && dbProperties.containsKey("rdbms.url")) {
            initializeConnectionPool(parameters);
        }

        LOG.info("[SetAppIdAndBusinessOperator-{}] 初始化成功 (使用统一缓存服务)",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    private void initializeConnectionPool(Configuration parameters) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbProperties.getProperty("rdbms.url"));
        config.setUsername(dbProperties.getProperty("rdbms.userName"));
        config.setPassword(dbProperties.getProperty("rdbms.password"));
        config.setDriverClassName(dbProperties.getProperty("rdbms.driverClass", "com.mysql.cj.jdbc.Driver"));
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000L);
        config.setIdleTimeout(600000L);
        config.setMaxLifetime(1800000L);
        config.setConnectionTestQuery("SELECT 1");
        config.setPoolName("SetAppId-MySQL-Pool-" + getRuntimeContext().getIndexOfThisSubtask());

        dataSource = new HikariDataSource(config);
        LOG.info("SetAppId MySQL连接池初始化成功");
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        Integer appId = input.getAppId();
        Integer plat = Dims.sdk(String.valueOf(input.getData().get("pl")));

        // 检查是否有数据
        cacheService.hasAppSdkData(appId, plat)
                .thenCompose(hasData -> {
                    LOG.debug("检查是否有数据: appId={}, plat={} ,hasData: {}", appId, plat,hasData);
                    // 异步写入MySQL（如果没有数据）
                    if (!hasData) {
                        String lockKey = lockManager.appSdkLockKey(appId, plat);
                        LOG.debug("获取锁: lockKey: {}", lockKey);
                        Supplier<CompletableFuture<Void>> writeOperation = () -> {
                            CompletableFuture<Void> future = new CompletableFuture<>();
                            try {
                                // 幂等写入
                                boolean success = writeToMysql(appId, plat);
                                LOG.debug("写入MySQL结果: success:{}, appId={}, platform={}", success,appId, plat);

                                if (success) {
                                    syncToKVAndCache(appId, plat);
                                }

                                future.complete(null);
                            } catch (Exception e) {
                                LOG.error("写入MySQL失败: appId={}, platform={}", appId, plat, e);
                                future.completeExceptionally(e);
                            }
                            return future;
                        };

                        // 这里没有采用双重检验，而是在写入时做了幂等操作
                        return lockManager.executeWithLockAsync(lockKey, writeOperation)
                                .exceptionally(ex -> {
                                    LOG.error("获取锁或写入MySQL失败: appId={}, platform={}", appId, plat, ex);
                                    return null;
                                })
                                // 获取 companyId
                                .thenCompose(v -> cacheService.getCompanyIdByAppId(appId));
                    }

                    // 获取 companyId
                    return cacheService.getCompanyIdByAppId(appId);
                })
                .thenApply(companyId -> createOutput(input, appId))
                .whenComplete((output, throwable) -> {
                    if (throwable != null) {
                        LOG.error("处理appId失败: appId is {} ", appId, throwable);
                        resultFuture.complete(Collections.singleton(createErrorOutput(input)));
                    } else {
                        resultFuture.complete(Collections.singleton(output));
                    }
                });
    }

    /**
     * 幂等写入MySQL的 app、app_create_notice
     *
     *
     * 幂等写入:
     * INSERT IGNORE INTO app_create_notice(app_id,created,insert_time) VALUES (?,0,NOW())
     * CREATE TABLE `app_create_notice` (
     *   `app_id` int(11) NOT NULL,
     *   `created` int(1) NOT NULL COMMENT '0:没有创建，1：创建好了',
     *   `insert_time` datetime NOT NULL,
     *   `update_time` datetime DEFAULT NULL,
     *   PRIMARY KEY (`app_id`)
     * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     *
     * 幂等写入:
     * INSERT IGNORE INTO app(main_id,sdk_platform,config_info,has_data,creat_time,is_delete,has_data_time) VALUES (?,?,NULL,1,NOW(),0,NOW())
     * CREATE TABLE `app` (
     *   `id` int(11) NOT NULL AUTO_INCREMENT,
     *   `main_id` int(11) NOT NULL,
     *   `sdk_platform` int(11) NOT NULL,
     *   `config_info` mediumtext,
     *   `has_data` tinyint(4) NOT NULL DEFAULT '0',
     *   `creat_time` datetime NOT NULL,
     *   `is_delete` tinyint(4) NOT NULL DEFAULT '0',
     *   `event_sum` int(11) NOT NULL DEFAULT '3000',
     *   `attr_sum` int(11) NOT NULL DEFAULT '1000',
     *   `server06_app_update_time` datetime DEFAULT NULL,
     *   `server06_update_time` datetime DEFAULT NULL,
     *   `server58_app_update_time` datetime DEFAULT NULL,
     *   `server58_update_time` datetime DEFAULT NULL,
     *   `zhugeioinf_update_time` datetime DEFAULT NULL,
     *   `zhugeioinf_app_update_time` datetime DEFAULT NULL,
     *   `server177_update_time` datetime DEFAULT NULL,
     *   `server177_app_update_time` datetime DEFAULT NULL,
     *   `infobright_update_time` datetime DEFAULT NULL,
     *   `infobright_app_update_time` datetime DEFAULT NULL,
     *   `infobright4_update_time` datetime DEFAULT NULL,
     *   `infobright4_app_update_time` datetime DEFAULT NULL,
     *   `infobright3_update_time` datetime DEFAULT NULL,
     *   `infobright3_app_update_time` datetime DEFAULT NULL,
     *   `has_data_time` datetime DEFAULT NULL,
     *   `etl_load_level` varchar(20) NOT NULL DEFAULT 'DAY' COMMENT 'MINUTE,HOUR,DAY',
     *   PRIMARY KEY (`id`),
     *   UNIQUE KEY `id_platform_uniq` (`main_id`,`sdk_platform`)
     * ) ENGINE=InnoDB AUTO_INCREMENT=209 DEFAULT CHARSET=utf8;
     * @return 是否写入成功
     */
    private boolean writeToMysql(Integer appId, Integer platform) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            // 插入到app表
            String appSql = "INSERT IGNORE INTO app(main_id,sdk_platform,config_info,has_data,creat_time,is_delete,has_data_time) " +
                    "VALUES (?,?,NULL,1,NOW(),0,NOW())";
            LOG.debug("写入MySQL[app]: appId={}, platform={}, sql={}", appId, platform,appSql);
            try (PreparedStatement stmt = conn.prepareStatement(appSql)) {
                stmt.setInt(1, appId);
                stmt.setInt(2, platform);
                stmt.executeUpdate();
            }

            // 插入到app_create_notice表
            String noticeSql = "INSERT IGNORE INTO app_create_notice(app_id,created,insert_time) VALUES (?,0,NOW())";
            LOG.debug("写入MySQL[app]: appId={}, sql={}", appId, noticeSql);
            try (PreparedStatement stmt = conn.prepareStatement(noticeSql)) {
                stmt.setInt(1, appId);
                stmt.executeUpdate();
            }

            conn.commit();
            LOG.debug("成功写入MySQL[app|app_create_notice]: appId={}, platform={}", appId, platform);
            return true;

        } catch (SQLException e) {
            LOG.error("写入MySQL失败: appId={}, platform={}", appId, platform, e);
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ignored) {}
            }
            return false;
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException ignored) {}
            }
        }
    }

    /**
     * 同步到KV和缓存
     * 参考 EventAttrAsyncOperator 的实现方式
     */
    private void syncToKVAndCache(Integer appId, Integer platform) {
        try {
            boolean cluster = cacheConfig.isKvrocksCluster();

            // 构建KV Key和Field
            String hasDataKey = cluster
                    ? "{" + CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP + "}"
                    : CacheKeyConstants.APP_ID_SDK_HAS_DATA_MAP;

            String hasDataField = CacheKeyConstants.appSdkHasDataField(appId, platform);

            // 异步写入KV (使用 HSET )
            kvrocksClient.asyncHSet(hasDataKey, hasDataField, "1")
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            LOG.error("同步KV失败: appId={}, platform={}", appId, platform, ex);
                        } else {
                            LOG.info("同步KV成功: appId={}, platform={}, key={}, field={}",
                                    appId, platform, hasDataKey, hasDataField);
                        }
                    });

            // 更新内存缓存
            cacheService.putAppSdkHasDataCache(appId, platform, true);

            LOG.info("同步KV和缓存完成: appId={}, platform={}", appId, platform);

        } catch (Exception e) {
            LOG.error("同步KV和缓存失败: appId={}, platform={}", appId, platform, e);
        }
    }

    private ZGMessage createOutput(ZGMessage input, Integer appId) {
        String business = input.getData().getOrDefault("business", "").toString();

        ZGMessage output = new ZGMessage();
        input.getData().put("business", business);
        input.getData().put("app_id", appId);
        input.getData().put("sdk", input.getSdk());

        output.setTopic(input.getTopic());
        output.setPartition(input.getPartition());
        output.setOffset(input.getOffset());
        output.setKey(input.getKey());
        output.setRawData(input.getRawData());
        output.setResult(input.getResult());
        output.setAppKey(input.getAppKey());
        output.setSdk(input.getSdk());
        output.setData(input.getData());
        output.setErrData(input.getErrData());
        output.setJson(input.getJson());
        output.setError(input.getError());
        output.setErrorCode(input.getErrorCode());
        output.setErrorDescribe(input.getErrorDescribe());
        output.setBusiness(business);
        output.setAppId(appId != null ? appId : 0);

        return output;
    }

    private ZGMessage createErrorOutput(ZGMessage input) {
        ZGMessage output = new ZGMessage();

        output.setTopic(input.getTopic());
        output.setPartition(input.getPartition());
        output.setOffset(input.getOffset());
        output.setKey(input.getKey());
        output.setRawData(input.getRawData());
        output.setResult(-1);
        output.setAppKey(input.getAppKey());
        output.setSdk(input.getSdk());
        output.setData(input.getData());
        output.setErrData(input.getData());
        int errorCode = ErrorMessageEnum.AK_NONE.getErrorCode();
        String errorInfo = ErrorMessageEnum.AK_NONE.getErrorMessage();

        output.setErrorCode(errorCode);
        output.setErrorDescribe(errorInfo);
        output.setError(errorInfo);
        output.setAppId(0);
        output.setBusiness("");

        return output;
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("MySQL连接池已关闭");
        }
        LOG.info("[SetAppIdAndBusinessOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }
}