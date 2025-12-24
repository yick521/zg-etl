package com.zhugeio.etl.pipeline.operator.id;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.Dims;
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

/**
 * 设置AppId和Business算子 - 改造版
 * 
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 
 * KVRocks Key 对照 (与同步服务一致):
 * - appKeyAppIdMap (Hash): field=appKey
 * - cidByAidMap (Hash): field=appId
 * - appIdSdkHasDataMap (Hash): field=appId_platform
 */
public class SetAppIdAndBusinessOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(SetAppIdAndBusinessOperator.class);

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private transient HikariDataSource dataSource;

    private CacheConfig cacheConfig;
    private final Properties dbProperties;

    public SetAppIdAndBusinessOperator(CacheConfig cacheConfig) {
        this(cacheConfig, null);
    }

    public SetAppIdAndBusinessOperator(CacheConfig cacheConfig, Properties dbProperties) {
        this.cacheConfig = cacheConfig;
        this.dbProperties = dbProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);

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
        String ak = input.getAppKey();
        Integer plat = Dims.sdk(String.valueOf(input.getData().get("pl")));

        // 使用统一缓存服务获取 appId
        cacheService.getAppIdByAppKey(ak)
                .thenCompose(appId -> {
                    if (appId != null) {
                        // 检查是否有数据
                        return cacheService.hasAppSdkData(appId, plat)
                                .thenCompose(hasData -> {
                                    // 异步写入MySQL（如果没有数据）
                                    if (!hasData && dataSource != null) {
                                        CompletableFuture.runAsync(() -> writeToMysql(appId, plat));
                                    }

                                    // 获取 companyId
                                    return cacheService.getCompanyIdByAppId(appId)
                                            .thenApply(companyId -> {
                                                ZGMessage output = createOutput(input, appId, companyId);
                                                return output;
                                            });
                                });
                    } else {
                        ZGMessage output = createErrorOutput(input);
                        return CompletableFuture.completedFuture(output);
                    }
                })
                .whenComplete((output, throwable) -> {
                    if (throwable != null) {
                        LOG.error("处理appId失败: ak={}", ak, throwable);
                        resultFuture.complete(Collections.singleton(createErrorOutput(input)));
                    } else {
                        resultFuture.complete(Collections.singleton(output));
                    }
                });
    }

    private void writeToMysql(Integer appId, Integer platform) {
        if (dataSource == null) return;
        
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);

            // 插入到app表
            String appSql = "INSERT IGNORE INTO app(main_id,sdk_platform,config_info,has_data,creat_time,is_delete,has_data_time) VALUES (?,?,NULL,1,NOW(),0,NOW())";
            try (PreparedStatement stmt = conn.prepareStatement(appSql)) {
                stmt.setInt(1, appId);
                stmt.setInt(2, platform);
                stmt.executeUpdate();
            }

            // 插入到app_create_notice表
            String noticeSql = "INSERT IGNORE INTO app_create_notice(app_id,created,insert_time) VALUES (?,0,NOW())";
            try (PreparedStatement stmt = conn.prepareStatement(noticeSql)) {
                stmt.setInt(1, appId);
                stmt.executeUpdate();
            }

            conn.commit();
            LOG.debug("成功写入MySQL: appId={}, platform={}", appId, platform);

        } catch (SQLException e) {
            LOG.error("写入MySQL失败: appId={}, platform={}", appId, platform, e);
            if (conn != null) {
                try { conn.rollback(); } catch (SQLException ignored) {}
            }
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (SQLException ignored) {}
            }
        }
    }

    private ZGMessage createOutput(ZGMessage input, Integer appId, Integer companyId) {
        String business = input.getData().getOrDefault("business", "").toString();

        ZGMessage output = new ZGMessage();
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
