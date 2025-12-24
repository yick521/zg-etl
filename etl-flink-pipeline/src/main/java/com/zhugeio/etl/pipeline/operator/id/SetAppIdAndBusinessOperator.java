package com.zhugeio.etl.pipeline.operator.id;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.model.App;
import com.zhugeio.etl.pipeline.model.CompanyApp;
import com.zhugeio.etl.common.util.Dims;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 设置AppId和Business异步算子
 * <p>
 * 优化点:
 * 1. ✅ 使用缓存减少重复查询
 * 2. ✅ 所有操作都是异步的
 * 3. ✅ 支持Kvrocks集群和单机模式
 */
public class SetAppIdAndBusinessOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, CompanyApp> appCache;
    private transient HikariDataSource dataSource;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    private Properties dbProperties;
    private String dbDriverClass;
    private String dbUrl;
    private String dbUserName;
    private String dbPassword;

    private static final ConfigOption<String> DB_DRIVER_CLASS = ConfigOptions
            .key("rdbms.driverClass")
            .stringType()
            .defaultValue("com.mysql.cj.jdbc.Driver");

    private static final ConfigOption<String> DB_URL = ConfigOptions
            .key("rdbms.url")
            .stringType()
            .defaultValue("jdbc:mysql://localhost:3306/sdkv?useUnicode=true&characterEncoding=utf8&autoReconnect=true&useSSL=false&serverTimezone=UTC");

    private static final ConfigOption<String> DB_USERNAME = ConfigOptions
            .key("rdbms.userName")
            .stringType()
            .defaultValue("root");

    private static final ConfigOption<String> DB_PASSWORD = ConfigOptions
            .key("rdbms.password")
            .stringType()
            .defaultValue("");

    private static final ConfigOption<Integer> HIKARI_MAX_POOL_SIZE = ConfigOptions
            .key("hikari.maximum.pool.size")
            .intType()
            .defaultValue(10);

    private static final ConfigOption<Integer> HIKARI_MIN_IDLE = ConfigOptions
            .key("hikari.minimum.idle")
            .intType()
            .defaultValue(2);

    private static final ConfigOption<Long> HIKARI_CONNECTION_TIMEOUT = ConfigOptions
            .key("hikari.connection.timeout")
            .longType()
            .defaultValue(30000L);

    private static final ConfigOption<Long> HIKARI_IDLE_TIMEOUT = ConfigOptions
            .key("hikari.idle.timeout")
            .longType()
            .defaultValue(600000L);

    private static final ConfigOption<Long> HIKARI_MAX_LIFETIME = ConfigOptions
            .key("hikari.max.lifetime")
            .longType()
            .defaultValue(1800000L);

    private static final Logger LOG = LoggerFactory.getLogger(SetAppIdAndBusinessOperator.class);

    public SetAppIdAndBusinessOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
    }
    
    public SetAppIdAndBusinessOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, Properties dbProperties) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster);
        this.dbProperties = dbProperties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (dbProperties != null) {
            dbDriverClass = dbProperties.getProperty("rdbms.driverClass", DB_DRIVER_CLASS.defaultValue());
            dbUrl = dbProperties.getProperty("rdbms.url", DB_URL.defaultValue());
            dbUserName = dbProperties.getProperty("rdbms.userName", DB_USERNAME.defaultValue());
            dbPassword = dbProperties.getProperty("rdbms.password", DB_PASSWORD.defaultValue());
            initializeConnectionPool(parameters);
        } else {
            // 如果没有传入 dbProperties，使用默认配置
            dbDriverClass = parameters.get(DB_DRIVER_CLASS);
            dbUrl = parameters.get(DB_URL);
            dbUserName = parameters.get(DB_USERNAME);
            dbPassword = parameters.get(DB_PASSWORD);
        }

        // 初始化KVRocks客户端
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        // 测试连接
        if (!kvrocks.testConnection()) {
            throw new RuntimeException("KVRocks连接失败!");
        }

        // 初始化Caffeine缓存
        appCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();

    }

    private void initializeConnectionPool(Configuration parameters) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUserName);
        config.setPassword(dbPassword);
        config.setDriverClassName(dbDriverClass);

        // 连接池配置
        config.setMaximumPoolSize(parameters.get(HIKARI_MAX_POOL_SIZE));
        config.setMinimumIdle(parameters.get(HIKARI_MIN_IDLE));
        config.setConnectionTimeout(parameters.get(HIKARI_CONNECTION_TIMEOUT));
        config.setIdleTimeout(parameters.get(HIKARI_IDLE_TIMEOUT));
        config.setMaxLifetime(parameters.get(HIKARI_MAX_LIFETIME));

        // 连接测试
        config.setConnectionTestQuery(dbProperties.getProperty("rdbms.testQuery","SELECT 1"));

        // 连接池名称
        config.setPoolName("Flink-MySQL-Pool-" + getRuntimeContext().getIndexOfThisSubtask());

        dataSource = new HikariDataSource(config);
        LOG.info("MySQL连接池初始化成功，最大连接数: {}", config.getMaximumPoolSize());
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        String ak = input.getAppKey();
        Integer plat = Dims.sdk(String.valueOf(input.getData().get("pl")));

        CompanyApp companyApp = appCache.getIfPresent(ak);

        if (companyApp != null) {
            ZGMessage output = createOutput(input, companyApp);
            resultFuture.complete(Collections.singleton(output));
            return;
        }

        String companyAppHashKey = "company_app";
        String appHashKey = "app";
        CompletableFuture<Map<String, String>> appFuture = kvrocks.asyncHGetAll(appHashKey);
        HashMap<String, Integer> appMap = new HashMap<>();

        try {
            Map<String, String> kvAppMap = appFuture.get(5, TimeUnit.SECONDS);
            for (Map.Entry<String, String> entry : kvAppMap.entrySet()) {
                String appStr = entry.getValue();
                try {
                    App app = App.fromJson(appStr);
                    appMap.put(app.getMainId() + "_" + app.getSdkPlatform(), app.getHasData());
                } catch (Exception e) {
                    LOG.warn("解析App JSON失败: {}", appStr, e);
                }
            }
        } catch (Exception e){
            LOG.warn("获取App数据失败: {}", Arrays.toString(e.getStackTrace()));
        }

        kvrocks.asyncHGetAll(companyAppHashKey)
                .thenCompose(map -> {
                    if (map != null) {
                        for (Map.Entry<String, String> entry : map.entrySet()) {
                            String appStr = entry.getValue();
                            try {
                                CompanyApp app = CompanyApp.fromJson(appStr);
                                if (ak.equals(app.getAppKey())) {
                                    appCache.put(ak, app);
                                    ZGMessage output = createOutput(input, app);

                                    // 异步写入MySQL
                                    if(!appMap.containsKey(app.getId() + "_" + plat)){
                                        CompletableFuture.runAsync(() ->
                                                writeToMysql(app.getId(), plat)
                                        );
                                    }
                                    return CompletableFuture.completedFuture(output);
                                }
                            } catch (Exception e) {
                                LOG.warn("解析CompanyApp JSON失败: {}", appStr, e);
                            }
                        }
                    }
                    ZGMessage output = createErrorOutput(input);
                    return CompletableFuture.completedFuture(output);
                })
                .whenComplete((output, throwable) -> {
                    if (throwable != null) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        resultFuture.complete(Collections.singleton(output));
                    }
                });
    }

    private void writeToMysql(Integer appId, Integer platform) {
        Connection conn = null;
        try {
            // 从连接池获取连接
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
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    LOG.error("回滚失败", ex);
                }
            }
        } finally {
            // 归还连接到连接池
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    LOG.error("关闭连接失败", e);
                }
            }
        }
    }

    private ZGMessage createOutput(ZGMessage input, CompanyApp companyApp) {
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

        if (companyApp != null) {
            output.setAppId(companyApp.getId());
        } else {
            output.setAppId(0);
        }

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
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (appCache != null) {
            System.out.printf(
                    "[SetAppIdAndBusinessOperator-%d] 缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    appCache.stats()
            );
        }

        // 关闭MySQL连接
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("MySQL连接池已关闭");
        }
    }
}