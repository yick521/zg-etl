package com.zhugeio.etl.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * 配置管理类
 *
 * 配置优先级: 系统属性 > 环境变量 > 配置文件
 */
public class Config {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    // ============ 原有常量 (保持不变) ============
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_GATE_SOURCE_TOPIC = "kafka.gate.sourceTopic";
    public static final String KAFKA_DW_SOURCE_TOPIC = "kafka.dw.sourceTopic";
    public static final String KAFKA_ID_SOURCE_TOPIC = "kafka.id.sourceTopic";
    public static final String KAFKA_DW_GROUP_ID = "kafka.dw.group.id";
    public static final String KAFKA_ID_GROUP_ID = "kafka.id.group.id";
    public static final String KAFKA_GATE_GROUP_ID = "kafka.gate.group.id";
    public static final String KAFKA_MAX_PARTITION_FETCH_BYTES = "kafka.max.partition.fetch.bytes";
    public static final String KAFKA_MAX_POLL_RECORDS = "kafka.max.poll.records";
    public static final String KAFKA_FETCH_MAX_WAIT_MS = "kafka.fetch.max.wait.ms";
    public static final String KAFKA_REQUEST_TIMEOUT_MS = "request.timeout.ms";
    public static final String KAFAK_SESSION_TIMEOUT_MS = "session.timeout.ms";
    public static final String KAFKA_DATA_QUALITY_TOPIC = "kafka.quality.topic";
    public static final String ENCRYPTION_SECRET_PATH = "encryption_secret_path";
    public static final String ROOT_SECRET_PATH = "root_secret_path";
    public static final String WORKER_SECRET_PATH = "worker_secret_path";
    public static final String PRIVATE_KEY_PATH = "private_key_path";
    public static final String SM2_PRIKEY = "sm2_priKey";

    public static final String GATE_SINK_TO_DOWNSTREAM = "gate.sink.to.downstream";
    public static final String ID_SINK_TO_DOONSTREAM = "id.sink.to.downstream";

    // ============ DwJob 新增常量 ============

    // Flink
    public static final String CHECKPOINT_DW_PATH = "checkpoint.dw.path";
    public static final String CHECKPOINT_ID_PATH = "checkpoint.id.path";

    // KVRocks
    public static final String KVROCKS_HOST = "kvrocks.host";
    public static final String KVROCKS_PORT = "kvrocks.port";
    public static final String KVROCKS_CLUSTER = "kvrocks.cluster";
    public static final String KVROCKS_LOCAL_CACHE_SIZE = "kvrocks.local.cache.size";
    public static final String KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES = "kvrocks.local.cache.expire.minutes";

    // Redis (百度关键词缓存)
    public static final String REDIS_HOST = "redis.host";
    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_PASSWORD = "redis.password";
    public static final String REDIS_CLUSTER = "redis.cluster";

    // Doris
    public static final String DORIS_HOST = "doris.host";
    public static final String DORIS_HTTP_PORT = "doris.http.port";
    public static final String DORIS_DB = "doris.db";
    public static final String DORIS_USER = "doris.user";
    public static final String DORIS_PASSWORD = "doris.password";
    public static final String DORIS_FE_NODES = "doris.fe.nodes";
    public static final String DORIS_USERNAME = "doris.username";
    public static final String DORIS_DATABASE = "doris.database";

    // ============ Doris Sink 性能配置 ============
    public static final String DORIS_SINK_BATCH_SIZE = "doris.sink.batch.size";
    public static final String DORIS_SINK_BATCH_INTERVAL_MS = "doris.sink.batch.interval.ms";
    public static final String DORIS_SINK_MAX_RETRIES = "doris.sink.max.retries";
    public static final String DORIS_SINK_LABEL_PREFIX = "doris.sink.label.prefix";
    public static final String DORIS_SINK_ENABLE_2PC = "doris.sink.enable.2pc";
    public static final String DORIS_SINK_BUFFER_SIZE = "doris.sink.buffer.size";
    public static final String DORIS_SINK_BUFFER_COUNT = "doris.sink.buffer.count";

    // ============ RDBMS (MySQL) 配置 ============
    public static final String RDBMS_URL = "rdbms.url";
    public static final String RDBMS_USERNAME = "rdbms.username";
    public static final String RDBMS_PASSWORD = "rdbms.password";
    public static final String RDBMS_DRIVER = "rdbms.driver";
    public static final String RDBMS_MAX_POOL_SIZE = "rdbms.max.pool.size";
    public static final String RDBMS_MIN_IDLE = "rdbms.min.idle";
    public static final String RDBMS_CONNECTION_TIMEOUT = "rdbms.connection.timeout";

    // IP 解析
    public static final String IP_FILE_DIR = "ip.file.dir";                    // IPv4 目录
    public static final String IPV6_FILE_DIR = "ipv6.file.dir";                // IPv6 目录 (独立!)
    public static final String IPV6_LOAD = "ipv6.load";                        // 是否加载 IPv6
    public static final String RELOAD_IP_FILE = "reload.ip.file";              // IPv4 热加载
    public static final String RELOAD_IPV6_FILE = "reload.ipv6.file";          // IPv6 热加载
    public static final String RELOAD_RATE_SECOND = "reload.rate.second";

    // HDFS 配置
    public static final String FLAG_HA = "flag.ha";                            // 是否 HA 模式
    public static final String FS_DEFAULT_FS = "fs.defaultFS";                 // HDFS 地址
    public static final String DFS_NAMESERVICES = "dfs.nameservices";          // HA nameservices
    public static final String DFS_HA_NAMESPACE = "dfs.ha.namenodes.namespace";// HA namenodes
    public static final String DFS_NAMENODE_RPC_Z1 = "dfs.namenode.rpc-address.namespace.z1";
    public static final String DFS_NAMENODE_RPC_Z2 = "dfs.namenode.rpc-address.namespace.z2";

    // 百度 API
    public static final String BAIDU_URL = "baidu.url";
    public static final String BAIDU_ID = "baidu_id";
    public static final String BAIDU_KEY = "baidu_key";

    // HTTP
    public static final String REQUEST_SOCKET_TIMEOUT = "requestSocketTimeout";
    public static final String REQUEST_CONNECT_TIMEOUT = "requestConnectTimeout";
    public static final String MAX_RETRY_NUM = "maxRetryNum";
    public static final String BATCH_SIZE = "batchSize";

    // 时间
    public static final String TIME_EXPIRE_SUBDAYS = "subtime";
    public static final String TIME_EXPIRE_ADDDAYS = "addtime";

    // 应用
    public static final String BLACK_APPIDS = "blackAppIds";
    public static final String WHITE_APPID = "white_appid";
    public static final String EVENT_ATTR_LENGTH_LIMIT = "event_attr_length_limit";
    public static final String WRITE_EVENT_ALL_FLAG = "write.event.all.flag";
    public static final String WRITE_EVENT_ATTR_EID_PARTITION = "write.event.attr.eid.partition";
    public static final String MAX_PROP_LENGTH = "maxPropLength";

    // 数据库类型
    public static final String DB_TYPE = "dbtype";  // 1=Kudu, 2=Doris

    // 算子配置
    public static final String OPERATOR_KEYWORD_TIMEOUT_MS = "operator.keyword.timeout.ms";
    public static final String OPERATOR_KEYWORD_CAPACITY = "operator.keyword.capacity";
    public static final String OPERATOR_IP_TIMEOUT_MS = "operator.ip.timeout.ms";
    public static final String OPERATOR_IP_CAPACITY = "operator.ip.capacity";
    public static final String OPERATOR_UA_TIMEOUT_MS = "operator.ua.timeout.ms";
    public static final String OPERATOR_UA_CAPACITY = "operator.ua.capacity";
    public static final String OPERATOR_UA_CACHE_SIZE = "operator.ua.cache.size";
    public static final String OPERATOR_UA_CACHE_EXPIRE_MINUTES = "operator.ua.cache.expire.minutes";
    public static final String OPERATOR_ROUTER_TIMEOUT_MS = "operator.router.timeout.ms";
    public static final String OPERATOR_ROUTER_CAPACITY = "operator.router.capacity";

    public static final String FLINK_CHECKPOINT_INTERVAL_MS = "checkpoint.interval.ms";

    // id映射归档配置
    public static final String ID_ARCHIVE_ENABLED = "id.archive.enabled";
    public static final String ID_ARCHIVE_BROKERS = "kafka.brokers";
    public static final String KAFKA_ID_ARCHIVE_TOPIC = "kafka.id.archive.topic";
    public static final String KAFKA_ID_ARCHIVE_GROUP_ID = "kafka.id.archive.group.id";
    public static final String CHECKPOINT_ID_ARCHIVE_PATH = "checkpoint.id.archive.path";
    public static final String INIT_SQL = "init.sql";

    /**
     * 数据质量服务开关
     */
    public static final String DQ_ENABLED = "dq.enabled";

    /**
     * 数据质量 Kafka Brokers
     */
    public static final String DQ_KAFKA_BROKERS = "kafka.brokers";

    /**
     * 数据质量 Kafka Topic
     */
    public static final String DQ_KAFKA_TOPIC = "kafka.quality.topic";

    private static final Properties properties = new Properties();

    // 静态初始化块 - 支持外部文件加载
    static {
        loadConfig();
    }

    /**
     * 加载配置文件
     * 优先级: 外部文件 > classpath
     */
    private static void loadConfig() {
        // -yt 上传的文件会在当前工作目录
        String[] externalPaths = {
                "config.properties",                     // YARN container 当前目录 (最常见)
                "./config.properties",                   // 当前目录
                "config/config.properties"               // config 子目录 (本地测试)
        };

        LOG.info("========== 配置加载开始 ==========");
        LOG.info("当前工作目录: {}", System.getProperty("user.dir"));

        // 列出当前目录文件，便于调试
        try {
            File currentDir = new File(".");
            File[] files = currentDir.listFiles();
            if (files != null) {
                LOG.info("当前目录文件列表:");
                for (File f : files) {
                    LOG.info("  - {} ({})", f.getName(), f.isDirectory() ? "目录" : "文件");
                }
            }
        } catch (Exception e) {
            LOG.warn("列出目录失败", e);
        }

        // 尝试从外部文件加载
        boolean loaded = false;
        for (String path : externalPaths) {
            File configFile = new File(path);
            LOG.info("尝试加载: {} (绝对路径: {}, 存在: {})",
                    path, configFile.getAbsolutePath(), configFile.exists());

            if (configFile.exists() && configFile.isFile()) {
                try (InputStream input = new FileInputStream(configFile)) {
                    properties.load(input);
                    LOG.info("✓ 成功从外部文件加载配置: {}", configFile.getAbsolutePath());
                    loaded = true;
                    break;
                } catch (IOException e) {
                    LOG.warn("加载外部配置失败: {}", path, e);
                }
            }
        }

        // 回退到 classpath
        if (!loaded) {
            LOG.info("外部文件未找到，尝试从 classpath 加载...");
            try (InputStream input = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
                if (input != null) {
                    properties.load(input);
                    LOG.info("✓ 从 classpath 加载配置");
                    loaded = true;
                } else {
                    LOG.error("✗ classpath 中也找不到 config.properties");
                }
            } catch (IOException e) {
                LOG.error("加载 classpath 配置失败", e);
            }
        }

        // 打印关键配置，便于验证
        LOG.info("========== 关键配置检查 ==========");
        LOG.info("kafka.brokers = {}", properties.getProperty(KAFKA_BROKERS));
        LOG.info("kafka.dw.sourceTopic = {}", properties.getProperty(KAFKA_DW_SOURCE_TOPIC));
        LOG.info("kafka.dw.group.id = {}", properties.getProperty(KAFKA_DW_GROUP_ID));
        LOG.info("========== 配置加载完成 ==========");
    }

    // ============ 原有方法 (保持不变) ============

    public static String getProp(String key) {
        return properties.getProperty(key);
    }

    public static String getProp(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static int getInt(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new RuntimeException("配置项 " + key + " 不存在");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("配置项 " + key + " 不是有效的整数: " + value, e);
        }
    }

    public static String readResource(String filename) {
        try (BufferedReader ir = new BufferedReader(new InputStreamReader(
                Config.class.getClassLoader().getResourceAsStream(filename)))) {
            return ir.readLine();
        } catch (IOException e) {
            throw new RuntimeException("读取资源文件失败: " + filename, e);
        }
    }

    public static String readFile(String filename) {
        try (BufferedReader ir = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            return ir.readLine();
        } catch (IOException e) {
            throw new RuntimeException("读取文件失败: " + filename, e);
        }
    }

    // ============ DwJob 新增方法 (带默认值版本) ============

    /**
     * 获取字符串配置 (支持默认值)
     * 优先级: 系统属性 > 环境变量 > 配置文件 > 默认值
     */
    public static String getString(String key, String defaultValue) {
        // 1. 系统属性
        String value = System.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }

        // 2. 环境变量 (将 . 替换为 _)
        String envKey = key.replace('.', '_').toUpperCase();
        value = System.getenv(envKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }

        // 3. 配置文件
        value = properties.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }

        // 4. 默认值
        return defaultValue;
    }

    /**
     * 获取字符串配置 (无默认值)
     */
    public static String getString(String key) {
        return getString(key, null);
    }

    /**
     * 获取整数配置 (支持默认值)
     */
    public static int getInt(String key, int defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value for {}: {}", key, value);
            }
        }
        return defaultValue;
    }

    /**
     * 获取长整数配置
     */
    public static long getLong(String key, long defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException e) {
                LOG.warn("Invalid long value for {}: {}", key, value);
            }
        }
        return defaultValue;
    }

    /**
     * 获取布尔配置
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = getString(key, null);
        if (value != null) {
            return "true".equalsIgnoreCase(value.trim()) || "1".equals(value.trim());
        }
        return defaultValue;
    }

    /**
     * 获取字符串列表
     */
    public static String[] getStringArray(String key, String delimiter) {
        String value = getString(key, "");
        if (value.isEmpty()) {
            return new String[0];
        }
        return value.split(delimiter);
    }

    /**
     * 是否是 Doris 模式
     */
    public static boolean isDorisMode() {
        return getInt(DB_TYPE, 1) == 2;
    }

    /**
     * 是否是 Kudu 模式
     */
    public static boolean isKuduMode() {
        return getInt(DB_TYPE, 1) == 1;
    }

    /**
     * 获取所有配置
     */
    public static Properties getAllProperties() {
        Properties all = new Properties();
        all.putAll(properties);
        return all;
    }

    // ============ Kafka 配置 ============

    /**
     * 获取 Kafka Consumer 默认配置
     */
    public static Properties getKafkaConsumerProps() {
        Properties props = new Properties();
        props.setProperty("max.partition.fetch.bytes", getString(KAFKA_MAX_PARTITION_FETCH_BYTES, "52428800"));
        props.setProperty("max.poll.records", getString(KAFKA_MAX_POLL_RECORDS, "1000000"));
        props.setProperty("fetch.max.wait.ms", getString(KAFKA_FETCH_MAX_WAIT_MS, "1000"));
        props.setProperty("request.timeout.ms", getString(KAFKA_REQUEST_TIMEOUT_MS, "120000"));
        props.setProperty("session.timeout.ms", getString(KAFAK_SESSION_TIMEOUT_MS, "60000"));
        return props;
    }

    // ============ RDBMS (MySQL) 配置 ============

    /**
     * 获取 RDBMS (MySQL) 连接配置
     * 用于 EventAsyncOperator、EventAttrAsyncOperator、UserPropAsyncOperator 等
     *
     * 配置项:
     * - rdbms.url: JDBC URL
     * - rdbms.username: 用户名
     * - rdbms.password: 密码
     * - rdbms.driver: 驱动类 (默认 com.mysql.cj.jdbc.Driver)
     * - rdbms.max.pool.size: 最大连接数 (默认 10)
     * - rdbms.min.idle: 最小空闲连接 (默认 2)
     * - rdbms.connection.timeout: 连接超时毫秒 (默认 30000)
     */
    public static Properties getRdbmsProperties() {
        Properties props = new Properties();

        String url = getString(RDBMS_URL, null);
        if (url == null || url.isEmpty()) {
            LOG.warn("RDBMS URL 未配置，返回空配置");
            return props;
        }

        props.setProperty("rdbms.url", url);
        props.setProperty("rdbms.username", getString(RDBMS_USERNAME, "root"));
        props.setProperty("rdbms.password", getString(RDBMS_PASSWORD, ""));
        props.setProperty("rdbms.driver", getString(RDBMS_DRIVER, "com.mysql.cj.jdbc.Driver"));
        props.setProperty("rdbms.max.pool.size", getString(RDBMS_MAX_POOL_SIZE, "10"));
        props.setProperty("rdbms.min.idle", getString(RDBMS_MIN_IDLE, "2"));
        props.setProperty("rdbms.connection.timeout", getString(RDBMS_CONNECTION_TIMEOUT, "30000"));

        // 也复制 maxPropLength 配置
        props.setProperty("maxPropLength", getString(MAX_PROP_LENGTH, "100"));

        return props;
    }

    /**
     * 检查 RDBMS 配置是否可用
     */
    public static boolean isRdbmsConfigured() {
        String url = getString(RDBMS_URL, null);
        return url != null && !url.isEmpty();
    }

    /**
     * 获取最大属性长度限制
     */
    public static int getMaxPropLength() {
        return getInt(MAX_PROP_LENGTH, 100);
    }
}