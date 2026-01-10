package com.zhugeio.etl.common.sink;

import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import com.zhugeio.etl.common.config.Config;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * 动态分表 Doris Sink 构建器
 *
 * 支持:
 * 1. 动态分表 (按 appId)
 * 2. 独立并行度设置 (避免反压)
 * 3. rebalance 数据打散 (避免倾斜)
 * 4. Group Commit (Doris 2.1+)
 * 5. 部分列更新
 */
public class DynamicDorisSinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDorisSinkBuilder.class);

    // ========== Buffer 默认值 ==========
    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;   // 8MB
    private static final int DEFAULT_BUFFER_COUNT = 2;
    private static final long DEFAULT_FLUSH_INTERVAL = 5000L;          // 5s

    // ========== 并行度默认值 ==========
    private static final int DEFAULT_SINK_PARALLELISM = -1;            // -1 表示使用环境默认并行度

    // 连接参数
    private String feNodes;
    private String database;
    private String tablePrefix;
    private String username = "root";
    private String password = "";

    // appId 提取器
    private SerializableFunction<T, Integer> appIdExtractor;

    // 批量参数
    private String labelPrefix;
    private int bufferSize;
    private int bufferCount;
    private long bufferFlushIntervalMs;
    private int maxRetries;
    private boolean enable2PC;

    // Stream Load 参数
    private String format = "json";
    private boolean partialUpdate = false;
    private boolean stripOuterArray = false;

    // Group Commit 模式
    private boolean enableGroupCommit = false;
    private String groupCommitMode = "async_mode";

    // 列名列表
    private String columns;

    // 回调
    private CommitSuccessCallback callback;

    // ========== 【新增】并行度和数据分布 ==========
    private int parallelism = DEFAULT_SINK_PARALLELISM;
    private boolean rebalance = false;      // 是否 rebalance 打散数据

    private DynamicDorisSinkBuilder() {
        this.labelPrefix = Config.getString(Config.DORIS_SINK_LABEL_PREFIX, "flink_doris");
        this.bufferSize = Config.getInt(Config.DORIS_SINK_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
        this.bufferCount = Config.getInt(Config.DORIS_SINK_BUFFER_COUNT, DEFAULT_BUFFER_COUNT);
        this.bufferFlushIntervalMs = Config.getLong(Config.DORIS_SINK_BATCH_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL);
        this.maxRetries = Config.getInt(Config.DORIS_SINK_MAX_RETRIES, 3);
        this.enable2PC = Config.getBoolean(Config.DORIS_SINK_ENABLE_2PC, false);
    }

    public static <T> DynamicDorisSinkBuilder<T> builder() {
        return new DynamicDorisSinkBuilder<>();
    }

    // ========== 连接参数 ==========

    public DynamicDorisSinkBuilder<T> feNodes(String feNodes) {
        this.feNodes = feNodes;
        return this;
    }

    public DynamicDorisSinkBuilder<T> database(String database) {
        this.database = database;
        return this;
    }

    public DynamicDorisSinkBuilder<T> tablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public DynamicDorisSinkBuilder<T> username(String username) {
        this.username = username;
        return this;
    }

    public DynamicDorisSinkBuilder<T> password(String password) {
        this.password = password;
        return this;
    }

    public DynamicDorisSinkBuilder<T> appIdExtractor(SerializableFunction<T, Integer> appIdExtractor) {
        this.appIdExtractor = appIdExtractor;
        return this;
    }

    // ========== Buffer 参数 ==========

    public DynamicDorisSinkBuilder<T> bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public DynamicDorisSinkBuilder<T> bufferSizeMB(int mb) {
        this.bufferSize = mb * 1024 * 1024;
        return this;
    }

    public DynamicDorisSinkBuilder<T> bufferCount(int bufferCount) {
        this.bufferCount = bufferCount;
        return this;
    }

    public DynamicDorisSinkBuilder<T> labelPrefix(String labelPrefix) {
        this.labelPrefix = labelPrefix;
        return this;
    }

    public DynamicDorisSinkBuilder<T> bufferFlushIntervalMs(long bufferFlushIntervalMs) {
        this.bufferFlushIntervalMs = bufferFlushIntervalMs;
        return this;
    }

    public DynamicDorisSinkBuilder<T> maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public DynamicDorisSinkBuilder<T> enable2PC(boolean enable) {
        this.enable2PC = enable;
        return this;
    }

    // ========== Stream Load 参数 ==========

    public DynamicDorisSinkBuilder<T> partialUpdate() {
        this.partialUpdate = true;
        return this;
    }

    public DynamicDorisSinkBuilder<T> partialUpdate(boolean enable) {
        this.partialUpdate = enable;
        return this;
    }

    public DynamicDorisSinkBuilder<T> columns(String columns) {
        this.columns = columns;
        return this;
    }

    public DynamicDorisSinkBuilder<T> enableGroupCommit() {
        this.enableGroupCommit = true;
        return this;
    }

    public DynamicDorisSinkBuilder<T> enableGroupCommit(String mode) {
        this.enableGroupCommit = true;
        this.groupCommitMode = mode;
        return this;
    }

    // ========== 【新增】并行度和数据分布 ==========

    /**
     * 设置 Sink 并行度
     * @param parallelism 并行度，-1 表示使用环境默认
     */
    public DynamicDorisSinkBuilder<T> parallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    /**
     * 启用 rebalance，将数据轮询分发到所有并行实例，避免数据倾斜
     */
    public DynamicDorisSinkBuilder<T> rebalance() {
        this.rebalance = true;
        return this;
    }

    /**
     * 启用 rebalance 并设置并行度
     */
    public DynamicDorisSinkBuilder<T> rebalance(int parallelism) {
        this.rebalance = true;
        this.parallelism = parallelism;
        return this;
    }

    // ========== 回调 ==========

    public DynamicDorisSinkBuilder<T> onSuccess(CommitSuccessCallback callback) {
        this.callback = callback;
        return this;
    }

    // ========== 构建 ==========

    public DorisSink<T> build() {
        validate();

        LOG.info("构建动态分表 Doris Sink: {}.{}_*, bufferSize={}MB, bufferCount={}, flushInterval={}ms, " +
                        "partialUpdate={}, enable2PC={}, groupCommit={}, parallelism={}, rebalance={}",
                database, tablePrefix,
                bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs,
                partialUpdate, enable2PC, enableGroupCommit,
                parallelism > 0 ? parallelism : "default", rebalance);

        DynamicTableSerializer<T> serializer = new DynamicTableSerializer<>(
                database, tablePrefix, appIdExtractor);

        return DorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .build();
    }

    public CallbackDorisSink<T> buildWithCallback() {
        validate();

        if (callback == null) {
            throw new IllegalArgumentException("callback is required for buildWithCallback()");
        }

        LOG.info("构建带回调的动态分表 Doris Sink: {}.{}_*, bufferSize={}MB, bufferCount={}, flushInterval={}ms, " +
                        "parallelism={}, rebalance={}",
                database, tablePrefix,
                bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs,
                parallelism > 0 ? parallelism : "default", rebalance);

        DynamicTableSerializer<T> serializer = new DynamicTableSerializer<>(
                database, tablePrefix, appIdExtractor);

        return CallbackDorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .setTableName(database + "." + tablePrefix + "_*")
                .setCommitCallback(callback)
                .build();
    }

    public void addTo(DataStream<T> stream) {
        addTo(stream, "DorisSink-" + tablePrefix, "doris-sink-" + tablePrefix.replace("_", "-"));
    }

    /**
     * 【改造】添加 Sink 到 DataStream，支持 rebalance 和独立并行度
     */
    public void addTo(DataStream<T> stream, String name, String uid) {
        // 是否需要 rebalance
        DataStream<T> finalStream = rebalance ? stream.rebalance() : stream;

        if (callback != null) {
            DataStreamSink<T> sinkOp = finalStream.sinkTo(buildWithCallback()).name(name).uid(uid);
            if (parallelism > 0) {
                sinkOp.setParallelism(parallelism);
            }
        } else {
            DataStreamSink<T> sinkOp = finalStream.sinkTo(build()).name(name).uid(uid);
            if (parallelism > 0) {
                sinkOp.setParallelism(parallelism);
            }
        }

        LOG.info("  ✓ {} Sink 已添加 (bufferSize={}MB, bufferCount={}, flushInterval={}ms, parallelism={}, rebalance={})",
                tablePrefix, bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs,
                parallelism > 0 ? parallelism : "default", rebalance);
    }

    // ========== 内部方法 ==========

    private void validate() {
        if (feNodes == null || feNodes.isEmpty()) {
            throw new IllegalArgumentException("feNodes is required");
        }
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("database is required");
        }
        if (tablePrefix == null || tablePrefix.isEmpty()) {
            throw new IllegalArgumentException("tablePrefix is required");
        }
        if (appIdExtractor == null) {
            throw new IllegalArgumentException("appIdExtractor is required");
        }
        if (partialUpdate && (columns == null || columns.isEmpty())) {
            throw new IllegalArgumentException("columns is required for partial update mode");
        }
    }

    private DorisOptions buildDorisOptions() {
        return DorisOptions.builder()
                .setFenodes(feNodes)
                .setTableIdentifier(database + "." + tablePrefix + "_0")
                .setUsername(username)
                .setPassword(password)
                .build();
    }

    private DorisExecutionOptions buildExecutionOptions() {
        Properties props = new Properties();
        props.setProperty("format", format);
        props.setProperty("strip_outer_array", "false");

        if (partialUpdate) {
            props.setProperty("partial_columns", "true");
            if (columns != null && !columns.isEmpty()) {
                props.setProperty("columns", columns);
            }
        }

        if (enableGroupCommit) {
            props.setProperty("group_commit", groupCommitMode);
            LOG.info("启用 Group Commit 模式: {}", groupCommitMode);
        }

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferSize(bufferSize)
                .setBufferCount(bufferCount)
                .setBufferFlushIntervalMs(bufferFlushIntervalMs)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props);

        if (enable2PC) {
            builder.enable2PC();
        } else {
            builder.disable2PC();
        }

        return builder.build();
    }

    // ========== 静态工厂方法 (支持并行度) ==========

    /**
     * 添加 UserRow 动态分表 Sink
     */
    public static void addUserSink(DataStream<UserRow> stream,
                                   String feNodes, String database,
                                   String username, String password,
                                   CommitSuccessCallback callback) {
        addUserSink(stream, feNodes, database, username, password, callback, -1);
    }

    /**
     * 添加 UserRow 动态分表 Sink (指定并行度)
     */
    public static void addUserSink(DataStream<UserRow> stream,
                                   String feNodes, String database,
                                   String username, String password,
                                   CommitSuccessCallback callback,
                                   int parallelism) {
        DynamicDorisSinkBuilder.<UserRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user")
                .appIdExtractor(UserRow::getAppId)
                .username(username)
                .password(password)
                .partialUpdate()
                .columns("device_id,zg_id,user_id,begin_date,platform")
                .rebalance(parallelism)
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 DeviceRow 动态分表 Sink
     */
    public static void addDeviceSink(DataStream<DeviceRow> stream,
                                     String feNodes, String database,
                                     String username, String password,
                                     CommitSuccessCallback callback) {
        addDeviceSink(stream, feNodes, database, username, password, callback, -1);
    }

    /**
     * 添加 DeviceRow 动态分表 Sink (指定并行度)
     */
    public static void addDeviceSink(DataStream<DeviceRow> stream,
                                     String feNodes, String database,
                                     String username, String password,
                                     CommitSuccessCallback callback,
                                     int parallelism) {
        DynamicDorisSinkBuilder.<DeviceRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_device")
                .appIdExtractor(DeviceRow::getAppId)
                .username(username)
                .password(password)
                .partialUpdate()
                .columns("device_id,device_md5,platform,device_type,l,h,device_brand,device_model," +
                        "resolution,phone,imei,mac,is_prison_break,is_crack,language,timezone," +
                        "attr1,attr2,attr3,attr4,attr5,last_update_date")
                .rebalance(parallelism)
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 UserPropertyRow 动态分表 Sink
     */
    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                           String feNodes, String database,
                                           String username, String password,
                                           CommitSuccessCallback callback) {
        addUserPropertySink(stream, feNodes, database, username, password, callback, -1);
    }

    /**
     * 添加 UserPropertyRow 动态分表 Sink (指定并行度)
     */
    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                           String feNodes, String database,
                                           String username, String password,
                                           CommitSuccessCallback callback,
                                           int parallelism) {
        DynamicDorisSinkBuilder.<UserPropertyRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user_property")
                .appIdExtractor(UserPropertyRow::getAppId)
                .username(username)
                .password(password)
                .partialUpdate()
                .columns("zg_id,property_id,user_id,property_name,property_data_type,property_value,platform,last_update_date")
                .rebalance(parallelism)
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 EventAttrRow 动态分表 Sink
     */
    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback) {
        addEventAttrSink(stream, feNodes, database, username, password, callback, -1);
    }

    /**
     * 添加 EventAttrRow 动态分表 Sink (指定并行度)
     * 数据量最大，支持独立配置更大的 buffer 和更高的并行度
     */
    /**
     * 添加 EventAttrRow 动态分表 Sink (指定并行度)
     */
    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback,
                                        int parallelism) {

        LOG.info("构建 EventAttrRow 动态分表 Doris Sink: {}.b_user_event_attr_*, parallelism={}",
                database, parallelism > 0 ? parallelism : "default");

        EventAttrRowSerializer serializer = new EventAttrRowSerializer(database, "b_user_event_attr");

        int bufferSize = Config.getInt("doris.sink.event-attr.buffer.size",
                Config.getInt(Config.DORIS_SINK_BUFFER_SIZE, 8 * 1024 * 1024));
        int bufferCount = Config.getInt("doris.sink.event-attr.buffer.count",
                Config.getInt(Config.DORIS_SINK_BUFFER_COUNT, 3));
        long bufferFlushIntervalMs = Config.getLong("doris.sink.event-attr.flush.interval",
                Config.getLong(Config.DORIS_SINK_BATCH_INTERVAL_MS, 5000L));
        boolean enableRebalance = Config.getBoolean("doris.sink.event-attr.rebalance", true);

        String labelPrefix = Config.getString(Config.DORIS_SINK_LABEL_PREFIX, "flink_doris");
        int maxRetries = Config.getInt(Config.DORIS_SINK_MAX_RETRIES, 3);
        boolean enable2PC = Config.getBoolean(Config.DORIS_SINK_ENABLE_2PC, false);

        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(feNodes)
                .setTableIdentifier(database + ".b_user_event_attr_0")
                .setUsername(username)
                .setPassword(password)
                .build();

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("strip_outer_array", "false");

        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferSize(bufferSize)
                .setBufferCount(bufferCount)
                .setBufferFlushIntervalMs(bufferFlushIntervalMs)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props);

        if (enable2PC) {
            execBuilder.enable2PC();
        } else {
            execBuilder.disable2PC();
        }

        DorisExecutionOptions execOptions = execBuilder.build();

        // 是否 rebalance
        DataStream<EventAttrRow> finalStream = enableRebalance ? stream.rebalance() : stream;

        if (callback != null) {
            CallbackDorisSink<EventAttrRow> sink = CallbackDorisSink.<EventAttrRow>builder()
                    .setDorisOptions(dorisOptions)
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(execOptions)
                    .setSerializer(serializer)
                    .setTableName(database + ".b_user_event_attr_*")
                    .setCommitCallback(callback)
                    .build();

            DataStreamSink<EventAttrRow> sinkOp = finalStream.sinkTo(sink)
                    .name("DorisSink-b_user_event_attr")
                    .uid("doris-sink-b-user-event-attr");

            if (parallelism > 0) {
                sinkOp.setParallelism(parallelism);
            }
        } else {
            DorisSink<EventAttrRow> sink = DorisSink.<EventAttrRow>builder()
                    .setDorisOptions(dorisOptions)
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(execOptions)
                    .setSerializer(serializer)
                    .build();

            DataStreamSink<EventAttrRow> sinkOp = finalStream.sinkTo(sink)
                    .name("DorisSink-b_user_event_attr")
                    .uid("doris-sink-b-user-event-attr");

            if (parallelism > 0) {
                sinkOp.setParallelism(parallelism);
            }
        }

        LOG.info("  ✓ b_user_event_attr Sink 已添加 (bufferSize={}MB, bufferCount={}, flushInterval={}ms, 2PC={}, parallelism={}, rebalance={})",
                bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs, enable2PC,
                parallelism > 0 ? parallelism : "default", enableRebalance);
    }
}