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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * 动态分表 Doris Sink 构建器
 *
 */
public class DynamicDorisSinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDorisSinkBuilder.class);

    // ========== 【优化】Buffer 默认值调大 ==========
    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;  // 10MB（原 5MB）
    private static final int DEFAULT_BUFFER_COUNT = 2;                 // 3（原 2）
    private static final long DEFAULT_FLUSH_INTERVAL = 5000L;          // 5s（原 3s）

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
    private String groupCommitMode = "async_mode";  // sync_mode 或 async_mode

    // 列名列表
    private String columns;

    // 回调
    private CommitSuccessCallback callback;

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

    // 】启用 Group Commit（Doris 2.1+）
    public DynamicDorisSinkBuilder<T> enableGroupCommit() {
        this.enableGroupCommit = true;
        return this;
    }

    public DynamicDorisSinkBuilder<T> enableGroupCommit(String mode) {
        this.enableGroupCommit = true;
        this.groupCommitMode = mode;
        return this;
    }

    // ========== 模式预设 ==========



    // ========== 回调 ==========

    public DynamicDorisSinkBuilder<T> onSuccess(CommitSuccessCallback callback) {
        this.callback = callback;
        return this;
    }

    // ========== 构建 ==========

    public DorisSink<T> build() {
        validate();

        LOG.info("构建动态分表 Doris Sink: {}.{}_*, bufferSize={}MB, bufferCount={}, flushInterval={}ms, " +
                        "partialUpdate={}, enable2PC={}, groupCommit={}",
                database, tablePrefix,
                bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs,
                partialUpdate, enable2PC, enableGroupCommit);

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

        LOG.info("构建带回调的动态分表 Doris Sink: {}.{}_*, bufferSize={}MB, bufferCount={}, flushInterval={}ms",
                database, tablePrefix,
                bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs);

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

    public void addTo(DataStream<T> stream, String name, String uid) {
        if (callback != null) {
            stream.sinkTo(buildWithCallback()).name(name).uid(uid);
        } else {
            stream.sinkTo(build()).name(name).uid(uid);
        }
        LOG.info("  ✓ {} Sink 已添加 (bufferSize={}MB, bufferCount={}, flushInterval={}ms)",
                tablePrefix, bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs);
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

        // 【新增】Group Commit 支持（Doris 2.1+）
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

    /**
     * 添加 UserRow 动态分表 Sink
     */
    public static void addUserSink(DataStream<UserRow> stream,
                                   String feNodes, String database,
                                   String username, String password,
                                   CommitSuccessCallback callback) {
        DynamicDorisSinkBuilder.<UserRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user")
                .appIdExtractor(UserRow::getAppId)
                .username(username)
                .password(password)
//                .balanced()
//                .enableGroupCommit()  // 【开启】Group Commit
                .partialUpdate()
                .columns("device_id,zg_id,user_id,begin_date,platform")
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
        DynamicDorisSinkBuilder.<DeviceRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_device")
                .appIdExtractor(DeviceRow::getAppId)
                .username(username)
                .password(password)
//                .balanced()
//                .enableGroupCommit()  // 【开启】Group Commit
                .partialUpdate()
                .columns("device_id,device_md5,platform,device_type,l,h,device_brand,device_model," +
                        "resolution,phone,imei,mac,is_prison_break,is_crack,language,timezone," +
                        "attr1,attr2,attr3,attr4,attr5,last_update_date")
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 UserPropertyRow 动态分表 Sink
     * 【注意】一条事件可能产生多条用户属性，数据量会膨胀，使用高吞吐模式
     */
    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                           String feNodes, String database,
                                           String username, String password,
                                           CommitSuccessCallback callback) {
        DynamicDorisSinkBuilder.<UserPropertyRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user_property")
                .appIdExtractor(UserPropertyRow::getAppId)
                .username(username)
                .password(password)
//                .highThroughput()
//                .enableGroupCommit()  // 【开启】Group Commit
                .partialUpdate()
                .columns("zg_id,property_id,user_id,property_name,property_data_type,property_value,platform,last_update_date")
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 EventAttrRow 动态分表 Sink（数据量最大，使用高吞吐模式）
     */
    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback) {

        LOG.info("构建 EventAttrRow 动态分表 Doris Sink: {}.b_user_event_attr_*", database);

        EventAttrRowSerializer serializer = new EventAttrRowSerializer(database, "b_user_event_attr");

        // 【优化】EventAttr 数据量大，使用更大的 buffer
        int bufferSize = Config.getInt(Config.DORIS_SINK_BUFFER_SIZE, 8 * 1024 * 1024);  // 20MB
        int bufferCount = Config.getInt(Config.DORIS_SINK_BUFFER_COUNT, 3);
        long bufferFlushIntervalMs = Config.getLong(Config.DORIS_SINK_BATCH_INTERVAL_MS, 5000L);

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
        // 【开启】Group Commit（Doris 2.1+）
//        props.setProperty("group_commit", "async_mode");

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

        if (callback != null) {
            CallbackDorisSink<EventAttrRow> sink = CallbackDorisSink.<EventAttrRow>builder()
                    .setDorisOptions(dorisOptions)
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(execOptions)
                    .setSerializer(serializer)
                    .setTableName(database + ".b_user_event_attr_*")
                    .setCommitCallback(callback)
                    .build();

            stream.sinkTo(sink)
                    .name("DorisSink-b_user_event_attr")
                    .uid("doris-sink-b-user-event-attr");
        } else {
            DorisSink<EventAttrRow> sink = DorisSink.<EventAttrRow>builder()
                    .setDorisOptions(dorisOptions)
                    .setDorisReadOptions(DorisReadOptions.builder().build())
                    .setDorisExecutionOptions(execOptions)
                    .setSerializer(serializer)
                    .build();

            stream.sinkTo(sink)
                    .name("DorisSink-b_user_event_attr")
                    .uid("doris-sink-b-user-event-attr");
        }

        LOG.info("  ✓ b_user_event_attr Sink 已添加 (bufferSize={}MB, bufferCount={}, flushInterval={}ms, 2PC={})",
                bufferSize / 1024 / 1024, bufferCount, bufferFlushIntervalMs, enable2PC);
    }
}
