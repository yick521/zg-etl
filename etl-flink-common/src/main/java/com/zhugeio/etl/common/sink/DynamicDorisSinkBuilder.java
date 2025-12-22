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
 * 根据 appId 自动路由到对应的表 (如 b_user_123, b_device_456)
 *
 * 使用示例:
 * <pre>
 * // 方式1: 使用预定义的快捷方法
 * DynamicDorisSinkBuilder.addUserSink(userStream, feNodes, database, username, password);
 * DynamicDorisSinkBuilder.addDeviceSink(deviceStream, feNodes, database, username, password);
 * DynamicDorisSinkBuilder.addUserPropertySink(propStream, feNodes, database, username, password);
 * DynamicDorisSinkBuilder.addEventAttrSink(eventStream, feNodes, database, username, password);
 *
 * // 方式2: 自定义构建
 * DynamicDorisSinkBuilder.&lt;MyRow&gt;builder()
 *     .feNodes("doris-fe:8030")
 *     .database("dwd")
 *     .tablePrefix("my_table")
 *     .appIdExtractor(MyRow::getAppId)
 *     .username("root")
 *     .password("")
 *     .partialUpdate()
 *     .addTo(stream);
 * </pre>
 */
public class DynamicDorisSinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DynamicDorisSinkBuilder.class);

    // 连接参数
    private String feNodes;
    private String database;
    private String tablePrefix;
    private String username = "root";
    private String password = "";

    // appId 提取器 - 使用可序列化的函数接口
    private SerializableFunction<T, Integer> appIdExtractor;

    // 批量参数 - 从配置读取默认值
    private String labelPrefix;
    private int batchSize;
    private long batchIntervalMs;
    private int maxRetries;
    private boolean enable2PC;

    // Stream Load 参数
    private String format = "json";
    private boolean partialUpdate = false;
    private boolean stripOuterArray = true;
    
    // 列名列表 (用于 partial update)
    private String columns;

    // 回调 (已经是 Serializable)
    private CommitSuccessCallback callback;

    private DynamicDorisSinkBuilder() {
        // 从配置文件读取默认值
        this.labelPrefix = Config.getString(Config.DORIS_SINK_LABEL_PREFIX, "flink_doris");
        this.batchSize = Config.getInt(Config.DORIS_SINK_BATCH_SIZE, 2000);
        this.batchIntervalMs = Config.getLong(Config.DORIS_SINK_BATCH_INTERVAL_MS, 3000);
        this.maxRetries = Config.getInt(Config.DORIS_SINK_MAX_RETRIES, 3);
        // 默认禁用 2PC，避免 "transaction not found" 错误
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

    /**
     * 设置 appId 提取器
     *
     * @param appIdExtractor 可序列化的函数，从记录中提取 appId
     */
    public DynamicDorisSinkBuilder<T> appIdExtractor(SerializableFunction<T, Integer> appIdExtractor) {
        this.appIdExtractor = appIdExtractor;
        return this;
    }

    // ========== 批量参数 ==========

    public DynamicDorisSinkBuilder<T> labelPrefix(String labelPrefix) {
        this.labelPrefix = labelPrefix;
        return this;
    }

    public DynamicDorisSinkBuilder<T> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public DynamicDorisSinkBuilder<T> batchIntervalMs(long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
        return this;
    }

    public DynamicDorisSinkBuilder<T> maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * 启用或禁用两阶段提交 (2PC)
     */
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
    
    /**
     * 设置列名列表 (用于 partial update，必须指定)
     * 
     * @param columns 逗号分隔的列名，如 "device_id,zg_id,user_id,begin_date,platform"
     */
    public DynamicDorisSinkBuilder<T> columns(String columns) {
        this.columns = columns;
        return this;
    }

    // ========== 模式预设 ==========

    /**
     * 高吞吐模式 - 较大批次，较长间隔
     */
    public DynamicDorisSinkBuilder<T> highThroughput() {
        this.batchSize = 50000;
        this.batchIntervalMs = 10000;
        return this;
    }

    /**
     * 低延迟模式 - 较小批次，较短间隔
     */
    public DynamicDorisSinkBuilder<T> lowLatency() {
        this.batchSize = 500;
        this.batchIntervalMs = 2000;
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

        LOG.info("构建动态分表 Doris Sink: {}.{}_*, batchSize={}, batchIntervalMs={}, partialUpdate={}, enable2PC={}, columns={}",
                database, tablePrefix, batchSize, batchIntervalMs, partialUpdate, enable2PC, columns);

        DynamicTableSerializer<T> serializer = new DynamicTableSerializer<>(
                database, tablePrefix, appIdExtractor);

        return DorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .build();
    }

    public void addTo(DataStream<T> stream) {
        addTo(stream, "DorisSink-" + tablePrefix, "doris-sink-" + tablePrefix.replace("_", "-"));
    }

    public void addTo(DataStream<T> stream, String name, String uid) {
        if (callback != null) {
            // 使用带回调的 Sink
            CallbackDorisSink<T> sink = buildWithCallback();
            stream.sinkTo(sink).name(name).uid(uid);
        } else {
            stream.sinkTo(build()).name(name).uid(uid);
        }
        LOG.info("  ✓ {} 动态分表 Sink 已添加 (2PC={}, columns={})", tablePrefix, enable2PC, columns);
    }

    public CallbackDorisSink<T> buildWithCallback() {
        validate();

        if (callback == null) {
            throw new IllegalArgumentException("callback is required for buildWithCallback()");
        }

        LOG.info("构建带回调的动态分表 Doris Sink: {}.{}_*, batchSize={}, batchIntervalMs={}, enable2PC={}, columns={}",
                database, tablePrefix, batchSize, batchIntervalMs, enable2PC, columns);

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
        // partial update 模式下必须指定 columns
        if (partialUpdate && (columns == null || columns.isEmpty())) {
            throw new IllegalArgumentException("columns is required for partial update mode");
        }
    }

    private DorisOptions buildDorisOptions() {
        // 注意: 动态表名模式下，这里的 tableIdentifier 只是占位符
        // 实际表名由 DynamicTableSerializer 决定
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
        props.setProperty("strip_outer_array", String.valueOf(stripOuterArray));
        if (partialUpdate) {
            props.setProperty("partial_columns", "true");
            // 重要: JSON 格式的 partial update 必须指定 columns
            if (columns != null && !columns.isEmpty()) {
                props.setProperty("columns", columns);
            }
        }

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferCount(batchSize)
                .setBufferFlushIntervalMs(batchIntervalMs)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props);

        // 根据配置决定是否启用 2PC
        if (enable2PC) {
            builder.enable2PC();
        } else {
            builder.disable2PC();
        }

        return builder.build();
    }

    // ========== 静态快捷方法 ==========

    /**
     * 添加 UserRow 动态分表 Sink
     * 表名: b_user_{appId}
     * 列: device_id, zg_id, user_id, begin_date, platform
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
                .lowLatency()
                .partialUpdate()
                .columns("device_id,zg_id,user_id,begin_date,platform")  // 必须指定列名
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 DeviceRow 动态分表 Sink
     * 表名: b_device_{appId}
     * 列: device_id, device_md5, platform, device_type, l, h, device_brand, device_model,
     *     resolution, phone, imei, mac, is_prison_break, is_crack, language, timezone,
     *     attr1, attr2, attr3, attr4, attr5, last_update_date
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
                .partialUpdate()
                .columns("device_id,device_md5,platform,device_type,l,h,device_brand,device_model," +
                        "resolution,phone,imei,mac,is_prison_break,is_crack,language,timezone," +
                        "attr1,attr2,attr3,attr4,attr5,last_update_date")
                .lowLatency()
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 UserPropertyRow 动态分表 Sink
     * 表名: b_user_property_{appId}
     * 列: zg_id, property_id, user_id, property_name, property_data_type, property_value, platform, last_update_date
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
                .partialUpdate()
                .columns("zg_id,property_id,user_id,property_name,property_data_type,property_value,platform,last_update_date")
                .lowLatency()
                .onSuccess(callback)
                .addTo(stream);
    }

    /**
     * 添加 EventAttrRow 动态分表 Sink
     * 表名: b_user_event_attr_{appId}
     *
     * 注意: EventAttrRow 使用专用序列化器，不使用通用的 DynamicTableSerializer
     * EventAttrRow 不使用 partial update，因为字段太多且动态变化
     */
    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback) {

        LOG.info("构建 EventAttrRow 动态分表 Doris Sink: {}.b_user_event_attr_*", database);

        // 使用专用序列化器
        EventAttrRowSerializer serializer = new EventAttrRowSerializer(database, "b_user_event_attr");

        // 从配置读取参数
        String labelPrefix = Config.getString(Config.DORIS_SINK_LABEL_PREFIX, "flink_doris");
        int batchSize = 500;  // 低延迟模式
        long batchIntervalMs = 2000;
        int maxRetries = Config.getInt(Config.DORIS_SINK_MAX_RETRIES, 3);
        boolean enable2PC = Config.getBoolean(Config.DORIS_SINK_ENABLE_2PC, false);

        // 构建 Doris 选项
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes(feNodes)
                .setTableIdentifier(database + ".b_user_event_attr_0")
                .setUsername(username)
                .setPassword(password)
                .build();

        // 构建执行选项 - 注意：EventAttrRow 不使用 partial_columns
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("strip_outer_array", "true");
        // 不启用 partial_columns，因为我们会序列化所有非空字段

        DorisExecutionOptions.Builder execBuilder = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferCount(batchSize)
                .setBufferFlushIntervalMs(batchIntervalMs)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props);

        if (enable2PC) {
            execBuilder.enable2PC();
        } else {
            execBuilder.disable2PC();
        }

        DorisExecutionOptions execOptions = execBuilder.build();

        // 构建 Sink
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

        LOG.info("  ✓ b_user_event_attr 动态分表 Sink 已添加 (使用专用序列化器, 2PC={})", enable2PC);
    }
}
