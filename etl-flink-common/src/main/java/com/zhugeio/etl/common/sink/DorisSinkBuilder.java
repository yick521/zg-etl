package com.zhugeio.etl.common.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Doris Sink 构建器
 *
 * 无业务依赖，可跨项目复用
 *
 * 使用示例:
 * <pre>
 * DorisSinkBuilder.&lt;UserRow&gt;builder()
 *     .feNodes("doris-fe:8030")
 *     .database("dwd")
 *     .table("b_user")
 *     .username("root")
 *     .password("xxx")
 *     .partialUpdate()
 *     .serializer(JsonSerializerFactory.createDefaultSerializer())
 *     .onSuccess((table, count) -> log.info("写入成功: {}", count))
 *     .addTo(stream);
 * </pre>
 */
public class DorisSinkBuilder<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkBuilder.class);

    // 连接参数
    private String feNodes;
    private String database;
    private String table;
    private String username = "root";
    private String password = "";

    // 批量参数
    private String labelPrefix = "flink_doris";
    private int batchSize = 10000;          // bufferCount
    private long batchIntervalMs = 5000;    // bufferFlushIntervalMs
    private int maxRetries = 3;

    // Stream Load 参数
    private String format = "json";
    private boolean partialUpdate = false;
    private boolean stripOuterArray = true;

    // 序列化器 & 回调
    private DorisRecordSerializer<T> serializer;
    private CommitSuccessCallback callback;

    private DorisSinkBuilder() {}

    public static <T> DorisSinkBuilder<T> builder() {
        return new DorisSinkBuilder<>();
    }

    // ========== 连接参数 ==========

    public DorisSinkBuilder<T> feNodes(String feNodes) {
        this.feNodes = feNodes;
        return this;
    }

    public DorisSinkBuilder<T> database(String database) {
        this.database = database;
        return this;
    }

    public DorisSinkBuilder<T> table(String table) {
        this.table = table;
        return this;
    }

    public DorisSinkBuilder<T> username(String username) {
        this.username = username;
        return this;
    }

    public DorisSinkBuilder<T> password(String password) {
        this.password = password;
        return this;
    }

    // ========== 批量参数 ==========

    public DorisSinkBuilder<T> labelPrefix(String labelPrefix) {
        this.labelPrefix = labelPrefix;
        return this;
    }

    public DorisSinkBuilder<T> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public DorisSinkBuilder<T> batchIntervalMs(long batchIntervalMs) {
        this.batchIntervalMs = batchIntervalMs;
        return this;
    }

    public DorisSinkBuilder<T> maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    // ========== Stream Load 参数 ==========

    public DorisSinkBuilder<T> format(String format) {
        this.format = format;
        return this;
    }

    public DorisSinkBuilder<T> stripOuterArray(boolean strip) {
        this.stripOuterArray = strip;
        return this;
    }

    /**
     * 启用部分列更新
     */
    public DorisSinkBuilder<T> partialUpdate() {
        this.partialUpdate = true;
        return this;
    }

    public DorisSinkBuilder<T> partialUpdate(boolean enable) {
        this.partialUpdate = enable;
        return this;
    }

    // ========== 模式预设 ==========

    /**
     * 高吞吐模式 (大批量写入)
     */
    public DorisSinkBuilder<T> highThroughput() {
        this.batchSize = 50000;
        this.batchIntervalMs = 10000;
        return this;
    }

    /**
     * 低延迟模式 (实时写入)
     */
    public DorisSinkBuilder<T> lowLatency() {
        this.batchSize = 5000;
        this.batchIntervalMs = 2000;
        return this;
    }

    // ========== 序列化器 & 回调 ==========

    public DorisSinkBuilder<T> serializer(DorisRecordSerializer<T> serializer) {
        this.serializer = serializer;
        return this;
    }

    public DorisSinkBuilder<T> onSuccess(CommitSuccessCallback callback) {
        this.callback = callback;
        return this;
    }

    // ========== 构建 ==========

    /**
     * 构建标准 DorisSink (无回调)
     */
    public DorisSink<T> build() {
        validate();

        LOG.info("构建 Doris Sink: {}.{}, batchSize={}, partialUpdate={}",
                database, table, batchSize, partialUpdate);

        return DorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .build();
    }

    /**
     * 构建带回调的 CallbackDorisSink
     */
    public CallbackDorisSink<T> buildWithCallback() {
        validate();

        if (callback == null) {
            throw new IllegalArgumentException("callback is required for buildWithCallback(), use onSuccess() to set");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("serializer is required for buildWithCallback()");
        }

        LOG.info("构建 Callback Doris Sink: {}.{}, batchSize={}, partialUpdate={}",
                database, table, batchSize, partialUpdate);

        return CallbackDorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .setTableName(database + "." + table)
                .setCommitCallback(callback)
                .build();
    }

    /**
     * 构建并添加到流
     */
    public void addTo(DataStream<T> stream) {
        addTo(stream, "DorisSink-" + table, "doris-sink-" + table.replace("_", "-"));
    }

    public void addTo(DataStream<T> stream, String name, String uid) {
        if (callback != null && serializer != null) {
            stream.sinkTo(buildWithCallback()).name(name).uid(uid);
        } else {
            stream.sinkTo(build()).name(name).uid(uid);
        }
        LOG.info("  ✓ {} Sink 已添加", table);
    }

    // ========== 内部方法 ==========

    private void validate() {
        if (feNodes == null || feNodes.isEmpty()) {
            throw new IllegalArgumentException("feNodes is required");
        }
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("database is required");
        }
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table is required");
        }
    }

    private DorisOptions buildDorisOptions() {
        return DorisOptions.builder()
                .setFenodes(feNodes)
                .setTableIdentifier(database + "." + table)
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
        }

        return DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferCount(batchSize)
                .setBufferFlushIntervalMs(batchIntervalMs)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props)
                .enable2PC()
                .build();
    }

    // ========== 静态工厂方法 (简化调用) ==========

    /**
     * 快速添加标准 Sink
     */
    public static <T> void addSink(
            DataStream<T> stream,
            String feNodes, String database, String table,
            String username, String password,
            DorisRecordSerializer<T> serializer) {

        DorisSinkBuilder.<T>builder()
                .feNodes(feNodes)
                .database(database)
                .table(table)
                .username(username)
                .password(password)
                .serializer(serializer)
                .addTo(stream);
    }

    /**
     * 快速添加部分更新 Sink
     */
    public static <T> void addPartialUpdateSink(
            DataStream<T> stream,
            String feNodes, String database, String table,
            String username, String password,
            DorisRecordSerializer<T> serializer) {

        DorisSinkBuilder.<T>builder()
                .feNodes(feNodes)
                .database(database)
                .table(table)
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(serializer)
                .addTo(stream);
    }

    /**
     * 快速添加高吞吐 Sink
     */
    public static <T> void addHighThroughputSink(
            DataStream<T> stream,
            String feNodes, String database, String table,
            String username, String password,
            DorisRecordSerializer<T> serializer) {

        DorisSinkBuilder.<T>builder()
                .feNodes(feNodes)
                .database(database)
                .table(table)
                .username(username)
                .password(password)
                .highThroughput()
                .serializer(serializer)
                .addTo(stream);
    }
}