package com.zhugeio.etl.common.sink;

import com.zhugeio.etl.common.config.Config;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * 动态分表 Doris Sink 构建器
 * <p>
 * 支持:
 * 1. 动态分表 (按 appId)
 * 2. 独立并行度设置 (避免反压)
 * 3. rebalance 数据打散 (避免倾斜)
 * 4. Group Commit (Doris 2.1+)
 * 5. 部分列更新
 * 6. 流式/批量写入模式切换
 * 7. 自定义序列化器
 * <p>
 * 使用示例:
 * <pre>{@code
 * // 流式写入（默认，依赖 Checkpoint）
 * DynamicDorisSinkBuilder.<UserRow>builder()
 *     .feNodes("fe1:8030,fe2:8030")
 *     .database("analytics")
 *     .tablePrefix("b_user")
 *     .appIdExtractor(UserRow::getAppId)
 *     .streamingMode()  // 可选，默认就是流式模式
 *     .addTo(stream);
 *
 * // 批量写入（不依赖 Checkpoint，高吞吐场景）
 * DynamicDorisSinkBuilder.<UserRow>builder()
 *     .feNodes("fe1:8030,fe2:8030")
 *     .database("analytics")
 *     .tablePrefix("b_user")
 *     .appIdExtractor(UserRow::getAppId)
 *     .batchMode()
 *     .batchMaxRows(100000)    // 可选，行数阈值
 *     .batchMaxBytesMB(50)     // 可选，字节阈值
 *     .batchFlushIntervalMs(5000)  // 可选，时间阈值
 *     .addTo(stream);
 *
 * // 自定义序列化器
 * DynamicDorisSinkBuilder.<EventAttrRow>builder()
 *     .feNodes("fe1:8030,fe2:8030")
 *     .database("analytics")
 *     .tablePrefix("b_user_event_attr")
 *     .serializer(new EventAttrRowSerializer(database, tablePrefix))
 *     .addTo(stream);
 * }</pre>
 */
public class DynamicDorisSinkBuilder<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicDorisSinkBuilder.class);

    // ==================== 写入模式枚举 ====================

    /**
     * 写入模式
     */
    public enum WriteMode {
        /**
         * 流式写入模式（默认）
         * - 依赖 Flink Checkpoint，在整个 Checkpoint 周期内持续写入数据
         * - 不会一直将数据缓存在内存中
         * - 支持 Exactly-Once 语义（需开启 2PC）
         * - 适合实时数据处理场景
         */
        STREAMING,

        /**
         * 批量写入模式
         * - 不依赖 Checkpoint，将数据缓存在内存中
         * - 根据 batchSize / bufferSize / flushInterval 参数控制写入时机
         * - 提供 At-Least-Once 语义，结合 Unique 模型可实现幂等写入
         * - 适合离线数据处理、数据迁移场景
         */
        BATCH
    }

    // ==================== 默认值常量 ====================

    // 流式模式默认值（写入时机由 Checkpoint 控制）
    private static final int DEFAULT_STREAMING_BUFFER_SIZE = 3 * 1024 * 1024;    // 3MB (Stream Load 缓冲)
    private static final int DEFAULT_STREAMING_BUFFER_COUNT = 2;                  // 缓冲区数量

    // 批量模式默认值（写入时机由以下阈值控制，任一满足即触发）
    private static final int DEFAULT_BATCH_MAX_BYTES = 100 * 1024 * 1024;        // 100MB (sink.buffer-flush.max-bytes)
    private static final int DEFAULT_BATCH_MAX_ROWS = 500000;                     // 50万条 (sink.buffer-flush.max-rows)
    private static final long DEFAULT_BATCH_FLUSH_INTERVAL = 10000L;              // 10s (sink.buffer-flush.interval)

    private static final int DEFAULT_SINK_PARALLELISM = -1;            // -1 表示使用环境默认并行度

    // ==================== 写入模式 ====================
    private WriteMode writeMode = WriteMode.STREAMING;

    // ==================== 连接参数 ====================
    private String feNodes;
    private String database;
    private String tablePrefix;
    private String username = "root";
    private String password = "";

    // ==================== 数据提取器 ====================
    private SerializableFunction<T, Integer> appIdExtractor;

    // ==================== 自定义序列化器 ====================
    private DorisRecordSerializer<T> customSerializer;

    // ==================== Buffer 参数（流式模式使用）====================
    private String labelPrefix;
    private int bufferSize;
    private int bufferCount;
    private int maxRetries;
    private boolean enable2PC;

    // ==================== 批量模式参数 ====================
    private int batchMaxRows;
    private int batchMaxBytes;
    private long batchFlushIntervalMs;

    // ==================== Stream Load 参数 ====================
    private String format = "json";
    private boolean partialUpdate = false;
    private String columns;

    // ==================== Group Commit 参数 ====================
    private boolean enableGroupCommit = false;
    private String groupCommitMode = "async_mode";

    // ==================== 并行度和数据分布 ====================
    private int parallelism = DEFAULT_SINK_PARALLELISM;
    private boolean rebalance = false;

    // ==================== 回调 ====================
    private CommitSuccessCallback callback;

    // ==================== 构造器 ====================

    private DynamicDorisSinkBuilder() {

        // 通用参数
        this.labelPrefix = Config.getString(Config.DORIS_SINK_LABEL_PREFIX, "flink_doris");
        this.maxRetries = Config.getInt(Config.DORIS_SINK_MAX_RETRIES, 3);
        this.enable2PC = Config.getBoolean(Config.DORIS_SINK_ENABLE_2PC, false);
        // 流式模式参数
        this.bufferSize = Config.getInt(Config.DORIS_SINK_BUFFER_SIZE, DEFAULT_STREAMING_BUFFER_SIZE);
        this.bufferCount = Config.getInt(Config.DORIS_SINK_BUFFER_COUNT, DEFAULT_STREAMING_BUFFER_COUNT);
        // 批量模式参数
        this.batchMaxRows = Config.getInt(Config.DORIS_SINK_BATCH_MAX_ROWS, DEFAULT_BATCH_MAX_ROWS);
        this.batchMaxBytes = Config.getInt(Config.DORIS_SINK_BATCH_MAX_BYTES, DEFAULT_BATCH_MAX_BYTES);
        this.batchFlushIntervalMs = Config.getLong(Config.DORIS_SINK_BATCH_FLUSH_INTERVAL, DEFAULT_BATCH_FLUSH_INTERVAL);
    }

    public static <T> DynamicDorisSinkBuilder<T> builder() {
        return new DynamicDorisSinkBuilder<>();
    }

    // ==================== 连接参数配置 ====================

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

    // ==================== 自定义序列化器配置 ====================

    /**
     * 设置自定义序列化器
     * <p>
     * 如果设置了自定义序列化器，将忽略 appIdExtractor，使用自定义序列化器进行序列化。
     * 适用于有特殊序列化需求的场景，如 EventAttrRow。
     *
     * @param serializer 自定义序列化器
     */
    public DynamicDorisSinkBuilder<T> serializer(DorisRecordSerializer<T> serializer) {
        this.customSerializer = serializer;
        return this;
    }

    // ==================== Buffer 参数配置（流式模式）====================

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

    public DynamicDorisSinkBuilder<T> maxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public DynamicDorisSinkBuilder<T> enable2PC(boolean enable) {
        this.enable2PC = enable;
        return this;
    }

    // ==================== 批量模式参数配置 ====================

    /**
     * 设置批量写入的行数阈值
     * 仅在批量模式下生效，当累积数据达到此行数时触发写入
     *
     * @param maxRows 行数阈值，默认 500000
     */
    public DynamicDorisSinkBuilder<T> batchMaxRows(int maxRows) {
        this.batchMaxRows = maxRows;
        return this;
    }

    /**
     * 设置批量写入的字节数阈值
     * 仅在批量模式下生效，当累积数据达到此字节数时触发写入
     *
     * @param maxBytes 字节阈值，默认 100MB
     */
    public DynamicDorisSinkBuilder<T> batchMaxBytes(int maxBytes) {
        this.batchMaxBytes = maxBytes;
        return this;
    }

    /**
     * 设置批量写入的字节数阈值（MB）
     */
    public DynamicDorisSinkBuilder<T> batchMaxBytesMB(int mb) {
        this.batchMaxBytes = mb * 1024 * 1024;
        return this;
    }

    /**
     * 设置批量写入的时间间隔阈值
     * 仅在批量模式下生效，即使未达到行数/字节数阈值，超过此时间也会触发写入
     *
     * @param intervalMs 时间间隔（毫秒），默认 10000
     */
    public DynamicDorisSinkBuilder<T> batchFlushIntervalMs(long intervalMs) {
        this.batchFlushIntervalMs = intervalMs;
        return this;
    }

    // ==================== Stream Load 参数配置 ====================

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

    // ==================== 写入模式配置 ====================

    /**
     * 设置为流式写入模式（默认）
     * <p>
     * 特点：
     * - 依赖 Flink Checkpoint，在整个 Checkpoint 周期内持续写入数据
     * - 不会一直将数据缓存在内存中
     * - 支持 Exactly-Once 语义（需开启 2PC）
     * - 适合实时数据处理场景
     */
    public DynamicDorisSinkBuilder<T> streamingMode() {
        this.writeMode = WriteMode.STREAMING;
        return this;
    }

    /**
     * 设置为批量写入模式
     * <p>
     * 特点：
     * - 不依赖 Checkpoint，将数据缓存在内存中
     * - 根据 batchMaxRows / batchMaxBytes / batchFlushIntervalMs 参数控制写入时机
     * - 提供 At-Least-Once 语义，结合 Doris Unique 模型可实现幂等写入
     * - 适合离线数据处理、数据迁移场景
     * <p>
     * 默认参数（参考 Doris 官方文档）：
     * - sink.buffer-flush.max-rows: 500000
     * - sink.buffer-flush.max-bytes: 100MB
     * - sink.buffer-flush.interval: 10s
     */
    public DynamicDorisSinkBuilder<T> batchMode() {
        this.writeMode = WriteMode.BATCH;
        return this;
    }

    /**
     * 设置写入模式
     *
     * @param mode 写入模式
     */
    public DynamicDorisSinkBuilder<T> writeMode(WriteMode mode) {
        this.writeMode = mode;
        return this;
    }

    // ==================== 并行度和数据分布配置 ====================

    /**
     * 设置 Sink 并行度
     *
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

    // ==================== 回调配置 ====================

    public DynamicDorisSinkBuilder<T> onSuccess(CommitSuccessCallback callback) {
        this.callback = callback;
        return this;
    }

    // ==================== 构建方法 ====================

    /**
     * 构建 DorisSink（无回调）
     */
    public DorisSink<T> build() {
        validate();
        logBuildInfo();

        DorisRecordSerializer<T> serializer = getSerializer();

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
            throw new IllegalArgumentException("callback is required for buildWithCallback()");
        }

        logBuildInfo();

        DorisRecordSerializer<T> serializer = getSerializer();
        return CallbackDorisSink.<T>builder()
                .setDorisOptions(buildDorisOptions())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(buildExecutionOptions())
                .setSerializer(serializer)
                .setTableName(database + "." + tablePrefix + "_*")
                .setCommitCallback(callback)
                .build();
    }

    /**
     * 添加 Sink 到 DataStream（使用默认名称）
     */
    public void addTo(DataStream<T> stream) {
        addTo(stream, "DorisSink-" + tablePrefix, "doris-sink-" + tablePrefix.replace("_", "-"));
    }

    /**
     * 添加 Sink 到 DataStream，支持 rebalance 和独立并行度
     */
    public void addTo(DataStream<T> stream, String name, String uid) {
        DataStream<T> finalStream = rebalance ? stream.rebalance() : stream;

        DataStreamSink<T> sinkOp;
        if (callback != null) {
            sinkOp = finalStream.sinkTo(buildWithCallback()).name(name).uid(uid);
        } else {
            sinkOp = finalStream.sinkTo(build()).name(name).uid(uid);
        }

        if (parallelism > 0) {
            sinkOp.setParallelism(parallelism);
        }

        LOG.info("  ✓ {} Sink 已添加 (writeMode={}, parallelism={}, rebalance={}, customSerializer={}{})",
                tablePrefix, writeMode,
                parallelism > 0 ? parallelism : "default", rebalance,
                customSerializer != null,
                writeMode == WriteMode.BATCH
                        ? String.format(", maxRows=%d, maxBytes=%dMB, interval=%dms", batchMaxRows, batchMaxBytes / 1024 / 1024, batchFlushIntervalMs)
                        : String.format(", bufferSize=%dMB, bufferCount=%d", bufferSize / 1024 / 1024, bufferCount));
    }

    // ==================== 内部方法 ====================

    /**
     * 获取序列化器（优先使用自定义序列化器）
     */
    private DorisRecordSerializer<T> getSerializer() {
        if (customSerializer != null) {
            LOG.info("使用自定义序列化器: {}", customSerializer.getClass().getSimpleName());
            return customSerializer;
        }
        // 使用默认的 DynamicTableSerializer
        return new DynamicTableSerializer<>(database, tablePrefix, appIdExtractor);
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
        // 如果没有自定义序列化器，则需要 appIdExtractor
        if (customSerializer == null && appIdExtractor == null) {
            throw new IllegalArgumentException("appIdExtractor is required when no custom serializer is provided");
        }
        if (partialUpdate && (columns == null || columns.isEmpty())) {
            throw new IllegalArgumentException("columns is required for partial update mode");
        }
    }

    private void logBuildInfo() {
        String serializerInfo = customSerializer != null
                ? "customSerializer=" + customSerializer.getClass().getSimpleName()
                : "defaultSerializer";

        if (writeMode == WriteMode.BATCH) {
            LOG.info("构建动态分表 Doris Sink: {}.{}_*, writeMode=BATCH, {}, maxRows={}, maxBytes={}MB, interval={}ms, " +
                            "partialUpdate={}, groupCommit={}, parallelism={}, rebalance={}",
                    database, tablePrefix, serializerInfo,
                    batchMaxRows, batchMaxBytes / 1024 / 1024, batchFlushIntervalMs,
                    partialUpdate, enableGroupCommit,
                    parallelism > 0 ? parallelism : "default", rebalance);
        } else {
            LOG.info("构建动态分表 Doris Sink: {}.{}_*, writeMode=STREAMING, {}, bufferSize={}MB, bufferCount={}, " +
                            "partialUpdate={}, enable2PC={}, groupCommit={}, parallelism={}, rebalance={}",
                    database, tablePrefix, serializerInfo,
                    bufferSize / 1024 / 1024, bufferCount,
                    partialUpdate, enable2PC, enableGroupCommit,
                    parallelism > 0 ? parallelism : "default", rebalance);
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
        LOG.info("Doris Sink 执行参数: {}", labelPrefix);
        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder()
                .setLabelPrefix(labelPrefix)
                .setBufferSize(bufferSize)
                .setBufferCount(bufferCount)
                .setMaxRetries(maxRetries)
                .setStreamLoadProp(props);

        // 根据写入模式设置
        if (writeMode == WriteMode.BATCH) {
            // 批量模式：不依赖 Checkpoint，根据攒批参数控制写入时机
            builder.setBatchMode(true);
            builder.setBufferFlushMaxRows(batchMaxRows);
            builder.setBufferFlushMaxBytes(batchMaxBytes);
            builder.setBufferFlushIntervalMs(batchFlushIntervalMs);
            // 批量模式不建议开启 2PC
            builder.disable2PC();
            LOG.info("启用批量写入模式: maxRows={}, maxBytes={}MB, interval={}ms",
                    batchMaxRows, batchMaxBytes / 1024 / 1024, batchFlushIntervalMs);
        } else {
            // 流式模式：依赖 Checkpoint
            if (enable2PC) {
                builder.enable2PC();
            } else {
                builder.disable2PC();
            }
        }

        return builder.build();
    }

    // ==================== Getter 方法（供 Factory 使用）====================

    public int getParallelism() {
        return parallelism;
    }

    public boolean isRebalance() {
        return rebalance;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public int getBatchMaxRows() {
        return batchMaxRows;
    }

    public int getBatchMaxBytes() {
        return batchMaxBytes;
    }

    public long getBatchFlushIntervalMs() {
        return batchFlushIntervalMs;
    }

    public String getDatabase() {
        return database;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }
}