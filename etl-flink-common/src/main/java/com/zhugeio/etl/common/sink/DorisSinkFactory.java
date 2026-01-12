package com.zhugeio.etl.common.sink;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import com.zhugeio.etl.common.sink.DynamicDorisSinkBuilder.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Doris Sink 工厂类
 * <p>
 * 提供各种 Row 类型的便捷创建方法，封装常用配置。
 * 支持流式写入（默认）和批量写入两种模式。
 * <p>
 * 使用示例:
 * <pre>{@code
 * // 流式写入（默认，用于实时 ETL）
 * DorisSinkFactory.addUserSink(stream, feNodes, database, username, password, callback);
 *
 * // 批量写入（用于历史数据回刷）
 * DorisSinkFactory.addUserSink(stream, feNodes, database, username, password, callback, 4, WriteMode.BATCH);
 * }</pre>
 */
public final class DorisSinkFactory {

    private DorisSinkFactory() {
    }

    // ==================== UserRow Sink ====================

    public static void addUserSink(DataStream<UserRow> stream,
                                   String feNodes, String database,
                                   String username, String password,
                                   CommitSuccessCallback callback) {
        addUserSink(stream, feNodes, database, username, password, -1, WriteMode.STREAMING);
    }

    public static void addUserSink(DataStream<UserRow> stream,
                                   String feNodes, String database,
                                   String username, String password,
                                   int parallelism) {
        addUserSink(stream, feNodes, database, username, password, parallelism, WriteMode.STREAMING);
    }

    public static void addUserSink(DataStream<UserRow> stream,
                                   String feNodes, String database,
                                   String username, String password,
                                   int parallelism,
                                   WriteMode writeMode) {
        DynamicDorisSinkBuilder<UserRow> builder = DynamicDorisSinkBuilder.<UserRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user")
                .appIdExtractor(UserRow::getAppId)
                .username(username)
                .password(password)
                .writeMode(writeMode)
                .partialUpdate()
                .columns("device_id,zg_id,user_id,begin_date,platform")
                .rebalance(parallelism);

        applyModeConfig(builder, writeMode);
        builder.addTo(stream);
    }

    // ==================== DeviceRow Sink ====================

    public static void addDeviceSink(DataStream<DeviceRow> stream,
                                     String feNodes, String database,
                                     String username, String password) {
        addDeviceSink(stream, feNodes, database, username, password, -1, WriteMode.STREAMING);
    }

    public static void addDeviceSink(DataStream<DeviceRow> stream,
                                     String feNodes, String database,
                                     String username, String password,
                                     int parallelism) {
        addDeviceSink(stream, feNodes, database, username, password, parallelism, WriteMode.STREAMING);
    }

    public static void addDeviceSink(DataStream<DeviceRow> stream,
                                     String feNodes, String database,
                                     String username, String password,
                                     int parallelism,
                                     WriteMode writeMode) {
        DynamicDorisSinkBuilder<DeviceRow> builder = DynamicDorisSinkBuilder.<DeviceRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_device")
                .appIdExtractor(DeviceRow::getAppId)
                .username(username)
                .password(password)
                .writeMode(writeMode)
                .partialUpdate()
                .columns("device_id,device_md5,platform,device_type,l,h,device_brand,device_model," +
                        "resolution,phone,imei,mac,is_prison_break,is_crack,language,timezone," +
                        "attr1,attr2,attr3,attr4,attr5,last_update_date")
                .rebalance(parallelism);

        applyModeConfig(builder, writeMode);
        builder.addTo(stream);
    }

    // ==================== UserPropertyRow Sink ====================

    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                           String feNodes, String database,
                                           String username, String password,
                                           CommitSuccessCallback callback) {
        addUserPropertySink(stream, feNodes, database, username, password, callback, -1, WriteMode.STREAMING);
    }

    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                           String feNodes, String database,
                                           String username, String password,
                                           CommitSuccessCallback callback,
                                           int parallelism) {
        addUserPropertySink(stream, feNodes, database, username, password, callback, parallelism, WriteMode.STREAMING);
    }

    public static void addUserPropertySink(DataStream<UserPropertyRow> stream,
                                           String feNodes, String database,
                                           String username, String password,
                                           CommitSuccessCallback callback,
                                           int parallelism,
                                           WriteMode writeMode) {
        DynamicDorisSinkBuilder<UserPropertyRow> builder = DynamicDorisSinkBuilder.<UserPropertyRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user_property")
                .appIdExtractor(UserPropertyRow::getAppId)
                .username(username)
                .password(password)
                .writeMode(writeMode)
                .partialUpdate()
                .columns("zg_id,property_id,user_id,property_name,property_data_type,property_value,platform,last_update_date")
                .rebalance(parallelism)
                .onSuccess(callback);

        applyModeConfig(builder, writeMode);
        builder.addTo(stream);
    }

    // ==================== EventAttrRow Sink ====================

    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback) {
        addEventAttrSink(stream, feNodes, database, username, password, callback, -1, WriteMode.STREAMING);
    }

    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback,
                                        int parallelism) {
        addEventAttrSink(stream, feNodes, database, username, password, callback, parallelism, WriteMode.STREAMING);
    }

    /**
     * 添加 EventAttrRow 动态分表 Sink
     * <p>
     * EventAttr 使用自定义的 EventAttrRowSerializer，支持其特殊的 toJson() 方法。
     */
    public static void addEventAttrSink(DataStream<EventAttrRow> stream,
                                        String feNodes, String database,
                                        String username, String password,
                                        CommitSuccessCallback callback,
                                        int parallelism,
                                        WriteMode writeMode) {
        // 使用自定义序列化器
        EventAttrRowSerializer serializer = new EventAttrRowSerializer(database, "b_user_event_attr");

        DynamicDorisSinkBuilder<EventAttrRow> builder = DynamicDorisSinkBuilder.<EventAttrRow>builder()
                .feNodes(feNodes)
                .database(database)
                .tablePrefix("b_user_event_attr")
                .serializer(serializer)  // 使用自定义序列化器
                .username(username)
                .password(password)
                .writeMode(writeMode)
                .rebalance(parallelism)
                .onSuccess(callback);

        applyModeConfig(builder, writeMode);
        builder.addTo(stream);
    }

    // ==================== 私有方法 ====================

    /**
     * 根据写入模式应用配置
     */
    private static <T> void applyModeConfig(DynamicDorisSinkBuilder<T> builder, WriteMode writeMode) {
        if (writeMode == WriteMode.BATCH) {
            builder.batchMaxRows(Config.getInt(Config.DORIS_SINK_BATCH_MAX_ROWS, 500000))
                    .batchMaxBytes(Config.getInt(Config.DORIS_SINK_BATCH_MAX_BYTES, 100 * 1024 * 1024))
                    .batchFlushIntervalMs(Config.getLong(Config.DORIS_SINK_BATCH_FLUSH_INTERVAL, 10000L));
        } else {
            builder.bufferSize(Config.getInt(Config.DORIS_SINK_BUFFER_SIZE, 3 * 1024 * 1024))
                    .bufferCount(Config.getInt(Config.DORIS_SINK_BUFFER_COUNT, 2));
        }
    }
}