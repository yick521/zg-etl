package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.config.FlinkEnvConfig;
import com.zhugeio.etl.common.sink.DorisSinkBuilder;
import com.zhugeio.etl.common.sink.JsonSerializerFactory;
import com.zhugeio.etl.common.source.KafkaSourceBuilder;
import com.zhugeio.etl.pipeline.entity.IdMappingMessage;
import com.zhugeio.etl.pipeline.kafka.IdMappingMessageSchema;
import com.zhugeio.etl.pipeline.model.*;
import com.zhugeio.etl.pipeline.operator.dw.IdMappingRouterOperator;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.zhugeio.etl.common.config.Config.*;

/**
 * ID 映射入库任务
 * 
 * 数据流:
 * Kafka (id_mapping_topic) → IdMappingRouter → 5个 Doris Sink
 * 
 * Doris 表:
 * - dwd_id_device: deviceMd5 → zgDeviceId
 * - dwd_id_user: cuid → zgUserId
 * - dwd_id_device_zgid: zgDeviceId → zgId
 * - dwd_id_user_zgid: zgUserId → zgId
 * - dwd_id_zgid_user: zgId → zgUserId
 */
public class IdMappingJob {

    private static final Logger LOG = LoggerFactory.getLogger(IdMappingJob.class);

    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";



    public static void main(String[] args) throws Exception {
        LOG.info("ID Mapping Sink Job 启动...");

        // 1. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointPath = CHECKPOINT_BASE + 
                Config.getString(CHECKPOINT_ID_MAPPING_PATH, "id_mapping_job");
        FlinkEnvConfig.configureCheckpoint(env, 60000L, checkpointPath);
        FlinkEnvConfig.configureFixedDelayRestart(env, 3, 10);

        // 2. Kafka Source
        String topic = Config.getString(KAFKA_ID_MAPPING_TOPIC, "id_mapping_topic");
        String groupId = Config.getString(KAFKA_ID_MAPPING_GROUP_ID, "id_mapping_sink_group");

        DataStream<IdMappingMessage> source = env.fromSource(
                KafkaSourceBuilder.<IdMappingMessage>builder()
                        .brokers(Config.getString(Config.KAFKA_BROKERS))
                        .topic(topic)
                        .groupId(groupId)
                        .deserializer(new IdMappingMessageSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "IdMappingKafkaSource"
        ).uid("id-mapping-kafka-source");

        // 3. 路由处理
        SingleOutputStreamOperator<DeviceIdRow> deviceStream = source
                .process(new IdMappingRouterOperator())
                .name("IdMappingRouter").uid("id-mapping-router");

        // 4. 获取侧输出
        DataStream<UserIdRow> userStream =
                deviceStream.getSideOutput(IdMappingRouterOperator.USER_OUTPUT);
        DataStream<DeviceZgidRow> deviceZgidStream =
                deviceStream.getSideOutput(IdMappingRouterOperator.DEVICE_ZGID_OUTPUT);
        DataStream<UserZgidRow> userZgidStream =
                deviceStream.getSideOutput(IdMappingRouterOperator.USER_ZGID_OUTPUT);
        DataStream<ZgidUserRow> zgidUserStream =
                deviceStream.getSideOutput(IdMappingRouterOperator.ZGID_USER_OUTPUT);

        // 5. Doris Sink (5张表)
        addDorisSinks(deviceStream, userStream, deviceZgidStream, 
                      userZgidStream, zgidUserStream);

        // 6. 执行
        env.execute("ID-Mapping-Sink-Job");
    }

    /**
     * 添加 5 个 Doris Sink
     */
    private static void addDorisSinks(
            DataStream<DeviceIdRow> deviceStream,
            DataStream<UserIdRow> userStream,
            DataStream<DeviceZgidRow> deviceZgidStream,
            DataStream<UserZgidRow> userZgidStream,
            DataStream<ZgidUserRow> zgidUserStream) {

        String feNodes = Config.getString(Config.DORIS_FE_NODES);
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");

        // 1. 设备映射表: dwd_id_device
        DorisRecordSerializer<DeviceIdRow> deviceSerializer = 
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<DeviceIdRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table("dwd_id_device")
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(deviceSerializer)
                .addTo(deviceStream);

        // 2. 用户映射表: dwd_id_user
        DorisRecordSerializer<UserIdRow> userSerializer = 
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<UserIdRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table("dwd_id_user")
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(userSerializer)
                .addTo(userStream);

        // 3. 设备→诸葛ID映射表: dwd_id_device_zgid
        DorisRecordSerializer<DeviceZgidRow> deviceZgidSerializer = 
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<DeviceZgidRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table("dwd_id_device_zgid")
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(deviceZgidSerializer)
                .addTo(deviceZgidStream);

        // 4. 用户→诸葛ID映射表: dwd_id_user_zgid
        DorisRecordSerializer<UserZgidRow> userZgidSerializer = 
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<UserZgidRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table("dwd_id_user_zgid")
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(userZgidSerializer)
                .addTo(userZgidStream);

        // 5. 诸葛ID→用户映射表: dwd_id_zgid_user
        DorisRecordSerializer<ZgidUserRow> zgidUserSerializer = 
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<ZgidUserRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table("dwd_id_zgid_user")
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(zgidUserSerializer)
                .addTo(zgidUserStream);

        LOG.info("Doris Sink 配置完成: 5张ID映射表");
    }
}
