package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.config.FlinkEnvConfig;
import com.zhugeio.etl.common.sink.DorisSinkBuilder;
import com.zhugeio.etl.common.sink.JsonSerializerFactory;
import com.zhugeio.etl.common.source.KafkaSourceBuilder;
import com.zhugeio.etl.common.util.DorisTableInitializer;
import com.zhugeio.etl.pipeline.entity.IdArchiveMessage;
import com.zhugeio.etl.pipeline.kafka.IdArchiveMessageSchema;
import com.zhugeio.etl.pipeline.model.*;
import com.zhugeio.etl.pipeline.operator.dw.IdArchiveRouterOperator;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
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
 * Kafka (id_archive_topic) → IdArchiveRouter → 5个 Doris Sink
 * 启动参数:
 * --init.sql.path /path/to/id_archive_tables.sql  (可选，指定建表 SQL 文件)
 *
 * Doris 表:
 * - dwd_id_device: deviceMd5 → zgDeviceId
 * - dwd_id_user: cuid → zgUserId
 * - dwd_id_device_zgid: zgDeviceId → zgId
 * - dwd_id_user_zgid: zgUserId → zgId
 * - dwd_id_zgid_user: zgId → zgUserId
 */
public class IdArchiveJob {

    private static final Logger LOG = LoggerFactory.getLogger(IdArchiveJob.class);

    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";



    public static void main(String[] args) throws Exception {
        LOG.info("ID Mapping Sink Job 启动...");


        ParameterTool params = ParameterTool.fromArgs(args);
        // 1. 初始化表 (如果指定了 SQL 文件)
        initTablesIfNeeded(params);

        // 2. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointPath = CHECKPOINT_BASE + 
                Config.getString(CHECKPOINT_ID_ARCHIVE_PATH, "id_mapping_job");
        FlinkEnvConfig.configureCheckpoint(env, 60000L, checkpointPath);
        FlinkEnvConfig.configureFixedDelayRestart(env, 3, 10);

        // 3. Kafka Source
        String topic = Config.getString(KAFKA_ID_ARCHIVE_TOPIC, "id_archive_topic");
        String groupId = Config.getString(KAFKA_ID_ARCHIVE_GROUP_ID, "id_mapping_sink_group");

        DataStream<IdArchiveMessage> source = env.fromSource(
                KafkaSourceBuilder.<IdArchiveMessage>builder()
                        .brokers(Config.getString(Config.KAFKA_BROKERS))
                        .topic(topic)
                        .groupId(groupId)
                        .deserializer(new IdArchiveMessageSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "IdMappingKafkaSource"
        ).uid("id-mapping-kafka-source");

        // 4. 路由处理
        SingleOutputStreamOperator<DeviceIdRow> deviceStream = source
                .process(new IdArchiveRouterOperator())
                .name("IdMappingRouter").uid("id-mapping-router");

        // 5. 获取侧输出
        DataStream<UserIdRow> userStream =
                deviceStream.getSideOutput(IdArchiveRouterOperator.USER_OUTPUT);
        DataStream<DeviceZgidRow> deviceZgidStream =
                deviceStream.getSideOutput(IdArchiveRouterOperator.DEVICE_ZGID_OUTPUT);
        DataStream<UserZgidRow> userZgidStream =
                deviceStream.getSideOutput(IdArchiveRouterOperator.USER_ZGID_OUTPUT);
        DataStream<ZgidUserRow> zgidUserStream =
                deviceStream.getSideOutput(IdArchiveRouterOperator.ZGID_USER_OUTPUT);

        // 5. Doris Sink (5张表)
        addDorisSinks(deviceStream, userStream, deviceZgidStream, 
                      userZgidStream, zgidUserStream);

        // 6. 执行
        env.execute("ID-Mapping-Sink-Job");
    }

    /**
     * 如果指定了 SQL 文件，则初始化表
     */
    private static void initTablesIfNeeded(ParameterTool params) {
        String sqlPath = params.get(INIT_SQL);

        if (sqlPath == null || sqlPath.isEmpty()) {
            LOG.info("未指定 --init.sql，跳过表初始化");
            return;
        }

        String feNodes = Config.getString(Config.DORIS_FE_NODES);
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");

        DorisTableInitializer initializer = new DorisTableInitializer(
                feNodes, database, username, password);

        LOG.info("初始化表: {}", sqlPath);
        initializer.init(sqlPath);
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
