package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.config.FlinkEnvConfig;
import com.zhugeio.etl.common.sink.DorisSinkBuilder;
import com.zhugeio.etl.common.sink.JsonSerializerFactory;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.model.ToufangAdClickRow;
import com.zhugeio.etl.pipeline.model.ToufangConvertEventRow;
import com.zhugeio.etl.pipeline.operator.adv.AdvProcessFunction;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * @author ningjh
 * @name GateJob
 * @date 2025/11/28
 * @description 广告数据入库，kafka来源数据由 id 模块写入
 */
public class AdvJob {
    private static final Logger logger = LoggerFactory.getLogger(AdvJob.class);
    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";

    public static void main(String[] args) {
        // 读取配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 配置
        String checkpointPath = CHECKPOINT_BASE + Config.getString(Config.CHECKPOINT_ID_PATH, "adv_job");
        FlinkEnvConfig.configureCheckpoint(env, Config.getLong(Config.FLINK_CHECKPOINT_INTERVAL_MS, 30000L), checkpointPath);

        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        int parallelism = env.getParallelism();  // 提交任务的命令中指定的全局并行度
        logger.info("parallelism : {}",parallelism);
        String feNodes = Config.getString(Config.DORIS_FE_NODES);
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");

        // 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(Config.getString(Config.KAFKA_BROKERS))
                .setTopics(Config.getString(Config.KAFKA_ADV_SOURCE_TOPIC))
                .setGroupId(Config.getString(Config.KAFKA_ADV_GROUP_ID))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 优先用已提交偏移量，没有则从最早开始
                .setProperties(Config.getKafkaConsumerProps()) // 关键：添加 Kafka Consumer 配置来控制消费速率
                .setDeserializer(new ZGMsgSchema()) // 关键：解析POJO
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-source-adv"
        ).uid("Kafka-source-adv");


        final OutputTag<ToufangAdClickRow> advClickOutputTag = new OutputTag<ToufangAdClickRow>("advClick"){};
        SingleOutputStreamOperator<ToufangConvertEventRow> toufangConvertEventRowDataStream = stream.process(new AdvProcessFunction(advClickOutputTag));
        SideOutputDataStream<ToufangAdClickRow> sideOutputDataStream = toufangConvertEventRowDataStream.getSideOutput(advClickOutputTag);
        // 1.
        DorisRecordSerializer<ToufangConvertEventRow> eventSerializer =
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<ToufangConvertEventRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table(Config.getString(Config.ADV_SINK_EVENT_TABLE))
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(eventSerializer)
                .addTo(toufangConvertEventRowDataStream);

        DorisRecordSerializer<ToufangAdClickRow> clickSerializer =
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<ToufangAdClickRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table(Config.getString(Config.ADV_SINK_CLICK_TABLE))
                .username(username)
                .password(password)
                .partialUpdate()
                .serializer(clickSerializer)
                .addTo(sideOutputDataStream);

        try {
            env.execute("etl-pipLine-AdvJob");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
