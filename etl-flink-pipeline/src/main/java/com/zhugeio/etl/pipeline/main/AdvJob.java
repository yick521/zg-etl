package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.sink.DorisSinkBuilder;
import com.zhugeio.etl.common.sink.JsonSerializerFactory;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.model.ToufangAdClickRow;
import com.zhugeio.etl.pipeline.model.ToufangConvertEventRow;
import com.zhugeio.etl.pipeline.operator.adv.AdvFlatMapFunction;
import com.zhugeio.etl.pipeline.operator.adv.AdvProcessFunction;
import com.zhugeio.etl.pipeline.operator.gate.GateFlatMapFunction;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
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
    public static void main(String[] args) {
        // 读取配置
        Properties configProperties = PropertiesUtil.getProperties("config.properties");
        logger.info("configProperties : {}",configProperties);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env,configProperties);
        int parallelism = env.getParallelism();  // 提交任务的命令中指定的全局并行度
        logger.info("parallelism : {}",parallelism);
        String feNodes = Config.getString(Config.DORIS_FE_NODES);
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");

        // 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(configProperties.getProperty("kafka.brokers"))
                .setTopics(configProperties.getProperty("kafka.adv.sinkTopic"))
                .setGroupId(configProperties.getProperty("kafka.adv.group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 优先用已提交偏移量，没有则从最早开始
                .setProperties(getRateLimitProperties(configProperties)) // 关键：添加 Kafka Consumer 配置来控制消费速率
                .setDeserializer(new ZGMsgSchema()) // 关键：解析POJO
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-source-adv"
        ).uid("Kafka-source-adv");

        SingleOutputStreamOperator<ZGMessage> flatMapStream = stream.flatMap(new GateFlatMapFunction(configProperties));


        final OutputTag<ToufangAdClickRow> advClickOutputTag = new OutputTag<ToufangAdClickRow>("advClick"){};

        SingleOutputStreamOperator<ToufangConvertEventRow> toufangConvertEventRowDataStream = flatMapStream.process(new AdvProcessFunction(advClickOutputTag));
        SideOutputDataStream<ToufangAdClickRow> sideOutputDataStream = toufangConvertEventRowDataStream.getSideOutput(advClickOutputTag);
        // 1.
        DorisRecordSerializer<ToufangConvertEventRow> eventSerializer =
                JsonSerializerFactory.createDefaultSerializer();
        DorisSinkBuilder.<ToufangConvertEventRow>builder()
                .feNodes(feNodes)
                .database(database)
                .table(configProperties.getProperty("adv.sink.event.table"))
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
                .table(configProperties.getProperty("adv.sink.click.table"))
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

    /**
     *
     * @param env
     * @param properties
     */
    public static void initCheckpoint(StreamExecutionEnvironment env, Properties properties){
        env.enableCheckpointing(Integer.parseInt(properties.getProperty("checkpoint.gate.interval")));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // 1. 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 2. 设置Checkpoint存储（文件系统）
        String checkpointPath = "hdfs:///user/flink/checkpoints/"+properties.getProperty("checkpoint.gate.path");
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        //            env.setStateBackend(new RocksDBStateBackend("hdfs:///user/flink/checkpoints/"+properties.getProperty("checkpoint.gate.path"), true));
    }

    /**
     * 专门设置速率限制属性的方法
     * @param configProperties
     * @return
     */
    private static Properties getRateLimitProperties(Properties configProperties) {
        Properties props = new Properties();
        // 每个分区fetch最大字节
        props.setProperty("max.partition.fetch.bytes", configProperties.getProperty("kafka.max.partition.fetch.bytes"));
        // 每次poll最大记录数
        props.setProperty("max.poll.records", configProperties.getProperty("kafka.max.poll.records"));
        // 服务器等待时间
        props.setProperty("fetch.max.wait.ms", configProperties.getProperty("kafka.fetch.max.wait.ms"));
        return props;
    }
}
