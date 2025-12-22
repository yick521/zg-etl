package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.gate.GateFlatMapFunction;
import com.zhugeio.etl.pipeline.operator.gate.GateProcessFunction;
import com.zhugeio.etl.pipeline.sink.CustomKafkaSink;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author ningjh
 * @name GateJob
 * @date 2025/11/28
 * @description
 */
public class GateJob {
    private static final Logger logger = LoggerFactory.getLogger(GateJob.class);
    public static void main(String[] args) {
        // 读取配置
        Properties configProperties = PropertiesUtil.getProperties("config.properties");
        logger.info("configProperties : {}",configProperties);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env,configProperties);
        int parallelism = env.getParallelism();  // 提交任务的命令中指定的全局并行度
        logger.info("parallelism : {}",parallelism);

        // 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(configProperties.getProperty("kafka.brokers"))
                .setTopics(configProperties.getProperty("kafka.gate.sourceTopic"))
                .setGroupId(configProperties.getProperty("kafka.gate.group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 优先用已提交偏移量，没有则从最早开始
                .setProperties(getRateLimitProperties(configProperties)) // 关键：添加 Kafka Consumer 配置来控制消费速率
                .setDeserializer(new ZGMsgSchema()) // 关键：解析POJO
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-source-gate"
        ).uid("gate-kafka-source");

        // 3.处理数据
        SingleOutputStreamOperator<ZGMessage> gateFlatMapStream = stream.flatMap(new GateFlatMapFunction(configProperties)).setParallelism(parallelism);

        // 4. 创建侧输出流-数据质量、debug数据
        final OutputTag<String> dataQualityTag4gate = new OutputTag<String>("dataQualityTag4gate"){};
        final OutputTag<String> debugTag4gate = new OutputTag<String>("debugTag4gate"){};

        // 5.分流 防止 gateFlatMapStream 被多次遍历
        SingleOutputStreamOperator<ZGMessage> process = gateFlatMapStream.process(new GateProcessFunction(dataQualityTag4gate, debugTag4gate));

        // 6.1 侧输出 - 数据质量
        DataStream<String> sideOutput4dataQuality = process.getSideOutput(dataQualityTag4gate);
        CustomKafkaSink.addCustomKafkaSink(sideOutput4dataQuality, configProperties.getProperty("kafka.quality.topic"),
                configProperties.getProperty("kafka.brokers"), true, "gate-data-quality-sink");

        // 6.2 侧输出 - debug
        DataStream<String> sideOutput4debug = process.getSideOutput(debugTag4gate);
        CustomKafkaSink.addCustomKafkaSink(sideOutput4debug, configProperties.getProperty("kafka.debug.topic"),
                configProperties.getProperty("kafka.brokers"), true, "gate-debug-sink");

        // 6.3 主流输出到下游
        CustomKafkaSink.addCustomKafkaSink(process.map(ZGMessage::getRawData), configProperties.getProperty("kafka.id.sourceTopic"),
                configProperties.getProperty("kafka.brokers"), true, "gate-sink");

        try {
            env.execute("etl-pipLine-GateJob");
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
