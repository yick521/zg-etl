package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.gate.GateProcessWindowsFunction;
import com.zhugeio.etl.pipeline.sink.CustomKafkaSink;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * @author ningjh
 * @name GateJob
 * @date 2025/11/28
 * @description
 */
public class WindowGateJob {
    private static final Logger logger = LoggerFactory.getLogger(WindowGateJob.class);
    public static void main(String[] args) {
        // 读取配置
        Properties configProperties = PropertiesUtil.getProperties("config.properties");
        System.out.println("configProperties : "+configProperties);
        logger.info("configProperties : {}",configProperties);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env, configProperties);
        int parallelism = env.getParallelism();  // 命令行指定的并行度
        logger.info("parallelism : {}",parallelism);

        // 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(configProperties.getProperty("kafka.brokers"))
                .setTopics(configProperties.getProperty("kafka.gate.sourceTopic"))
                .setGroupId(configProperties.getProperty("kafka.gate.group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 优先用已提交偏移量，没有则从最早开始
                // 关键：添加 Kafka Consumer 配置来控制消费速率
                .setProperties(getRateLimitProperties(configProperties))
                // 关键：解析POJO
                .setDeserializer(new ZGMsgSchema())
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-source-gate-1"
        ).uid("gate-kafka-source");

        // 3. 创建侧输出流
        final OutputTag<String> dataQualityTag = new OutputTag<String>("dataQualityTag4gate"){};
        final OutputTag<String> debugTag = new OutputTag<String>("debugTag4gate"){};

        //  4. 分组
        KeyedStream<ZGMessage, Integer> keyedStream = stream.keyBy(new KeySelector<ZGMessage, Integer>() {
            @Override
            public Integer getKey(ZGMessage zgMessage) throws Exception {
                return (int) Math.floorMod(zgMessage.getOffset(),128);
            }
        });
        // 5. 窗口
        WindowedStream<ZGMessage, Integer, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(Integer.parseInt(configProperties.getProperty("windows.gate.interval")))));
        // 6. 处理
        SingleOutputStreamOperator<ZGMessage> process = windowedStream.process(new GateProcessWindowsFunction(dataQualityTag,debugTag,configProperties)).uid("gate-process-function").setParallelism(parallelism);

        // 7. 侧输出 - 数据质量
        DataStream<String> sideOutput4dataQuality = process.getSideOutput(dataQualityTag);
        CustomKafkaSink.addCustomKafkaSink(
                sideOutput4dataQuality,
                configProperties.getProperty("kafka.quality.topic"),
                configProperties.getProperty("kafka.brokers"),
                true,
                "gate-data-quality-sink"
        );

        // 8. 侧输出 - debug
        DataStream<String> sideOutput4debug = process.getSideOutput(debugTag);
        CustomKafkaSink.addCustomKafkaSink(
                sideOutput4debug,
                configProperties.getProperty("kafka.debug.topic"),
                configProperties.getProperty("kafka.brokers"),
                true,
                "gate-debug-sink"
        );

        // 9.主流输出到下游
        CustomKafkaSink.addCustomKafkaSink(
                process.map(ZGMessage::getRawData),
                configProperties.getProperty("kafka.id.sourceTopic"),
                configProperties.getProperty("kafka.brokers"),
                true,
                "gate-sink"
        );

        try {
            env.execute("etl-pipLine-gateJob");
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
