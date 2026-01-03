package com.zhugeio.etl.pipeline.main;

import com.alibaba.fastjson.JSON;
import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.gate.GateFlatMapFunction;
import com.zhugeio.etl.pipeline.operator.gate.GateProcessFunction;
import com.zhugeio.etl.pipeline.sink.CustomKafkaSink;
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

import java.util.Properties;

/**
 * Gate处理主任务
 * 
 * 处理流程:
 * 1. 数据清洗
 * 2. 格式校验
 * 3. 数据质量分流
 * 4. Debug分流
 * 
 * 配置项:
 * - gate.sink.to.downstream: 是否输出到下游主Topic (默认true)
 * 
 * 使用方式:
 * 1. 单独运行: GateJob.main() - gate.sink.to.downstream=true
 * 2. 与 AllJob 串联: 设置 gate.sink.to.downstream=false
 */
public class GateJob {

    private static final Logger LOG = LoggerFactory.getLogger(GateJob.class);

    // 数据质量侧输出标签
    public static final OutputTag<String> DATA_QUALITY_TAG = new OutputTag<String>("dataQualityTag4gate") {};
    public static final OutputTag<String> DEBUG_TAG = new OutputTag<String>("debugTag4gate") {};

    // ==================== Main 入口 ====================

    public static void main(String[] args) {
        LOG.info("Gate ETL Pipeline 启动...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env);

        LOG.info("parallelism: {}", env.getParallelism());

        // 执行 Gate 处理流程
        execute(env);

        try {
            env.execute("etl-pipeline-GateJob");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== 核心方法 ====================

    /**
     * 执行 Gate 完整流程 (Pipeline + Sink)
     * 
     * @param env Flink 执行环境
     * @return 处理后的数据流
     */
    public static DataStream<ZGMessage> execute(StreamExecutionEnvironment env) {
        // 1. 构建流水线
        SingleOutputStreamOperator<ZGMessage> result = buildGatePipeline(env);
        
        // 2. 添加 Sink
        addSinks(result);
        
        return result;
    }

    /**
     * 构建 Gate 流水线 (不包含 Sink)
     * 
     * @param env Flink 执行环境
     * @return 处理后的数据流
     */
    public static SingleOutputStreamOperator<ZGMessage> buildGatePipeline(StreamExecutionEnvironment env) {
        // 1. 创建 KafkaSource
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(Config.getString(Config.KAFKA_BROKERS))
                .setTopics(Config.getString(Config.KAFKA_GATE_SOURCE_TOPIC))
                .setGroupId(Config.getString(Config.KAFKA_GATE_GROUP_ID))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperties(Config.getKafkaConsumerProps())
                .setDeserializer(new ZGMsgSchema())
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-source-gate"
        ).uid("gate-kafka-source");

        // 3. 处理数据
        SingleOutputStreamOperator<ZGMessage> gateStep1 = sourceStream
                .flatMap(new GateFlatMapFunction())
                .name("gate-flatMap")
                .uid("gate-flatMap");

        // 4. 分流处理
        SingleOutputStreamOperator<ZGMessage> gateStep2 = gateStep1
                .process(new GateProcessFunction(DATA_QUALITY_TAG, DEBUG_TAG))
                .name("gate-process")
                .uid("gate-process");

        LOG.info("Gate Pipeline 构建完成");
        return gateStep2;
    }

    /**
     * 添加 Sink
     * 
     * @param result 处理后的数据流
     */
    public static void addSinks(SingleOutputStreamOperator<ZGMessage> result) {
        // 从配置获取是否输出到下游主Topic (默认true)
        boolean sinkToDownstream = Config.getBoolean(Config.GATE_SINK_TO_DOWNSTREAM, true);

        // 主流输出到下游 (ID Job)
        if (sinkToDownstream) {
            CustomKafkaSink.addCustomKafkaSink(
                    result.map(msg -> JSON.toJSONString(msg.getData())),
                    Config.getString(Config.KAFKA_ID_SOURCE_TOPIC),
                    Config.getString(Config.KAFKA_BROKERS),
                    true,
                    "gate-sink"
            );
            LOG.info("Gate 主流输出到下游 Topic: {}", Config.getString(Config.KAFKA_ID_SOURCE_TOPIC));
        }

        // 数据质量侧输出
        DataStream<String> dqSideOutput = result.getSideOutput(DATA_QUALITY_TAG);
        CustomKafkaSink.addCustomKafkaSink(
                dqSideOutput,
                Config.getString(Config.DQ_KAFKA_TOPIC),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "gate-data-quality-sink"
        );

        // Debug侧输出
        DataStream<String> debugSideOutput = result.getSideOutput(DEBUG_TAG);
        CustomKafkaSink.addCustomKafkaSink(
                debugSideOutput,
                Config.getString("kafka.debug.topic"),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "gate-debug-sink"
        );

        LOG.info("Gate Sink 配置完成, sinkToDownstream={}", sinkToDownstream);
    }

    // ==================== 辅助方法 ====================

    /**
     * 初始化 Checkpoint 配置
     */
    public static void initCheckpoint(StreamExecutionEnvironment env) {
        int checkpointInterval = Config.getInt("checkpoint.gate.interval", 30000);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 设置Checkpoint存储
        String checkpointPath = "hdfs:///user/flink/checkpoints/" + Config.getString("checkpoint.gate.path", "gate_job");
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
    }

    /**
     * 速率限制配置
     */
    private static Properties getRateLimitProperties() {
        Properties props = new Properties();
        props.setProperty("max.partition.fetch.bytes", Config.getString(Config.KAFKA_MAX_PARTITION_FETCH_BYTES, "1048576"));
        props.setProperty("max.poll.records", Config.getString(Config.KAFKA_MAX_POLL_RECORDS, "500"));
        props.setProperty("fetch.max.wait.ms", Config.getString(Config.KAFKA_FETCH_MAX_WAIT_MS, "500"));
        return props;
    }
}
