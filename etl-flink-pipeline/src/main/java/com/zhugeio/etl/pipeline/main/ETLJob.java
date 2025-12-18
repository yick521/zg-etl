package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.gate.*;
import com.zhugeio.etl.pipeline.sink.CustomKafkaSink;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author ningjh
 * @name GateJob
 * @date 2025/11/28
 * @description
 */
public class ETLJob {
    private static final Logger logger = LoggerFactory.getLogger(ETLJob.class);
    public static void main(String[] args) {
        // 读取配置
        Properties config = PropertiesUtil.getProperties("config.properties");
        logger.info("config : {}",config);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env,config);
        int parallelism = env.getParallelism();  // 提交任务的命令中指定的全局并行度
        logger.info("parallelism : {}",parallelism);

        // gate : 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(config.getProperty("kafka.brokers"))
                .setTopics(config.getProperty("kafka.gate.sourceTopic"))
                .setGroupId(config.getProperty("kafka.gate.group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 优先用已提交偏移量，没有则从最早开始
                .setProperties(getRateLimitProperties(config)) // 关键：添加 Kafka Consumer 配置来控制消费速率
                .setDeserializer(new ZGMsgSchema()) // 关键：解析POJO
                .build();

        // gate : 2. 创建数据流
        DataStream<ZGMessage> stream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-source-gate"
        ).uid("gate-kafka-source");

        // gate : 3.处理数据
        SingleOutputStreamOperator<ZGMessage> gateFlatMapStream = stream.flatMap(new GateFlatMapFunction(config)).setParallelism(parallelism);

        // gate : 4. 创建侧输出流-数据质量、debug数据
        final OutputTag<String> dataQualityTag4gate = new OutputTag<String>("dataQualityTag4gate"){};
        final OutputTag<String> debugTag4gate = new OutputTag<String>("debugTag4gate"){};

        // gate : 5.分流 防止 gateFlatMapStream 被多次遍历
        SingleOutputStreamOperator<ZGMessage> process = gateFlatMapStream.process(new GateProcessFunction(dataQualityTag4gate, debugTag4gate));

        // gate : 6.1 侧输出 - 数据质量
        DataStream<String> sideOutput4dataQuality = process.getSideOutput(dataQualityTag4gate);
        CustomKafkaSink.addCustomKafkaSink(sideOutput4dataQuality, config.getProperty("kafka.quality.topic"),
                config.getProperty("kafka.brokers"), true, "gate-data-quality-sink");

        // gate : 6.2 侧输出 - debug
        DataStream<String> sideOutput4debug = process.getSideOutput(debugTag4gate);
        CustomKafkaSink.addCustomKafkaSink(sideOutput4debug, config.getProperty("kafka.debug.topic"),
                config.getProperty("kafka.brokers"), true, "gate-debug-sink");

        // id : 0. 处理广告 初始化 批次处理前更新部分必须的缓存数据 会影响id模块消费速度
        SingleOutputStreamOperator<ZGMessage> advStream;
        String advBussStart = config.getProperty("adv.buss.start");
        if("true".equals(advBussStart)){
            advStream = AsyncDataStream.unorderedWait(
                            process,
                            new AdvAsyncOperator(
                                    config.getProperty("adv.redis.host"),
                                    Integer.parseInt(config.getProperty("adv.redis.port")),
                                    Boolean.parseBoolean(config.getProperty("adv.redis.isCluster"))
                            ),
                            Integer.parseInt(config.getProperty("async.timeout.ms")),
                            TimeUnit.MILLISECONDS,
                            Integer.parseInt(config.getProperty("async.thread.num"))
                    ).name("advAsyncIO")
                    .uid("advAsyncIO")
                    .setParallelism(parallelism);
        }else {
            advStream = process;
        }

        // id : 8.1 处理广告 投放四期：app端广告信息存ssdb ip+ua
        SingleOutputStreamOperator<ZGMessage> advSaveAppAdDataStream;
        if("true".equals(advBussStart)){
            SingleOutputStreamOperator<ZGMessage> advSaveAppAdDataStream0 = advStream.flatMap(
                        new AdvSaveAppAdDataFlatMapFunction(config)
                    ).name("advSaveAppAdDataStream")
                    .uid("advSaveAppAdDataStream")
                    .setParallelism(parallelism);
            // 分流 防止 被多次遍历
            final OutputTag<String> advSaveAppAdDataTag = new OutputTag<String>("advSaveAppAdDataTag"){};
            advSaveAppAdDataStream = advSaveAppAdDataStream0.process(new AdvSaveAppAdDataProcessFunction(advSaveAppAdDataTag));
            // 侧输出 - 广告数据
            SideOutputDataStream<String> advSaveAppAdDataSideOutput = advSaveAppAdDataStream.getSideOutput(advSaveAppAdDataTag);
            CustomKafkaSink.addCustomKafkaSink(advSaveAppAdDataSideOutput, config.getProperty("kafka.adv.sinkTopic"),
                    config.getProperty("kafka.brokers"), true, "advSaveAppAdDataSideOutput-sink");
        }else {
            advSaveAppAdDataStream = process;
        }

        // id : 8.2 处理广告 投放五期：新增 事件属性（lid）、用户属性(首次、末次)
        SingleOutputStreamOperator<ZGMessage> advLidAndUserFirstEndDataStream;
        if("true".equals(advBussStart)){
            SingleOutputStreamOperator<ZGMessage> advLidAndUserFirstEndDataStream0 = advStream.flatMap(
                            new AdvLidAndUserFirstEndFlatMapFunction(config)
                    ).name("advLidAndUserFirstEndDataStream0")
                    .uid("advLidAndUserFirstEndDataStream0")
                    .setParallelism(parallelism);
            // 分流 防止 被多次遍历
            final OutputTag<String> advLidAndUserFirstEndTag = new OutputTag<String>("advLidAndUserFirstEndTag"){};
            advLidAndUserFirstEndDataStream = advLidAndUserFirstEndDataStream0.process(new AdvLidAndUserFirstEndProcessFunction(advLidAndUserFirstEndTag));
            // 侧输出 - 广告数据
            SideOutputDataStream<String> advLidAndUserFirstEndSideOutput = advLidAndUserFirstEndDataStream.getSideOutput(advLidAndUserFirstEndTag);
            CustomKafkaSink.addCustomKafkaSink(advLidAndUserFirstEndSideOutput, config.getProperty("kafka.adv.sinkTopic"),
                    config.getProperty("kafka.brokers"), true, "advLidAndUserFirstEndSideOutput-sink");
        }else {
            advLidAndUserFirstEndDataStream = advSaveAppAdDataStream;
        }

        // id : 13.1 处理广告 投放五期：查询回传表判断是否符合回传行为 (匹配深度回传事件并发kafka)
        SingleOutputStreamOperator<ZGMessage> advConvertEventDataStream;
        if("true".equals(advBussStart)){
            SingleOutputStreamOperator<ZGMessage> advConvertEventDataStream0 = advSaveAppAdDataStream.flatMap(
                    new AdvConvertEventFlatMapFunction(config)
                    ).name("advConvertEventDataStream0")
                    .uid("advConvertEventDataStream0")
                    .setParallelism(parallelism);
            // 分流 防止 被多次遍历
            final OutputTag<String> advConvertEventTag = new OutputTag<String>("advConvertEventTag"){};
            final OutputTag<String> advConvertEventUserTag = new OutputTag<String>("advConvertEventUserTag"){};
            advConvertEventDataStream = advConvertEventDataStream0.process(new AdvConvertEventProcessFunction(advConvertEventTag, advConvertEventUserTag));
            // 侧输出 - 广告数据
            SideOutputDataStream<String> advConvertEventSideOutput = advConvertEventDataStream.getSideOutput(advConvertEventTag);
            CustomKafkaSink.addCustomKafkaSink(advConvertEventSideOutput, config.getProperty("kafka.adv.sinkTopic"),
                    config.getProperty("kafka.brokers"), true, "advConvertEventSideOutput-sink");
            // 侧输出 - 广告数据 - 用户
            SideOutputDataStream<String> advConvertEventUserSideOutput = advConvertEventDataStream.getSideOutput(advConvertEventUserTag);
            CustomKafkaSink.addCustomKafkaSink(advConvertEventSideOutput, config.getProperty("kafka.adv.user.sinkTopic"),
                    config.getProperty("kafka.brokers"), true, "advConvertEventUserSideOutput-sink");



        }else {
            advConvertEventDataStream = advSaveAppAdDataStream;
        }


        // id : 13.2 处理广告 投放四期 根据事件id为事件属性添加utm数据：由原dw模块迁移过来
        SingleOutputStreamOperator<ZGMessage> advAddUtmDataStream;
        if("true".equals(advBussStart)){
            advAddUtmDataStream = advConvertEventDataStream.flatMap(new AdvAddUtmFlatMapFunction(config)).name("advAddUtmDataStream")
                    .uid("advAddUtmDataStream")
                    .setParallelism(parallelism);;
        }else {
            advAddUtmDataStream = advConvertEventDataStream;
        }
        advAddUtmDataStream.print();




        try {
            env.execute("etl-pipLine-AllJob");
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
     * @param config
     * @return
     */
    private static Properties getRateLimitProperties(Properties config) {
        Properties props = new Properties();
        // 每个分区fetch最大字节
        props.setProperty("max.partition.fetch.bytes", config.getProperty("kafka.max.partition.fetch.bytes"));
        // 每次poll最大记录数
        props.setProperty("max.poll.records", config.getProperty("kafka.max.poll.records"));
        // 服务器等待时间
        props.setProperty("fetch.max.wait.ms", config.getProperty("kafka.fetch.max.wait.ms"));
        return props;
    }
}
