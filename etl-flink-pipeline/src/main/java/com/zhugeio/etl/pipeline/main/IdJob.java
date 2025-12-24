package com.zhugeio.etl.pipeline.main;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.gate.*;
import com.zhugeio.etl.pipeline.operator.id.*;
import com.zhugeio.etl.pipeline.sink.CustomKafkaSink;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class IdJob {
    private static final Logger logger = LoggerFactory.getLogger(IdJob.class);
    public static void main(String[] args) {
        Properties configProperties = PropertiesUtil.getProperties("config.properties");
        System.out.println("configProperties : "+configProperties);
        logger.info("configProperties : {}",configProperties);
        int asyncCapacity = Integer.parseInt(configProperties.getProperty("async.capacity"));
        String kvrocksHost = configProperties.getProperty("kvrocks.host");
        int kvrocksPort = Integer.parseInt(configProperties.getProperty("kvrocks.port"));
        int maxPropLength = Integer.parseInt(configProperties.getProperty("maxPropLength", "100"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env, configProperties);
        int parallelism = env.getParallelism();  // 提交任务的命令中指定的全局并行度
        logger.info("parallelism : {}",parallelism);

        // 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(configProperties.getProperty("kafka.brokers"))
                .setTopics(configProperties.getProperty("kafka.id.sourceTopic"))
                .setGroupId(configProperties.getProperty("kafka.id.group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 优先用已提交偏移量，没有则从最早开始
                .setProperties(getRateLimitProperties(configProperties))
                .setDeserializer(new ZGMsgSchema())
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> source = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source-id"
        );

        DataStream<ZGMessage> execute = IdJob.execute(source, configProperties, parallelism);
        CustomKafkaSink.addCustomKafkaSink(execute.map(ZGMessage::getRawData), configProperties.getProperty("kafka.dw.sourceTopic"),
                configProperties.getProperty("kafka.brokers"), true, "id-sink");
        try {
            env.execute("etl-pipLine-IdJob");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static DataStream<ZGMessage> execute(DataStream<ZGMessage> source,Properties configProperties,int parallelism){
        int asyncCapacity = Integer.parseInt(configProperties.getProperty("async.capacity"));
        String kvrocksHost = configProperties.getProperty("kvrocks.host");
        int kvrocksPort = Integer.parseInt(configProperties.getProperty("kvrocks.port"));
        int maxPropLength = Integer.parseInt(configProperties.getProperty("maxPropLength", "100"));
        // id : 0. 处理广告 初始化 批次处理前更新部分必须的缓存数据 会影响id模块消费速度
        DataStream<ZGMessage> idStep0;
        String advBussStart = configProperties.getProperty("adv.buss.start");
        if("true".equals(advBussStart)){
            idStep0 = AsyncDataStream.unorderedWait(
                            source,
                            new AdvAsyncOperator(
                                    configProperties.getProperty("adv.redis.host"),
                                    Integer.parseInt(configProperties.getProperty("adv.redis.port")),
                                    Boolean.parseBoolean(configProperties.getProperty("adv.redis.isCluster"))
                            ),
                            Integer.parseInt(configProperties.getProperty("async.timeout.ms")),
                            TimeUnit.MILLISECONDS,
                            Integer.parseInt(configProperties.getProperty("async.thread.num"))
                    ).name("id-advAsyncIO")
                    .uid("id-advAsyncIO")
                    .setParallelism(parallelism);
        }else {
            idStep0 = source;
        }

        // id : 1.检查是否是json格式数据
        DataStream<ZGMessage> idStep1 = idStep0.map(new CheckJsonAndOwnerOperator());

        // id : 2.appId business 设置
        SingleOutputStreamOperator<ZGMessage> idStep2 = AsyncDataStream.unorderedWait(
                        idStep1,
                        new SetAppIdAndBusinessOperator(kvrocksHost, kvrocksPort, true, configProperties),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("id-appId-AsyncIO")
                .uid("id-appId-AsyncIO")
                .setParallelism(parallelism);

        // id : 3.统一Id处理[设备Id 会话Id 用户Id 诸葛Id]
        DataStream<ZGMessage> idStep3 = IdJob.processWithIdAndAsyncIO(idStep2, asyncCapacity, kvrocksHost, kvrocksPort,parallelism);


        // id : 4. 处理广告
        DataStream<ZGMessage> idStep4;
        if("true".equals(advBussStart)){
            // id : 4.1 处理广告 投放四期：app端广告信息存ssdb ip+ua
            SingleOutputStreamOperator<ZGMessage> advSaveAppAdDataStream;
            SingleOutputStreamOperator<ZGMessage> advSaveAppAdDataStream0 = idStep3.flatMap(
                            new AdvSaveAppAdDataFlatMapFunction(configProperties)
                    ).name("advSaveAppAdDataStream")
                    .uid("advSaveAppAdDataStream")
                    .setParallelism(parallelism);
            // 分流 防止 被多次遍历
            final OutputTag<String> advSaveAppAdDataTag = new OutputTag<String>("id-advSaveAppAdDataTag"){};
            // 主数据流
            advSaveAppAdDataStream = advSaveAppAdDataStream0.process(new AdvSaveAppAdDataProcessFunction(advSaveAppAdDataTag));
            // 侧输出 - 广告数据
            SideOutputDataStream<String> advSaveAppAdDataSideOutput = advSaveAppAdDataStream.getSideOutput(advSaveAppAdDataTag);
            CustomKafkaSink.addCustomKafkaSink(advSaveAppAdDataSideOutput, configProperties.getProperty("kafka.adv.sinkTopic"),
                    configProperties.getProperty("kafka.brokers"), true, "id-advSaveAppAdDataSideOutput-sink");

            // id : 4.2 处理广告 投放五期：新增 事件属性（lid）、用户属性(首次、末次)
            SingleOutputStreamOperator<ZGMessage> advLidAndUserFirstEndDataStream;
            SingleOutputStreamOperator<ZGMessage> advLidAndUserFirstEndDataStream0 = advSaveAppAdDataStream0.flatMap(
                            new AdvLidAndUserFirstEndFlatMapFunction(configProperties)
                    ).name("id-advLidAndUserFirstEndDataStream0")
                    .uid("id-advLidAndUserFirstEndDataStream0")
                    .setParallelism(parallelism);
            // 分流 防止 被多次遍历
            final OutputTag<String> advLidAndUserFirstEndTag = new OutputTag<String>("advLidAndUserFirstEndTag"){};
            // 主数据流
            advLidAndUserFirstEndDataStream = advLidAndUserFirstEndDataStream0.process(new AdvLidAndUserFirstEndProcessFunction(advLidAndUserFirstEndTag));
            // 侧输出 - 广告数据
            SideOutputDataStream<String> advLidAndUserFirstEndSideOutput = advLidAndUserFirstEndDataStream.getSideOutput(advLidAndUserFirstEndTag);
            CustomKafkaSink.addCustomKafkaSink(advLidAndUserFirstEndSideOutput, configProperties.getProperty("kafka.adv.sinkTopic"),
                    configProperties.getProperty("kafka.brokers"), true, "id-advLidAndUserFirstEndSideOutput-sink");
            idStep4 = advLidAndUserFirstEndDataStream;
        }else {
            idStep4 = idStep3;
        }


        // id : 5.设备属性处理
        SingleOutputStreamOperator<ZGMessage> idStep5 = AsyncDataStream.unorderedWait(
                        idStep4,
                        new DevicePropertyOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("devicePropProcess-AsyncIO")
                .uid("deviceProp-AsyncIO")
                .setParallelism(parallelism);

        // id : 6.虚拟属性处理
        SingleOutputStreamOperator<ZGMessage> idStep6 = AsyncDataStream.unorderedWait(
                        idStep5,
                        new VirtualPropertyOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("virtualPropProcess-AsyncIO")
                .uid("virtualProp-AsyncIO")
                .setParallelism(parallelism);

        // id : 7.虚拟事件处理
        SingleOutputStreamOperator<ZGMessage> idStep7 = AsyncDataStream.unorderedWait(
                        idStep6,
                        new VirtualEventOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("virtualEventProcess-AsyncIO")
                .uid("virtualEvent-AsyncIO")
                .setParallelism(parallelism);

        // 8.用户属性处理
        SingleOutputStreamOperator<ZGMessage> idStep8 = AsyncDataStream.unorderedWait(
                        idStep7,
                        new UserPropAsyncOperator(kvrocksHost, kvrocksPort, true, configProperties, maxPropLength),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("userPropProcess-AsyncIO")
                .uid("userProp-AsyncIO")
                .setParallelism(parallelism);

        // 9.事件属性处理

        // id : 10 处理广告
        SingleOutputStreamOperator<ZGMessage> idStep10;
        if("true".equals(advBussStart)){
            // id : 10.1 处理广告 投放五期：查询回传表判断是否符合回传行为 (匹配深度回传事件并发kafka)
            SingleOutputStreamOperator<ZGMessage> advConvertEventDataStream0 = idStep8.flatMap(
                            new AdvConvertEventFlatMapFunction(configProperties)
                    ).name("advConvertEventDataStream0")
                    .uid("advConvertEventDataStream0")
                    .setParallelism(parallelism);
            // 分流 防止 被多次遍历
            final OutputTag<String> advConvertEventTag = new OutputTag<String>("id-advConvertEventTag"){};
            final OutputTag<String> advConvertEventUserTag = new OutputTag<String>("id-advConvertEventUserTag"){};
            // 主数据
            SingleOutputStreamOperator<ZGMessage> advConvertEventDataStream = advConvertEventDataStream0.process(new AdvConvertEventProcessFunction(advConvertEventTag, advConvertEventUserTag));
            // 侧输出 - 广告数据
            SideOutputDataStream<String> advConvertEventSideOutput = advConvertEventDataStream.getSideOutput(advConvertEventTag);
            CustomKafkaSink.addCustomKafkaSink(
                    advConvertEventSideOutput,
                    configProperties.getProperty("kafka.adv.sinkTopic"),
                    configProperties.getProperty("kafka.brokers"),
                    true,
                    "id-advConvertEventSideOutput-sink"
            );
            // 侧输出 - 广告数据 - 用户
            SideOutputDataStream<String> advConvertEventUserSideOutput = advConvertEventDataStream.getSideOutput(advConvertEventUserTag);
            CustomKafkaSink.addCustomKafkaSink(
                    advConvertEventUserSideOutput,
                    configProperties.getProperty("kafka.adv.user.sinkTopic"),
                    configProperties.getProperty("kafka.brokers"),
                    true,
                    "id-advConvertEventUserSideOutput-sink"
            );
            idStep10 = advConvertEventDataStream;
        }else {
            idStep10 = idStep8;
        }

        // 11.输出主数据流和数据质量
        // 11.1 数据质量分流
        final OutputTag<String> dataQualityTag4id = new OutputTag<String>("dataQualityTag4id"){};
        SingleOutputStreamOperator<ZGMessage> idResultStream = idStep10.process(new IdResultProcessFunction(dataQualityTag4id));
        SideOutputDataStream<String> idResultStreamSideOutput = idResultStream.getSideOutput(dataQualityTag4id);
        CustomKafkaSink.addCustomKafkaSink(idResultStreamSideOutput, configProperties.getProperty("kafka.quality.topic"),
                configProperties.getProperty("kafka.brokers"), true, "id-data-quality-sink");
        // 11.2 主数据返回
        return idResultStream;
    }

    public static DataStream<ZGMessage> processWithIdAndAsyncIO(
            DataStream<ZGMessage> source,
            int capacity,
            String kvrocksHost,
            int kvrocksPort,
            int parallelism
            ) {

        System.out.println("📊 使用流式异步处理（AsyncIO + 真实KVRocks）\n");

        // 1. 设备ID映射
        SingleOutputStreamOperator<ZGMessage> withDeviceId = AsyncDataStream.unorderedWait(
                        source,
                        new DeviceIdAsyncOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("DeviceId-AsyncIO")
                .uid("device-id-async")
                .setParallelism(parallelism);

        // 2. 会话ID处理
        DataStream<ZGMessage> withSessionId = withDeviceId
                .process(new SessionIdProcessOperator())
                .name("SessionId-Process")
                .uid("session-id-process")
                .setParallelism(parallelism);

        // 3. 用户ID映射
        SingleOutputStreamOperator<ZGMessage> withUserId = AsyncDataStream.unorderedWait(
                        withSessionId,
                        new UserIdAsyncOperator(kvrocksHost, kvrocksPort),
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("UserId-AsyncIO")
                .uid("user-id-async")
                .setParallelism(parallelism);

        // 4. 诸葛ID映射
        SingleOutputStreamOperator<ZGMessage> withZgid = AsyncDataStream.unorderedWait(
                        withUserId,
                        new ZgidAsyncOperator(kvrocksHost, kvrocksPort),
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("Zgid-AsyncIO")
                .uid("zgid-async")
                .setParallelism(parallelism);

        return withZgid;
    }

    /**
     * 专门设置速率限制属性的方法
     */
    private static Properties getRateLimitProperties(Properties configProperties) {
        Properties props = new Properties();
        // 每个分区fetch最大字节
        props.setProperty("max.partition.fetch.bytes", configProperties.getProperty("kafka.max.partition.fetch.bytes"));
        // 每次poll最大记录数
        props.setProperty("max.poll.records", configProperties.getProperty("kafka.max.partition.fetch.bytes"));
        // 服务器等待时间
        props.setProperty("fetch.max.wait.ms", configProperties.getProperty("kafka.fetch.max.wait.ms"));
        return props;
    }

    public static void initCheckpoint(StreamExecutionEnvironment env, Properties properties){
        env.enableCheckpointing(30 * 1000L);
        env.getCheckpointConfig().setCheckpointInterval(30 * 1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        stateBackend.setDbStoragePath("hdfs:///user/flink/checkpoints/"+properties.getProperty("checkpoint.id.path"));
        env.setStateBackend(stateBackend);
    }
}