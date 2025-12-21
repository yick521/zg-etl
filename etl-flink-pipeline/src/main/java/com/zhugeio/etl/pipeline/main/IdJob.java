package com.zhugeio.etl.pipeline.main;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.id.*;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
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

        // 1. 创建 KafkaSource，使用自定义反序列化模式获取分区信息
        KafkaSource<ZGMessage> kafkaSource = KafkaSource.<ZGMessage>builder()
                .setBootstrapServers(configProperties.getProperty("kafka.brokers"))
                .setTopics(configProperties.getProperty("kafka.id.sourceTopic"))
                .setGroupId(configProperties.getProperty("kafka.id.group.id"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperties(getRateLimitProperties(configProperties))
                .setDeserializer(new ZGMsgSchema())
                .build();

        // 2. 创建数据流
        DataStream<ZGMessage> source = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source-Dw"
        );

        final OutputTag<JSONObject> dataDwTag = new OutputTag<JSONObject>("data-Dw") {};

        // 1.检查是否是json格式数据
        DataStream<ZGMessage> step1 = source.map(new CheckJsonAndOwnerOperator());

        // 2.检查数据满足需求(文件夹 :/basicSchema.json)
        DataStream<ZGMessage> step2 = step1.map(new CheckBasicSchemaOperator());

        // 3.appId business 设置
        SingleOutputStreamOperator<ZGMessage> step3 = AsyncDataStream.unorderedWait(
                        step2,
                        new SetAppIdAndBusinessOperator(kvrocksHost, kvrocksPort, true, configProperties),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("appId-AsyncIO")
                .uid("appId-async")
                .setParallelism(4);

        // 3.统一Id处理[设备Id 会话Id 用户Id 诸葛Id]
        DataStream<ZGMessage> step4 = processWithIdAndAsyncIO(step3, asyncCapacity, kvrocksHost, kvrocksPort);

        // 4.设备属性处理
        SingleOutputStreamOperator<ZGMessage> step5 = AsyncDataStream.unorderedWait(
                        step4,
                        new DevicePropertyOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("devicePropProcess-AsyncIO")
                .uid("deviceProp-AsyncIO")
                .setParallelism(4);

        // 5.虚拟属性处理
        SingleOutputStreamOperator<ZGMessage> step6 = AsyncDataStream.unorderedWait(
                        step5,
                        new VirtualPropertyOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("virtualPropProcess-AsyncIO")
                .uid("virtualProp-AsyncIO")
                .setParallelism(4);

        // 6.虚拟事件处理
        SingleOutputStreamOperator<ZGMessage> step7 = AsyncDataStream.unorderedWait(
                step6,
                new VirtualEventOperator(kvrocksHost, kvrocksPort, true),
                5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("virtualEventProcess-AsyncIO")
                .uid("virtualEvent-AsyncIO")
                .setParallelism(4);

        // 7.用户属性处理
        SingleOutputStreamOperator<ZGMessage> step8 = AsyncDataStream.unorderedWait(
                step7,
                new UserPropAsyncOperator(kvrocksHost, kvrocksPort, true, configProperties, maxPropLength),
                5000, TimeUnit.MILLISECONDS, asyncCapacity
                ).name("userPropProcess-AsyncIO")
                .uid("userProp-AsyncIO")
                .setParallelism(4);

        // 8.事件属性处理

        try {
            env.execute("test");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static DataStream<ZGMessage> processWithIdAndAsyncIO(
            DataStream<ZGMessage> source,
            int capacity,
            String kvrocksHost,
            int kvrocksPort) {

        System.out.println("📊 使用流式异步处理（AsyncIO + 真实KVRocks）\n");

        // 1. 设备ID映射
        SingleOutputStreamOperator<ZGMessage> withDeviceId = AsyncDataStream.unorderedWait(
                        source,
                        new DeviceIdAsyncOperator(kvrocksHost, kvrocksPort, true),
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("DeviceId-AsyncIO")
                .uid("device-id-async")
                .setParallelism(4);

        // 2. 会话ID处理
        DataStream<ZGMessage> withSessionId = withDeviceId
                .process(new SessionIdProcessOperator())
                .name("SessionId-Process")
                .uid("session-id-process")
                .setParallelism(4);

        // 3. 用户ID映射
        SingleOutputStreamOperator<ZGMessage> withUserId = AsyncDataStream.unorderedWait(
                        withSessionId,
                        new UserIdAsyncOperator(kvrocksHost, kvrocksPort),
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("UserId-AsyncIO")
                .uid("user-id-async")
                .setParallelism(4);

        // 4. 诸葛ID映射
        SingleOutputStreamOperator<ZGMessage> withZgid = AsyncDataStream.unorderedWait(
                        withUserId,
                        new ZgidAsyncOperator(kvrocksHost, kvrocksPort),
                        5000, TimeUnit.MILLISECONDS, capacity
                ).name("Zgid-AsyncIO")
                .uid("zgid-async")
                .setParallelism(4);

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