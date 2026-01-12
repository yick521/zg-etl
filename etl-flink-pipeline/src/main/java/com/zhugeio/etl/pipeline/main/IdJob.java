package com.zhugeio.etl.pipeline.main;

import com.alibaba.fastjson.JSON;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.config.FlinkEnvConfig;
import com.zhugeio.etl.common.source.KafkaSourceBuilder;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.gate.*;
import com.zhugeio.etl.pipeline.operator.id.*;
import com.zhugeio.etl.pipeline.sink.CustomKafkaSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * ID处理主任务
 * 
 * 处理流程:
 * 1. JSON校验和owner设置
 * 2. AppId和Business设置
 * 3. 统一ID处理 (设备ID → 会话ID → 用户ID → 诸葛ID)
 * 4. 设备属性处理
 * 5. 虚拟属性处理
 * 6. 虚拟事件处理
 * 7. 事件ID处理
 * 8. 事件属性处理
 * 9. 用户属性处理
 * 10. 结果输出 (数据质量分流)
 * 
 * 配置项:
 * - id.sink.to.downstream: 是否输出到下游主Topic (默认true)
 * 
 * 使用方式:
 * 1. 单独运行: IdJob.main() - id.sink.to.downstream=true
 * 2. 与 AllJob 串联: 设置 id.sink.to.downstream=false
 */
public class IdJob {

    private static final Logger LOG = LoggerFactory.getLogger(IdJob.class);
    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";

    // 数据质量侧输出标签
    public static final OutputTag<String> DATA_QUALITY_TAG = new OutputTag<String>("dataQualityTag4id") {};

    // ==================== Main 入口 ====================

    public static void main(String[] args) throws Exception {
        LOG.info("ID ETL Pipeline 启动...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint 配置
        String checkpointPath = CHECKPOINT_BASE + Config.getString(Config.CHECKPOINT_ID_PATH, "id_job");
        FlinkEnvConfig.configureCheckpoint(env, Config.getLong(Config.FLINK_CHECKPOINT_INTERVAL_MS, 30000L), checkpointPath);

        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointTimeout(120000L);

        LOG.info("parallelism: {}", env.getParallelism());

        // Kafka Source
        DataStream<ZGMessage> source = env.fromSource(
                KafkaSourceBuilder.<ZGMessage>builder()
                        .brokers(Config.getString(Config.KAFKA_BROKERS))
                        .topic(Config.getString(Config.KAFKA_ID_SOURCE_TOPIC))
                        .groupId(Config.getString(Config.KAFKA_ID_GROUP_ID))
                        .consumerProps(Config.getKafkaConsumerProps())
                        .deserializer(new ZGMsgSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource"
        ).uid("kafka-source");

        // 执行 ID 处理流程
        SingleOutputStreamOperator<ZGMessage> result = execute(source);

        // 2. 添加 Sink
        addSinks(result);

        env.execute("ID-ETL-Pipeline");
    }

    // ==================== 核心方法 ====================

    /**
     * 执行 ID 完整流程 (Pipeline + Sink)
     * 
     * @param source 输入数据流
     * @return 处理后的数据流
     */
    public static SingleOutputStreamOperator<ZGMessage> execute(DataStream<ZGMessage> source) {
        // 1. 构建流水线
        SingleOutputStreamOperator<ZGMessage> result = buildIdPipeline(source);
        return result;
    }

    /**
     * 构建 ID 流水线 (不包含 Sink)
     * 
     * @param source 输入数据流
     * @return 处理后的数据流
     */
    public static SingleOutputStreamOperator<ZGMessage> buildIdPipeline(DataStream<ZGMessage> source) {
        // 从 Config 获取配置
        int asyncCapacity = Config.getInt("async.capacity", 300);
        int asyncTimeoutMs = Config.getInt("async.timeout.ms", 5000);
        String kvrocksHost = Config.getString(Config.KVROCKS_HOST, "localhost");
        int kvrocksPort = Config.getInt(Config.KVROCKS_PORT, 6379);
        boolean kvrocksCluster = Config.getBoolean(Config.KVROCKS_CLUSTER, true);
        int maxPropLength = Config.getInt(Config.MAX_PROP_LENGTH, 100);
        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .kvrocksTimeout(asyncTimeoutMs)
                .build();

        // 广告业务开关
        boolean advBussStart = Config.getBoolean("adv.buss.start", false);
        LOG.info("广告业务开关: {}", advBussStart);

        // ========== Step 0: 广告初始化 (可选) ==========
        DataStream<ZGMessage> idStep0;
        if (advBussStart) {
            idStep0 = AsyncDataStream.unorderedWait(
                    source,
                    new AdvAsyncOperator(
                            Config.getString("adv.redis.host"),
                            Config.getInt("adv.redis.port", 6379),
                            Config.getBoolean("adv.redis.isCluster", false)
                    ),
                    asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
            ).name("id-advAsyncIO").uid("id-advAsyncIO");
        } else {
            idStep0 = source;
        }

        // ========== Step 1: JSON校验和owner设置 ==========
        DataStream<ZGMessage> idStep1 = idStep0
                .map(new CheckJsonAndOwnerOperator())
                .name("id-checkJson")
                .uid("id-checkJson");

//        // ========== Step 1.5: BasicSchema校验  ==========
//        DataStream<ZGMessage> idStep1_5 = idStep1
//                .map(new CheckBasicSchemaOperator())
//                .name("id-checkBasicSchema")
//                .uid("id-checkBasicSchema");

        // ========== Step 2: AppId和Business设置 ==========
        SingleOutputStreamOperator<ZGMessage> idStep2 = AsyncDataStream.unorderedWait(
                idStep1,
                new SetAppIdAndBusinessOperator(cacheConfig, Config.getRdbmsProperties()),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("id-appId-AsyncIO").uid("id-appId-AsyncIO");

        // ========== Step 3: 统一ID处理 (设备ID → 会话ID → 用户ID → 诸葛ID) ==========
        DataStream<ZGMessage> idStep3 = buildIdMappingPipeline(
                idStep2, asyncCapacity, asyncTimeoutMs, kvrocksHost, kvrocksPort, kvrocksCluster);

        // ========== Step 4: 广告处理 (可选) ==========
        DataStream<ZGMessage> idStep4;
        if (advBussStart) {
            idStep4 = buildAdvBusinessPipeline(idStep3);
        } else {
            idStep4 = idStep3;
        }

        // ========== Step 5: 设备属性处理 ==========
        SingleOutputStreamOperator<ZGMessage> idStep5 = AsyncDataStream.unorderedWait(
                idStep4,
                new DevicePropertyOperator(cacheConfig),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("deviceProp-AsyncIO").uid("deviceProp-AsyncIO");

        // ========== Step 6: 虚拟属性处理 ==========
        SingleOutputStreamOperator<ZGMessage> idStep6 = AsyncDataStream.unorderedWait(
                idStep5,
                new VirtualPropertyOperator(cacheConfig),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("virtualProp-AsyncIO").uid("virtualProp-AsyncIO");

        // ========== Step 7: 虚拟事件处理 ==========
        SingleOutputStreamOperator<ZGMessage> idStep7 = AsyncDataStream.unorderedWait(
                idStep6,
                new VirtualEventOperator(cacheConfig),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("virtualEvent-AsyncIO").uid("virtualEvent-AsyncIO");

        // ========== Step 8: 事件ID处理 ==========
        SingleOutputStreamOperator<ZGMessage> idStep8 = AsyncDataStream.unorderedWait(
                idStep7,
                new EventAsyncOperator(cacheConfig, Config.getRdbmsProperties(), maxPropLength),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("event-AsyncIO").uid("event-AsyncIO");

        // ========== Step 9: 事件属性处理 ==========
        SingleOutputStreamOperator<ZGMessage> idStep9 = AsyncDataStream.unorderedWait(
                idStep8,
                new EventAttrAsyncOperator(cacheConfig, Config.getRdbmsProperties(), maxPropLength, 60),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("eventAttr-AsyncIO").uid("eventAttr-AsyncIO");

        // ========== Step 10: 用户属性处理 ==========
        SingleOutputStreamOperator<ZGMessage> idStep10 = AsyncDataStream.unorderedWait(
                idStep9,
                new UserPropAsyncOperator(cacheConfig, Config.getRdbmsProperties(), maxPropLength),
                asyncTimeoutMs, TimeUnit.MILLISECONDS, asyncCapacity
        ).name("userProp-AsyncIO").uid("userProp-AsyncIO");

        // ========== Step 11: 广告回传处理 (可选) ==========
        SingleOutputStreamOperator<ZGMessage> idStep11;
        if (advBussStart) {
            idStep11 = buildAdvConvertPipeline(idStep10);
        } else {
            idStep11 = idStep10;
        }

        // ========== Step 12: 结果和数据质量  分流 ==========
        SingleOutputStreamOperator<ZGMessage> idResultStream = idStep11
                .process(new IdResultProcessFunction(DATA_QUALITY_TAG))
                .name("id-result")
                .uid("id-result");

        // 数据质量侧输出
        SideOutputDataStream<String> dqSideOutput = idResultStream.getSideOutput(DATA_QUALITY_TAG);
        CustomKafkaSink.addCustomKafkaSink(
                dqSideOutput,
                Config.getString(Config.DQ_KAFKA_TOPIC),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "id-data-quality-sink"
        );

        LOG.info("ID Pipeline 构建完成");
        return idResultStream;
    }

    /**
     * 添加 Sink
     * 
     * @param result 处理后的数据流
     */
    public static void addSinks(SingleOutputStreamOperator<ZGMessage> result) {
        CustomKafkaSink.addCustomKafkaSink(
                result.map(msg -> JSON.toJSONString(msg.getData())),
                Config.getString(Config.KAFKA_DW_SOURCE_TOPIC),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "id-sink"
        );
        LOG.info("ID 主流输出到下游 Topic: {}", Config.getString(Config.KAFKA_DW_SOURCE_TOPIC));
    }

    // ==================== 内部构建方法 ====================

    /**
     * 统一ID处理流程
     */
    private static DataStream<ZGMessage> buildIdMappingPipeline(
            DataStream<ZGMessage> source,
            int capacity,
            int timeoutMs,
            String kvrocksHost,
            int kvrocksPort,
            boolean kvrocksCluster) {

        LOG.info("使用统一ID处理流程 (OneIdService + Hash结构)");

        // 1. 设备ID映射
        SingleOutputStreamOperator<ZGMessage> withDeviceId = AsyncDataStream.unorderedWait(
                source,
                new DeviceIdAsyncOperator(kvrocksHost, kvrocksPort, kvrocksCluster),
                timeoutMs, TimeUnit.MILLISECONDS, capacity
        ).name("DeviceId-AsyncIO").uid("device-id-async");

        // 2. 会话ID处理
        DataStream<ZGMessage> withSessionId = withDeviceId
                .process(new SessionIdProcessOperator())
                .name("SessionId-Process")
                .uid("session-id-process");

        // 3. 用户ID映射
        SingleOutputStreamOperator<ZGMessage> withUserId = AsyncDataStream.unorderedWait(
                withSessionId,
                new UserIdAsyncOperator(kvrocksHost, kvrocksPort, kvrocksCluster),
                timeoutMs, TimeUnit.MILLISECONDS, capacity
        ).name("UserId-AsyncIO").uid("user-id-async");

        // 4. 诸葛ID映射 (完整的用户-设备绑定逻辑)
        SingleOutputStreamOperator<ZGMessage> withZgid = AsyncDataStream.unorderedWait(
                withUserId,
                new ZgidAsyncOperator(kvrocksHost, kvrocksPort, kvrocksCluster),
                timeoutMs, TimeUnit.MILLISECONDS, capacity
        ).name("Zgid-AsyncIO").uid("zgid-async");

        return withZgid;
    }

    /**
     * 广告业务处理
     */
    private static DataStream<ZGMessage> buildAdvBusinessPipeline(DataStream<ZGMessage> source) {
        // 广告数据保存
        SingleOutputStreamOperator<ZGMessage> advSaveStream = source
                .flatMap(new AdvSaveAppAdDataFlatMapFunction())
                .name("advSaveAppAdDataStream")
                .uid("advSaveAppAdDataStream");

        final OutputTag<String> advSaveTag = new OutputTag<String>("id-advSaveAppAdDataTag") {};
        SingleOutputStreamOperator<ZGMessage> advSaveProcessed = advSaveStream
                .process(new AdvSaveAppAdDataProcessFunction(advSaveTag));

        SideOutputDataStream<String> advSaveSideOutput = advSaveProcessed.getSideOutput(advSaveTag);
        CustomKafkaSink.addCustomKafkaSink(
                advSaveSideOutput,
                Config.getString("kafka.adv.sinkTopic"),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "id-advSaveAppAdDataSideOutput-sink"
        );

        // 广告LID和用户首末次处理
        SingleOutputStreamOperator<ZGMessage> advLidStream = advSaveProcessed
                .flatMap(new AdvLidAndUserFirstEndFlatMapFunction())
                .name("id-advLidAndUserFirstEndDataStream0")
                .uid("id-advLidAndUserFirstEndDataStream0");

        final OutputTag<String> advLidTag = new OutputTag<String>("advLidAndUserFirstEndTag") {};
        SingleOutputStreamOperator<ZGMessage> advLidProcessed = advLidStream
                .process(new AdvLidAndUserFirstEndProcessFunction(advLidTag));

        SideOutputDataStream<String> advLidSideOutput = advLidProcessed.getSideOutput(advLidTag);
        CustomKafkaSink.addCustomKafkaSink(
                advLidSideOutput,
                Config.getString("kafka.adv.sinkTopic"),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "id-advLidAndUserFirstEndSideOutput-sink"
        );

        return advLidProcessed;
    }

    /**
     * 广告回传处理
     */
    private static SingleOutputStreamOperator<ZGMessage> buildAdvConvertPipeline(DataStream<ZGMessage> source) {
        SingleOutputStreamOperator<ZGMessage> advConvertStream = source
                .flatMap(new AdvConvertEventFlatMapFunction())
                .name("advConvertEventDataStream0")
                .uid("advConvertEventDataStream0");

        final OutputTag<String> advConvertTag = new OutputTag<String>("id-advConvertEventTag") {};
        final OutputTag<String> advConvertUserTag = new OutputTag<String>("id-advConvertEventUserTag") {};

        SingleOutputStreamOperator<ZGMessage> advConvertProcessed = advConvertStream
                .process(new AdvConvertEventProcessFunction(advConvertTag, advConvertUserTag));

        SideOutputDataStream<String> advConvertSideOutput = advConvertProcessed.getSideOutput(advConvertTag);
        CustomKafkaSink.addCustomKafkaSink(
                advConvertSideOutput,
                Config.getString(Config.getString(Config.KAFKA_ADV_SOURCE_TOPIC)),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "id-advConvertEventSideOutput-sink"
        );

        SideOutputDataStream<String> advConvertUserSideOutput = advConvertProcessed.getSideOutput(advConvertUserTag);
        CustomKafkaSink.addCustomKafkaSink(
                advConvertUserSideOutput,
                Config.getString(Config.getString(Config.KAFKA_ADV_USER_SOURCE_TOPIC)),
                Config.getString(Config.KAFKA_BROKERS),
                true,
                "id-advConvertEventUserSideOutput-sink"
        );

        return advConvertProcessed;
    }
}
