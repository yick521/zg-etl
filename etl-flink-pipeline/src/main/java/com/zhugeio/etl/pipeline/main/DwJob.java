package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.config.FlinkEnvConfig;
import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import com.zhugeio.etl.common.sink.CommitSuccessCallback;
import com.zhugeio.etl.common.sink.DynamicDorisSinkBuilder;
import com.zhugeio.etl.common.source.KafkaSourceBuilder;
import com.zhugeio.etl.pipeline.dataquality.DataQualityKafkaService;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.kafka.ZGMsgSchema;
import com.zhugeio.etl.pipeline.operator.dw.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * DW 层 ETL 主任务
 */
public class DwJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwJob.class);

    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";

    public static void main(String[] args) throws Exception {
        LOG.info("DW ETL Pipeline  启动...");

        // 1. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointPath = CHECKPOINT_BASE + Config.getString(Config.CHECKPOINT_DW_PATH, "dw_job");
        FlinkEnvConfig.configureCheckpoint(env, 60000L, checkpointPath);
        FlinkEnvConfig.configureFixedDelayRestart(env, 3, 10);

        // 2. Source
        DataStream<ZGMessage> source = env.fromSource(
                KafkaSourceBuilder.<ZGMessage>builder()
                        .brokers(Config.getString(Config.KAFKA_BROKERS))
                        .topic(Config.getString(Config.KAFKA_DW_SOURCE_TOPIC))
                        .groupId(Config.getString(Config.KAFKA_DW_GROUP_ID))
                        .consumerProps(Config.getKafkaConsumerProps())
                        .deserializer(new ZGMsgSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource"
        ).uid("kafka-source");

        // 3. 富化
        DataStream<ZGMessage> enriched = buildEnrichmentPipeline(source);

        // 4. 异步路由 + 解包
        SingleOutputStreamOperator<EventAttrRow> routed = buildAsyncRouterPipeline(enriched);

        // 5. 动态分表 Sink
        addDynamicDorisSinks(routed);

        // 6. 执行
        env.execute("DW-ETL-Pipeline-DynamicTable");
    }

    /**
     * 异步路由流水线
     */
    private static SingleOutputStreamOperator<EventAttrRow> buildAsyncRouterPipeline(DataStream<ZGMessage> enriched) {
        
        // Step 1: 异步路由
        DataStream<RouterOutput> routerOutput = AsyncDataStream.unorderedWait(
                enriched,
                new DataRouterOperator(
                        Config.getString(Config.KVROCKS_HOST, "localhost"),
                        Config.getInt(Config.KVROCKS_PORT, 6379),
                        Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 10000),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 60),
                        Config.getString(Config.BLACK_APPIDS, "-1"),
                        Config.getString(Config.WHITE_APPID, ""),
                        Config.getInt(Config.TIME_EXPIRE_SUBDAYS, 7),
                        Config.getInt(Config.TIME_EXPIRE_ADDDAYS, 1),
                        Config.getInt(Config.EVENT_ATTR_LENGTH_LIMIT, 256),
                        Config.getBoolean(Config.DQ_ENABLED, true),
                        Config.getString(Config.BAIDU_URL, ""),
                        Config.getString(Config.BAIDU_ID, ""),
                        Config.getString(Config.BAIDU_KEY, ""),
                        Config.getString(Config.REDIS_HOST, "localhost"),
                        Config.getInt(Config.REDIS_PORT, 6379),
                        Config.getBoolean(Config.REDIS_CLUSTER, false),
                        Config.getInt(Config.REQUEST_SOCKET_TIMEOUT, 5),
                        Config.getInt(Config.REQUEST_CONNECT_TIMEOUT, 5)
                ),
                Config.getLong(Config.OPERATOR_KEYWORD_TIMEOUT_MS, 5000L),
                TimeUnit.MILLISECONDS,
                Config.getInt(Config.OPERATOR_KEYWORD_CAPACITY, 100)
        ).name("DataRouterAsync").uid("data-router-async");
        
        // Step 2: 解包分流
        return routerOutput
                .process(new RouterOutputUnpacker())
                .name("RouterUnpacker").uid("router-unpacker");
    }

    /**
     * 构建富化流水线
     */
    private static DataStream<ZGMessage> buildEnrichmentPipeline(DataStream<ZGMessage> input) {
        // IP 富化
        DataStream<ZGMessage> withIp = AsyncDataStream.unorderedWait(
                input,
                new IpEnrichOperator(
                        Config.getString(Config.IP_FILE_DIR, "/ipFileDirNew"),
                        Config.getBoolean(Config.RELOAD_IP_FILE, false),
                        Config.getString(Config.IPV6_FILE_DIR, "/ipv6FileDir"),
                        Config.getBoolean(Config.IPV6_LOAD, false),
                        Config.getBoolean(Config.RELOAD_IPV6_FILE, false),
                        Config.getLong(Config.RELOAD_RATE_SECOND, 43200L),
                        Config.getBoolean(Config.FLAG_HA, true),
                        Config.getString(Config.FS_DEFAULT_FS, "hdfs://zhugeio"),
                        Config.getString(Config.DFS_NAMESERVICES, "zhugeio"),
                        Config.getString(Config.DFS_HA_NAMESPACE, "realtime-1,realtime-2"),
                        Config.getString(Config.DFS_NAMENODE_RPC_Z1, "realtime-1:8020"),
                        Config.getString(Config.DFS_NAMENODE_RPC_Z2, "realtime-2:8020")
                ),
                Config.getLong(Config.OPERATOR_IP_TIMEOUT_MS, 60000L),
                TimeUnit.MILLISECONDS,
                Config.getInt(Config.OPERATOR_IP_CAPACITY, 100)
        ).name("IpEnrichOperator").uid("ip-enrich");

        // UA 富化
        DataStream<ZGMessage> withUa = AsyncDataStream.unorderedWait(
                withIp,
                new UserAgentEnrichOperator(
                        Config.getInt(Config.OPERATOR_UA_CACHE_SIZE, 10000),
                        Config.getLong(Config.OPERATOR_UA_CACHE_EXPIRE_MINUTES, 60L)
                ),
                Config.getLong(Config.OPERATOR_UA_TIMEOUT_MS, 10000L),
                TimeUnit.MILLISECONDS,
                Config.getInt(Config.OPERATOR_UA_CAPACITY, 100)
        ).name("UserAgentEnrichOperator").uid("ua-enrich");

        // 关键词富化
        return AsyncDataStream.unorderedWait(
                withUa,
                new SearchKeywordEnrichOperator(
                        Config.getString(Config.KVROCKS_HOST, "localhost"),
                        Config.getInt(Config.KVROCKS_PORT, 6379),
                        Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 5000),
                        Config.getLong(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 30L)
                ),
                Config.getLong(Config.OPERATOR_KEYWORD_TIMEOUT_MS, 5000L),
                TimeUnit.MILLISECONDS,
                Config.getInt(Config.OPERATOR_KEYWORD_CAPACITY, 100)
        ).name("SearchKeywordEnrichOperator").uid("keyword-enrich");
    }

    /**
     * 添加动态分表 Doris Sink
     * 
     * 根据 appId 自动路由到:
     * - b_user_{appId}
     * - b_device_{appId}
     * - b_user_property_{appId}
     * - b_user_event_attr_{appId}
     */
    private static void addDynamicDorisSinks(SingleOutputStreamOperator<EventAttrRow> routed) {
        // 获取侧输出流
        DataStream<UserRow> userStream = routed.getSideOutput(RouterOutputUnpacker.USER_OUTPUT);
        DataStream<DeviceRow> deviceStream = routed.getSideOutput(RouterOutputUnpacker.DEVICE_OUTPUT);
        DataStream<UserPropertyRow> userPropStream = routed.getSideOutput(RouterOutputUnpacker.USER_PROPERTY_OUTPUT);

        // Doris 配置
        String feNodes = Config.getString(Config.DORIS_FE_NODES);
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");

        // 数据质量回调
        CommitSuccessCallback callback = createCallback();

        // ==================== 动态分表 Sink ====================
        
        // UserRow -> b_user_{appId}
        DynamicDorisSinkBuilder.addUserSink(
                userStream, feNodes, database, username, password, callback);

        // DeviceRow -> b_device_{appId}
        DynamicDorisSinkBuilder.addDeviceSink(
                deviceStream, feNodes, database, username, password, callback);

        // UserPropertyRow -> b_user_property_{appId}
        DynamicDorisSinkBuilder.addUserPropertySink(
                userPropStream, feNodes, database, username, password, callback);

        // EventAttrRow -> b_user_event_attr_{appId}
        DynamicDorisSinkBuilder.addEventAttrSink(
                routed, feNodes, database, username, password, callback);

        LOG.info("======================================");
        LOG.info("动态分表 Doris Sink 配置完成:");
        LOG.info("  - UserRow      -> {}.b_user_{{appId}}", database);
        LOG.info("  - DeviceRow    -> {}.b_device_{{appId}}", database);
        LOG.info("  - UserPropRow  -> {}.b_user_property_{{appId}}", database);
        LOG.info("  - EventAttrRow -> {}.b_user_event_attr_{{appId}}", database);
        LOG.info("======================================");
    }

    /**
     * 创建数据质量回调
     */
    private static CommitSuccessCallback createCallback() {
        if (!Config.getBoolean(Config.DQ_ENABLED, true)) {
            return null;
        }
        DataQualityKafkaService dqService = DataQualityKafkaService.getInstance();
        return (tableName, count) -> {
            if (dqService != null && dqService.isEnabled()) {
                dqService.recordSuccessCount(tableName, count);
                dqService.flushSuccessCounts();
            }
        };
    }
}
