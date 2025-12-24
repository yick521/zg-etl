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

public class DwJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwJob.class);
    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";

    // 【优化】增大异步容量
    private static final int ASYNC_CAPACITY_IP = 500;
    private static final int ASYNC_CAPACITY_UA = 500;
    private static final int ASYNC_CAPACITY_KEYWORD = 500;
    private static final int ASYNC_CAPACITY_ROUTER = 1000;

    private static final long ASYNC_TIMEOUT_IP = 30000L;
    private static final long ASYNC_TIMEOUT_UA = 5000L;
    private static final long ASYNC_TIMEOUT_KEYWORD = 5000L;
    private static final long ASYNC_TIMEOUT_ROUTER = 10000L;

    public static void main(String[] args) throws Exception {
        LOG.info("DW ETL Pipeline 启动 (性能优化版)...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointPath = CHECKPOINT_BASE + Config.getString(Config.CHECKPOINT_DW_PATH, "dw_job");
        FlinkEnvConfig.configureCheckpoint(env, Config.getLong(Config.FLINK_CHECKPOINT_INTERVAL_MS, 60000L), checkpointPath);

        int customColumns = Config.getInt("event.attr.custom.columns", 100);
        EventAttrRow.configure(customColumns);

//        FlinkEnvConfig.configureFixedDelayRestart(env, 3, 10);

        // 【优化】启用非对齐 Checkpoint（减少 checkpoint 期间的背压和延迟）
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        // 设置 checkpoint 超时时间
        env.getCheckpointConfig().setCheckpointTimeout(120000L);  // 2 分钟

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

        DataStream<ZGMessage> enriched = buildEnrichmentPipeline(source);
        SingleOutputStreamOperator<EventAttrRow> routed = buildRouterPipeline(enriched);
        addDynamicDorisSinks(routed);

        env.execute("DW-ETL-Pipeline-Optimized");
    }

    private static DataStream<ZGMessage> buildEnrichmentPipeline(DataStream<ZGMessage> input) {
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
                ASYNC_TIMEOUT_IP, TimeUnit.MILLISECONDS, ASYNC_CAPACITY_IP
        ).name("IpEnrichOperator").uid("ip-enrich");

        DataStream<ZGMessage> withUa = AsyncDataStream.unorderedWait(
                withIp,
                new UserAgentEnrichOperator(
                        Config.getInt(Config.OPERATOR_UA_CACHE_SIZE, 10000),
                        Config.getLong(Config.OPERATOR_UA_CACHE_EXPIRE_MINUTES, 60L)
                ),
                ASYNC_TIMEOUT_UA, TimeUnit.MILLISECONDS, ASYNC_CAPACITY_UA
        ).name("UserAgentEnrichOperator").uid("ua-enrich");

        return AsyncDataStream.unorderedWait(
                withUa,
                new SearchKeywordEnrichOperator(
                        Config.getString(Config.KVROCKS_HOST, "localhost"),
                        Config.getInt(Config.KVROCKS_PORT, 6379),
                        Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 5000),
                        Config.getLong(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 30L)
                ),
                ASYNC_TIMEOUT_KEYWORD, TimeUnit.MILLISECONDS, ASYNC_CAPACITY_KEYWORD
        ).name("SearchKeywordEnrichOperator").uid("keyword-enrich");
    }

    private static SingleOutputStreamOperator<EventAttrRow> buildRouterPipeline(DataStream<ZGMessage> enriched) {
        DataStream<RouterOutput> routerOutput = AsyncDataStream.unorderedWait(
                enriched,
                new DataRouterOperator(
                        Config.getString(Config.KVROCKS_HOST, "localhost"),
                        Config.getInt(Config.KVROCKS_PORT, 6379),
                        Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 10000),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 60),
                        Config.getString(Config.BLACK_APPIDS, "-1"),
                        Config.getInt(Config.TIME_EXPIRE_SUBDAYS, 7),
                        Config.getInt(Config.TIME_EXPIRE_ADDDAYS, 1),
                        Config.getInt(Config.EVENT_ATTR_LENGTH_LIMIT, 256),
                        Config.getBoolean(Config.DQ_ENABLED, true)
                ),
                ASYNC_TIMEOUT_ROUTER, TimeUnit.MILLISECONDS, ASYNC_CAPACITY_ROUTER
        ).name("DataRouterAsync").uid("data-router-async");

        return routerOutput
                .process(new RouterOutputUnpacker())
                .name("RouterUnpacker").uid("router-unpacker");
    }

    private static void addDynamicDorisSinks(SingleOutputStreamOperator<EventAttrRow> routed) {
        DataStream<UserRow> userStream = routed.getSideOutput(RouterOutputUnpacker.USER_OUTPUT);
        DataStream<DeviceRow> deviceStream = routed.getSideOutput(RouterOutputUnpacker.DEVICE_OUTPUT);
        DataStream<UserPropertyRow> userPropStream = routed.getSideOutput(RouterOutputUnpacker.USER_PROPERTY_OUTPUT);

        String feNodes = Config.getString(Config.DORIS_FE_NODES);
        String database = Config.getString(Config.DORIS_DATABASE, "dwd");
        String username = Config.getString(Config.DORIS_USERNAME, "root");
        String password = Config.getString(Config.DORIS_PASSWORD, "");

        CommitSuccessCallback callback = createCallback();

        DynamicDorisSinkBuilder.addUserSink(userStream, feNodes, database, username, password, callback);
        DynamicDorisSinkBuilder.addDeviceSink(deviceStream, feNodes, database, username, password, callback);
        DynamicDorisSinkBuilder.addUserPropertySink(userPropStream, feNodes, database, username, password, callback);
        DynamicDorisSinkBuilder.addEventAttrSink(routed, feNodes, database, username, password, callback);

        LOG.info("Doris Sink 配置完成, 异步容量: IP={}, UA={}, Keyword={}, Router={}",
                ASYNC_CAPACITY_IP, ASYNC_CAPACITY_UA, ASYNC_CAPACITY_KEYWORD, ASYNC_CAPACITY_ROUTER);
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
