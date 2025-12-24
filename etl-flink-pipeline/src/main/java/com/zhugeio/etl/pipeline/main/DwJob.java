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
 * DW处理主任务
 * 
 * 处理流程:
 * 1. IP解析增强
 * 2. UA解析增强
 * 3. 搜索关键词解析
 * 4. 数据路由
 * 5. Doris写入
 * 
 * 使用方式:
 * 1. 单独运行: DwJob.main()
 * 2. 与 AllJob 串联: 
 *    - DwJob.execute(source) - 包含Sink
 *    - DwJob.buildDwPipeline(source) + DwJob.addDorisSinks(routed) - 分开控制
 */
public class DwJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwJob.class);
    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";

    // 异步算子默认配置
    private static final int ASYNC_CAPACITY_IP = 500;
    private static final int ASYNC_CAPACITY_UA = 500;
    private static final int ASYNC_CAPACITY_KEYWORD = 500;
    private static final int ASYNC_CAPACITY_ROUTER = 1000;

    private static final long ASYNC_TIMEOUT_IP = 30000L;
    private static final long ASYNC_TIMEOUT_UA = 5000L;
    private static final long ASYNC_TIMEOUT_KEYWORD = 5000L;
    private static final long ASYNC_TIMEOUT_ROUTER = 10000L;

    // ==================== Main 入口 ====================

    public static void main(String[] args) throws Exception {
        LOG.info("DW ETL Pipeline 启动...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String checkpointPath = CHECKPOINT_BASE + Config.getString(Config.CHECKPOINT_DW_PATH, "dw_job");
        FlinkEnvConfig.configureCheckpoint(env, Config.getLong(Config.FLINK_CHECKPOINT_INTERVAL_MS, 60000L), checkpointPath);

        int customColumns = Config.getInt("event.attr.custom.columns", 100);
        EventAttrRow.configure(customColumns);

        env.getCheckpointConfig().enableUnalignedCheckpoints();
        env.getCheckpointConfig().setCheckpointTimeout(120000L);

        LOG.info("parallelism: {}", env.getParallelism());

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

        // 执行 DW 处理流程 (包含 Sink)
        execute(source);

        env.execute("DW-ETL-Pipeline");
    }

    // ==================== 核心方法 ====================

    /**
     * 执行 DW 完整流程 (Pipeline + Sink)
     * 
     * @param source 输入数据流
     * @return 路由后的 EventAttrRow 流
     */
    public static SingleOutputStreamOperator<EventAttrRow> execute(DataStream<ZGMessage> source) {
        // 1. 构建流水线
        SingleOutputStreamOperator<EventAttrRow> routed = buildDwPipeline(source);
        
        // 2. 添加 Sink
        addDorisSinks(routed);
        
        return routed;
    }

    /**
     * 构建 DW 流水线 (不包含 Sink)
     * 
     * @param source 输入数据流
     * @return 路由后的 EventAttrRow 流
     */
    public static SingleOutputStreamOperator<EventAttrRow> buildDwPipeline(DataStream<ZGMessage> source) {
        // 从 Config 获取配置
        int asyncCapacityIp = Config.getInt("async.capacity.ip", ASYNC_CAPACITY_IP);
        int asyncCapacityUa = Config.getInt("async.capacity.ua", ASYNC_CAPACITY_UA);
        int asyncCapacityKeyword = Config.getInt("async.capacity.keyword", ASYNC_CAPACITY_KEYWORD);
        int asyncCapacityRouter = Config.getInt("async.capacity.router", ASYNC_CAPACITY_ROUTER);

        long asyncTimeoutIp = Config.getLong("async.timeout.ip", ASYNC_TIMEOUT_IP);
        long asyncTimeoutUa = Config.getLong("async.timeout.ua", ASYNC_TIMEOUT_UA);
        long asyncTimeoutKeyword = Config.getLong("async.timeout.keyword", ASYNC_TIMEOUT_KEYWORD);
        long asyncTimeoutRouter = Config.getLong("async.timeout.router", ASYNC_TIMEOUT_ROUTER);

        // ========== Step 1: 数据增强 ==========
        DataStream<ZGMessage> enriched = buildEnrichmentPipeline(
                source,
                asyncCapacityIp, asyncCapacityUa, asyncCapacityKeyword,
                asyncTimeoutIp, asyncTimeoutUa, asyncTimeoutKeyword);

        // ========== Step 2: 数据路由 ==========
        SingleOutputStreamOperator<EventAttrRow> routed = buildRouterPipeline(
                enriched, asyncCapacityRouter, asyncTimeoutRouter);

        LOG.info("DW Pipeline 构建完成");
        return routed;
    }

    /**
     * 添加 Doris Sink
     * 
     * @param routed 路由后的数据流
     */
    public static void addDorisSinks(SingleOutputStreamOperator<EventAttrRow> routed) {
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

        LOG.info("Doris Sink 配置完成, feNodes={}, database={}", feNodes, database);
    }

    // ==================== 内部构建方法 ====================

    /**
     * 构建增强流水线
     */
    private static DataStream<ZGMessage> buildEnrichmentPipeline(DataStream<ZGMessage> input,
                                                                   int capacityIp,
                                                                   int capacityUa,
                                                                   int capacityKeyword,
                                                                   long timeoutIp,
                                                                   long timeoutUa,
                                                                   long timeoutKeyword) {
        // IP解析
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
                timeoutIp, TimeUnit.MILLISECONDS, capacityIp
        ).name("IpEnrichOperator").uid("ip-enrich");

        // UA解析
        DataStream<ZGMessage> withUa = AsyncDataStream.unorderedWait(
                withIp,
                new UserAgentEnrichOperator(
                        Config.getInt(Config.OPERATOR_UA_CACHE_SIZE, 10000),
                        Config.getLong(Config.OPERATOR_UA_CACHE_EXPIRE_MINUTES, 60L)
                ),
                timeoutUa, TimeUnit.MILLISECONDS, capacityUa
        ).name("UserAgentEnrichOperator").uid("ua-enrich");

        // 搜索关键词解析
        return AsyncDataStream.unorderedWait(
                withUa,
                new SearchKeywordEnrichOperator(
                        Config.getString(Config.KVROCKS_HOST, "localhost"),
                        Config.getInt(Config.KVROCKS_PORT, 6379),
                        Config.getBoolean(Config.KVROCKS_CLUSTER, false),
                        Config.getInt(Config.KVROCKS_LOCAL_CACHE_SIZE, 5000),
                        Config.getLong(Config.KVROCKS_LOCAL_CACHE_EXPIRE_MINUTES, 30L)
                ),
                timeoutKeyword, TimeUnit.MILLISECONDS, capacityKeyword
        ).name("SearchKeywordEnrichOperator").uid("keyword-enrich");
    }

    /**
     * 构建路由流水线
     */
    private static SingleOutputStreamOperator<EventAttrRow> buildRouterPipeline(DataStream<ZGMessage> enriched,
                                                                                  int capacityRouter,
                                                                                  long timeoutRouter) {
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
                timeoutRouter, TimeUnit.MILLISECONDS, capacityRouter
        ).name("DataRouterAsync").uid("data-router-async");

        return routerOutput
                .process(new RouterOutputUnpacker())
                .name("RouterUnpacker").uid("router-unpacker");
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
