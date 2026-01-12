package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.common.config.FlinkEnvConfig;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 全链路处理任务
 * 
 * 串联 Gate → ID → DW 完整流程
 * 
 * 流程:
 * 1. GateJob: 数据清洗、格式校验
 * 2. IdJob: ID映射、事件/属性处理
 * 3. DwJob: 数据增强、路由、写入Doris
 * 
 * 配置项 (config.properties):
 * - gate.sink.to.downstream=false  # Gate不输出到中间Topic
 * - id.sink.to.downstream=false    # ID不输出到中间Topic
 *
 */
public class AllJob {

    private static final Logger LOG = LoggerFactory.getLogger(AllJob.class);
    private static final String CHECKPOINT_BASE = "hdfs:///user/flink/checkpoints/";

    public static void main(String[] args) {
        LOG.info("All ETL Pipeline 启动...");

        String checkpointPath = CHECKPOINT_BASE + Config.getString(Config.CHECKPOINT_DW_PATH, "all_job");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkEnvConfig.configureCheckpoint(env, Config.getLong(Config.FLINK_CHECKPOINT_INTERVAL_MS, 60000L), checkpointPath);

        int customColumns = Config.getInt("event.attr.custom.columns", 100);
        EventAttrRow.configure(customColumns);

        LOG.info("parallelism: {}", env.getParallelism());

        // ========== Gate → ID → DW ==========
        // 通过配置 gate.sink.to.downstream=false, id.sink.to.downstream=false
        // 实现数据在内存传递，无中间Kafka
        DataStream<ZGMessage> gateResult = GateJob.execute(env);
        DataStream<ZGMessage> idResult = IdJob.execute(gateResult);
//        DataStream<ZGMessage> printedStream = idResult.map(message -> {
//            LOG.info("ZGMessage内容: {}", message.toString());
//            return message;
//        }).name("print-message").setParallelism(1);
        DwJob.execute(idResult);

        try {
            env.execute("etl-pipeline-AllJob");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
