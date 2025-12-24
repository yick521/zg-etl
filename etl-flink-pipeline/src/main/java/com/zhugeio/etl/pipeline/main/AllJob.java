package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.common.config.Config;
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
 * 使用方式:
 * flink run -p 10 -c com.zhugeio.etl.pipeline.main.AllJob etl-pipeline.jar
 */
public class AllJob {

    private static final Logger LOG = LoggerFactory.getLogger(AllJob.class);

    public static void main(String[] args) {
        LOG.info("All ETL Pipeline 启动...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env);

        LOG.info("parallelism: {}", env.getParallelism());

        // ========== Gate → ID → DW ==========
        // 通过配置 gate.sink.to.downstream=false, id.sink.to.downstream=false
        // 实现数据在内存传递，无中间Kafka
        DataStream<ZGMessage> gateResult = GateJob.execute(env);
        DataStream<ZGMessage> idResult = IdJob.execute(gateResult);
        DwJob.execute(idResult);

        try {
            env.execute("etl-pipeline-AllJob");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化 Checkpoint 配置
     */
    public static void initCheckpoint(StreamExecutionEnvironment env) {
        int checkpointInterval = Config.getInt("checkpoint.all.interval", 30000);
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 设置Checkpoint存储
        String checkpointPath = "hdfs:///user/flink/checkpoints/" + Config.getString("checkpoint.all.path", "all_job");
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
    }
}
