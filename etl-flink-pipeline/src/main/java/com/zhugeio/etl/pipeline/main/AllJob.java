package com.zhugeio.etl.pipeline.main;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.tool.properties.PropertiesUtil;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author ningjh
 * @name GateJob
 * @date 2025/11/28
 * @description
 */
public class AllJob {
    private static final Logger logger = LoggerFactory.getLogger(AllJob.class);
    public static void main(String[] args) {
        // 读取配置
        Properties configProperties = PropertiesUtil.getProperties("config.properties");
        logger.info("config : {}",configProperties);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        initCheckpoint(env,configProperties);
        int parallelism = env.getParallelism();  // 提交任务的命令中指定的全局并行度
        logger.info("parallelism : {}",parallelism);

        // gate
        DataStream<ZGMessage> gateStream = GateJob.execute(env,configProperties, parallelism);

        // id
        DataStream<ZGMessage> idStream = IdJob.execute(gateStream, configProperties, parallelism);

        // dw
        // todo:

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
