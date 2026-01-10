package com.zhugeio.etl.common.config;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Flink 环境配置工具
 *
 * 无业务依赖，可跨项目复用
 */
public class FlinkEnvConfig {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvConfig.class);

    private FlinkEnvConfig() {}

    // ========== Checkpoint 配置 ==========

    /**
     * 配置 Checkpoint (完整参数)
     *
     * @param env Flink 环境
     * @param intervalMs Checkpoint 间隔 (毫秒)
     * @param checkpointPath Checkpoint 存储路径
     * @param useRocksDB 是否使用 RocksDB (大状态推荐)
     * @param incremental 是否增量 Checkpoint (仅 RocksDB 有效)
     */
    public static void configureCheckpoint(
            StreamExecutionEnvironment env,
            long intervalMs,
            String checkpointPath,
            boolean useRocksDB,
            boolean incremental) {

        // 启用 Checkpoint
        env.enableCheckpointing(intervalMs);

        CheckpointConfig ckConfig = env.getCheckpointConfig();

        // 精确一次语义
        ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 最大并发 Checkpoint 数
        ckConfig.setMaxConcurrentCheckpoints(1);

        // 最小间隔 (防止 Checkpoint 过于频繁)
        ckConfig.setMinPauseBetweenCheckpoints(intervalMs / 2);

        // Checkpoint 超时
        ckConfig.setCheckpointTimeout(intervalMs * 2);

        // 容忍失败次数
        ckConfig.setTolerableCheckpointFailureNumber(10);

        // 取消时保留 Checkpoint
        ckConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 状态后端
        if (useRocksDB) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(incremental));
            LOG.info("状态后端: RocksDB (incremental={})", incremental);
        } else {
            env.setStateBackend(new HashMapStateBackend());
            LOG.info("状态后端: HashMap");
        }

        // 存储路径
        ckConfig.setCheckpointStorage(checkpointPath);

        LOG.info("Checkpoint 配置完成: interval={}ms, path={}", intervalMs, checkpointPath);
    }

    /**
     * 配置 Checkpoint (默认 RocksDB 增量)
     */
    public static void configureCheckpoint(
            StreamExecutionEnvironment env,
            long intervalMs,
            String checkpointPath) {
        configureCheckpoint(env, intervalMs, checkpointPath, true, true);
    }

    /**
     * 配置 Checkpoint (HashMap 状态后端，适合小状态)
     */
    public static void configureHashMapCheckpoint(
            StreamExecutionEnvironment env,
            long intervalMs,
            String checkpointPath) {
        configureCheckpoint(env, intervalMs, checkpointPath, false, false);
    }

    // ========== 重启策略 ==========

    /**
     * 固定延迟重启策略
     *
     * @param env Flink 环境
     * @param maxAttempts 最大重试次数
     * @param delaySeconds 重试间隔 (秒)
     */
    public static void configureFixedDelayRestart(
            StreamExecutionEnvironment env,
            int maxAttempts,
            long delaySeconds) {

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                maxAttempts,
                Time.of(delaySeconds, TimeUnit.SECONDS)
        ));

        LOG.info("重启策略: fixedDelay (maxAttempts={}, delay={}s)", maxAttempts, delaySeconds);
    }

    /**
     * 失败率重启策略
     *
     * @param env Flink 环境
     * @param maxFailures 时间窗口内最大失败次数
     * @param windowMinutes 时间窗口 (分钟)
     * @param delaySeconds 重试间隔 (秒)
     */
    public static void configureFailureRateRestart(
            StreamExecutionEnvironment env,
            int maxFailures,
            long windowMinutes,
            long delaySeconds) {

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                maxFailures,
                Time.of(windowMinutes, TimeUnit.MINUTES),
                Time.of(delaySeconds, TimeUnit.SECONDS)
        ));

        LOG.info("重启策略: failureRate (maxFailures={}, window={}min, delay={}s)",
                maxFailures, windowMinutes, delaySeconds);
    }

    /**
     * 禁用重启
     */
    public static void configureNoRestart(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.noRestart());
        LOG.info("重启策略: 禁用");
    }

    // ========== Kryo 序列化 ==========

    /**
     * 注册 Kryo 类型
     */
    public static void registerKryoTypes(StreamExecutionEnvironment env, Class<?>... classes) {
        for (Class<?> clazz : classes) {
            env.getConfig().registerKryoType(clazz);
        }
        LOG.info("注册 Kryo 类型: {} 个", classes.length);
    }

    /**
     * 强制使用 Kryo
     */
    public static void forceKryo(StreamExecutionEnvironment env) {
        env.getConfig().enableForceKryo();
        LOG.info("强制使用 Kryo 序列化");
    }

    // ========== 其他配置 ==========

    /**
     * 配置缓冲区超时 (影响延迟)
     *
     * @param env Flink 环境
     * @param timeoutMs 超时毫秒 (0 = 立即发送, -1 = 仅当缓冲区满时发送)
     */
    public static void configureBufferTimeout(StreamExecutionEnvironment env, long timeoutMs) {
        env.setBufferTimeout(timeoutMs);
        LOG.info("缓冲区超时: {}ms", timeoutMs);
    }

    /**
     * 低延迟配置
     */
    public static void configureLowLatency(StreamExecutionEnvironment env) {
        env.setBufferTimeout(10);  // 10ms
        LOG.info("低延迟模式: bufferTimeout=10ms");
    }

    /**
     * 高吞吐配置
     */
    public static void configureHighThroughput(StreamExecutionEnvironment env) {
        env.setBufferTimeout(100);  // 100ms
        LOG.info("高吞吐模式: bufferTimeout=100ms");
    }
}