package com.zhugeio.etl.common.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 算子级 Metrics (增强版 - 支持定时打印)
 *
 * 核心指标:
 * - in: 输入数据量
 * - out: 输出数据量 (处理成功)
 * - error: 处理异常数
 * - skip: 跳过数 (数据不需要处理)
 * - cacheHitRate: 缓存命中率
 * - latency: 处理耗时 (滑动窗口，最近 N 条的平均)
 *
 * 定时打印功能:
 * - 每隔指定时间打印一次 QPS、延迟、缓存命中率等指标
 * - 默认 60 秒打印一次
 *
 * 使用:
 * <pre>
 * // open() 中 - 启用定时打印 (每60秒)
 * metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ua_enrich", 60);
 *
 * // 或者不启用定时打印
 * metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ua_enrich");
 *
 * // 处理逻辑中
 * metrics.in();  // 开始计时
 *
 * try {
 *     if (data.get("ua") == null) {
 *         metrics.skip();
 *         return message;
 *     }
 *
 *     if (cache.get(ua) != null) {
 *         metrics.cacheHit();
 *     } else {
 *         metrics.cacheMiss();
 *     }
 *
 *     metrics.out();  // 成功，自动计算耗时
 * } catch (Exception e) {
 *     metrics.error();  // 失败，自动计算耗时
 * }
 *
 * // close() 中
 * metrics.shutdown();
 * </pre>
 */
public class OperatorMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorMetrics.class);

    private final String name;

    // 核心计数
    private final LongAdder inCount = new LongAdder();
    private final LongAdder outCount = new LongAdder();
    private final LongAdder errorCount = new LongAdder();
    private final LongAdder skipCount = new LongAdder();

    // 缓存统计
    private final LongAdder cacheHitCount = new LongAdder();
    private final LongAdder cacheMissCount = new LongAdder();

    // 延迟统计 (时间窗口，无锁)
    private static final int BUCKET_COUNT = 60;
    private final LongAdder[] bucketSum = new LongAdder[BUCKET_COUNT];
    private final LongAdder[] bucketCount = new LongAdder[BUCKET_COUNT];
    private final AtomicLong lastBucketTime = new AtomicLong(0);
    private final AtomicLong latencyMax = new AtomicLong(0);

    // 每个线程记录自己的开始时间
    private final ThreadLocal<Long> startTime = new ThreadLocal<>();

    private Counter flinkInCounter;

    // ========== 定时打印相关 ==========
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> printTask;
    private final int printIntervalSeconds;

    // 上次打印时的计数，用于计算区间 QPS
    private long lastPrintIn = 0;
    private long lastPrintOut = 0;
    private long lastPrintError = 0;
    private long lastPrintSkip = 0;
    private long lastPrintTime = System.currentTimeMillis();

    /**
     * 私有构造函数
     * @param name 指标名称
     * @param printIntervalSeconds 打印间隔秒数，<=0 表示不打印
     */
    private OperatorMetrics(String name, int printIntervalSeconds) {
        this.name = name;
        this.printIntervalSeconds = printIntervalSeconds;

        // 初始化桶
        for (int i = 0; i < BUCKET_COUNT; i++) {
            bucketSum[i] = new LongAdder();
            bucketCount[i] = new LongAdder();
        }
    }

    /**
     * 创建 Metrics (不启用定时打印)
     */
    public static OperatorMetrics create(MetricGroup metricGroup, String name) {
        return create(metricGroup, name, 0);
    }

    /**
     * 创建 Metrics (启用定时打印)
     * @param metricGroup Flink MetricGroup
     * @param name 指标名称
     * @param printIntervalSeconds 打印间隔秒数，<=0 表示不打印
     */
    public static OperatorMetrics create(MetricGroup metricGroup, String name, int printIntervalSeconds) {
        OperatorMetrics m = new OperatorMetrics(name, printIntervalSeconds);
        m.register(metricGroup);

        if (printIntervalSeconds > 0) {
            m.startPrintTask();
        }

        return m;
    }

    private void register(MetricGroup root) {
        MetricGroup g = root.addGroup("etl");

        flinkInCounter = g.counter("in");
        g.gauge("out", (Gauge<Long>) outCount::sum);
        g.gauge("error", (Gauge<Long>) errorCount::sum);
        g.gauge("skip", (Gauge<Long>) skipCount::sum);

        g.meter("qps", new MeterView(flinkInCounter, 60));

        g.gauge("cache_hit_rate", (Gauge<Double>) () -> {
            long hits = cacheHitCount.sum();
            long total = hits + cacheMissCount.sum();
            return total == 0 ? 0.0 : (double) hits / total * 100;
        });

        g.gauge("latency_avg_ms", (Gauge<Double>) this::calcAvgLatency);
        g.gauge("latency_max_ms", (Gauge<Long>) latencyMax::get);
    }

    /**
     * 启动定时打印任务
     */
    private void startPrintTask() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-printer-" + name);
            t.setDaemon(true);
            return t;
        });

        printTask = scheduler.scheduleAtFixedRate(
                this::printMetrics,
                printIntervalSeconds,
                printIntervalSeconds,
                TimeUnit.SECONDS
        );

        LOG.info("[{}] 定时打印任务已启动，间隔 {} 秒", name, printIntervalSeconds);
    }

    /**
     * 打印指标
     */
    private void printMetrics() {
        try {
            long now = System.currentTimeMillis();
            long elapsed = now - lastPrintTime;
            double elapsedSec = elapsed / 1000.0;

            long currentIn = inCount.sum();
            long currentOut = outCount.sum();
            long currentError = errorCount.sum();
            long currentSkip = skipCount.sum();

            // 计算区间增量
            long deltaIn = currentIn - lastPrintIn;
            long deltaOut = currentOut - lastPrintOut;
            long deltaError = currentError - lastPrintError;
            long deltaSkip = currentSkip - lastPrintSkip;

            // 计算 QPS (区间内的每秒处理数)
            double qpsIn = elapsedSec > 0 ? deltaIn / elapsedSec : 0;
            double qpsOut = elapsedSec > 0 ? deltaOut / elapsedSec : 0;

            // 缓存命中率
            double cacheHitRate = getCacheHitRate();

            // 延迟
            double avgLatency = calcAvgLatency();
            long maxLatency = latencyMax.get();

            LOG.info("╔════════════════════════════════════════════════════════════╗");
            LOG.info("║  [{}] 性能指标报告", name);
            LOG.info("╠════════════════════════════════════════════════════════════╣");
            LOG.info("║  时间窗口: {} 秒", String.format("%.1f", elapsedSec));
            LOG.info("╠════════════════════════════════════════════════════════════╣");
            LOG.info("║  吞吐量:");
            LOG.info("║    输入 QPS: {} /s (本次 +{})", String.format("%,.0f", qpsIn), deltaIn);
            LOG.info("║    输出 QPS: {} /s (本次 +{})", String.format("%,.0f", qpsOut), deltaOut);
            LOG.info("║    错误数: {} (本次 +{})", currentError, deltaError);
            LOG.info("║    跳过数: {} (本次 +{})", currentSkip, deltaSkip);
            LOG.info("╠════════════════════════════════════════════════════════════╣");
            LOG.info("║  延迟:");
            LOG.info("║    平均延迟: {} ms", String.format("%.2f", avgLatency));
            LOG.info("║    最大延迟: {} ms", maxLatency);
            LOG.info("╠════════════════════════════════════════════════════════════╣");
            LOG.info("║  缓存:");
            LOG.info("║    命中率: {}%", String.format("%.2f", cacheHitRate));
            LOG.info("║    命中: {}, 未命中: {}", cacheHitCount.sum(), cacheMissCount.sum());
            LOG.info("╠════════════════════════════════════════════════════════════╣");
            LOG.info("║  累计: in={}, out={}, error={}, skip={}",
                    currentIn, currentOut, currentError, currentSkip);
            LOG.info("╚════════════════════════════════════════════════════════════╝");

            // 更新上次打印的值
            lastPrintIn = currentIn;
            lastPrintOut = currentOut;
            lastPrintError = currentError;
            lastPrintSkip = currentSkip;
            lastPrintTime = now;

        } catch (Exception e) {
            LOG.warn("打印指标失败", e);
        }
    }

    // ========== 核心方法 ==========

    public void in() {
        inCount.increment();
        flinkInCounter.inc();
        startTime.set(System.currentTimeMillis());
    }

    public void out() {
        outCount.increment();
        recordLatency();
    }

    public void error() {
        errorCount.increment();
        recordLatency();
    }

    public void skip() {
        skipCount.increment();
    }

    // ========== 缓存统计 ==========

    public void cacheHit() {
        cacheHitCount.increment();
    }

    public void cacheMiss() {
        cacheMissCount.increment();
    }

    // ========== 内部方法 ==========

    private void recordLatency() {
        Long start = startTime.get();
        if (start == null) return;

        long latencyMs = System.currentTimeMillis() - start;
        long nowSec = System.currentTimeMillis() / 1000;
        int bucket = (int) (nowSec % BUCKET_COUNT);

        // 清理过期桶 (CAS 保证只有一个线程清理)
        long lastSec = lastBucketTime.get();
        if (nowSec > lastSec && lastBucketTime.compareAndSet(lastSec, nowSec)) {
            for (long s = lastSec + 1; s <= nowSec && s <= lastSec + BUCKET_COUNT; s++) {
                int b = (int) (s % BUCKET_COUNT);
                bucketSum[b].reset();
                bucketCount[b].reset();
            }
        }

        // 写入当前桶 (无锁)
        bucketSum[bucket].add(latencyMs);
        bucketCount[bucket].increment();

        // 更新最大值
        long max = latencyMax.get();
        while (latencyMs > max) {
            if (latencyMax.compareAndSet(max, latencyMs)) break;
            max = latencyMax.get();
        }
    }

    private double calcAvgLatency() {
        long totalSum = 0;
        long totalCount = 0;

        for (int i = 0; i < BUCKET_COUNT; i++) {
            totalSum += bucketSum[i].sum();
            totalCount += bucketCount[i].sum();
        }

        return totalCount == 0 ? 0.0 : (double) totalSum / totalCount;
    }

    // ========== 获取统计值 ==========

    public long getIn() { return inCount.sum(); }
    public long getOut() { return outCount.sum(); }
    public long getError() { return errorCount.sum(); }
    public long getSkip() { return skipCount.sum(); }

    public double getCacheHitRate() {
        long hits = cacheHitCount.sum();
        long total = hits + cacheMissCount.sum();
        return total == 0 ? 0.0 : (double) hits / total * 100;
    }

    public double getAvgLatency() {
        return calcAvgLatency();
    }

    public long getMaxLatency() {
        return latencyMax.get();
    }

    /**
     * 关闭定时任务 - 必须在算子 close() 时调用
     */
    public void shutdown() {
        if (printTask != null) {
            printTask.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 最后打印一次汇总
        LOG.info("[{}] 最终统计: in={}, out={}, error={}, skip={}, cacheHitRate={}%, avgLatency={}ms",
                name, getIn(), getOut(), getError(), getSkip(),
                String.format("%.2f", getCacheHitRate()),
                String.format("%.2f", getAvgLatency()));
    }
}