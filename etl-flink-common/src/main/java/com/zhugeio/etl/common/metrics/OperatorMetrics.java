package com.zhugeio.etl.common.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 算子级 Metrics
 *
 * 核心指标:
 * - in: 输入数据量
 * - out: 输出数据量 (处理成功)
 * - error: 处理异常数
 * - skip: 跳过数 (数据不需要处理)
 * - cacheHitRate: 缓存命中率
 * - latency: 处理耗时 (滑动窗口，最近 N 条的平均)
 *
 * 使用:
 * <pre>
 * // open() 中
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
 * </pre>
 *
 *
 * 暴露到 Flink Metrics 的指标:
 *
 * etl.{name}.in              - 输入数量 (Counter)
 * etl.{name}.out             - 输出数量 (Gauge)
 * etl.{name}.error           - 异常数量 (Gauge)
 * etl.{name}.skip            - 跳过数量 (Gauge)
 * etl.{name}.qps             - 每秒处理数 (Meter, 60秒滑动窗口)
 * etl.{name}.cache_hit_rate  - 缓存命中率 % (Gauge)
 * etl.{name}.latency_avg_ms  - 平均延迟 ms (Gauge, 最近1万条)
 * etl.{name}.latency_max_ms  - 最大延迟 ms (Gauge)
 */
public class OperatorMetrics {

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
    // 按秒分桶，保留最近 60 秒的数据
    private static final int BUCKET_COUNT = 60;
    private final LongAdder[] bucketSum = new LongAdder[BUCKET_COUNT];
    private final LongAdder[] bucketCount = new LongAdder[BUCKET_COUNT];
    private final AtomicLong lastBucketTime = new AtomicLong(0);
    private final AtomicLong latencyMax = new AtomicLong(0);

    // 每个线程记录自己的开始时间
    private final ThreadLocal<Long> startTime = new ThreadLocal<>();

    private Counter flinkInCounter;

    private OperatorMetrics(String name) {
        this.name = name;
        // 初始化桶
        for (int i = 0; i < BUCKET_COUNT; i++) {
            bucketSum[i] = new LongAdder();
            bucketCount[i] = new LongAdder();
        }
    }

    public static OperatorMetrics create(MetricGroup metricGroup, String name) {
        OperatorMetrics m = new OperatorMetrics(name);
        m.register(metricGroup);
        return m;
    }

    private void register(MetricGroup root) {
        MetricGroup g = root.addGroup("etl").addGroup(name);

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
}