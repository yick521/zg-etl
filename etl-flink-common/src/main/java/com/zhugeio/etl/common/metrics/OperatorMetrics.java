package com.zhugeio.etl.common.metrics;

import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

import com.codahale.metrics.SlidingTimeWindowReservoir;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 算子级 Metrics
 *
 * 使用:
 * <pre>
 * // open()
 * metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ua_enrich", 60);
 *
 * // 处理逻辑
 * metrics.in();
 * try {
 *     if (skipCondition) { metrics.skip(); return; }
 *     if (cacheHit) { metrics.cacheHit(); } else { metrics.cacheMiss(); }
 *     metrics.out();
 * } catch (Exception e) {
 *     metrics.error();
 * }
 *
 * // close()
 * metrics.shutdown();
 * </pre>
 */
public class OperatorMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorMetrics.class);

    private final String name;

    // ========== Flink Metrics ==========
    private Counter inCounter;
    private Counter outCounter;
    private Counter errorCounter;
    private Counter skipCounter;
    private Counter cacheHitCounter;
    private Counter cacheMissCounter;

    private Meter inMeter;
    private Meter outMeter;

    // 延迟统计 - 使用 Dropwizard Histogram
    private Histogram latencyHistogram;

    // 记录每个线程的开始时间
    private final ThreadLocal<Long> startTime = new ThreadLocal<>();

    // ========== 定时打印 ==========
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> printTask;
    private final int printIntervalSeconds;

    private OperatorMetrics(String name, int printIntervalSeconds) {
        this.name = name;
        this.printIntervalSeconds = printIntervalSeconds;
    }

    public static OperatorMetrics create(MetricGroup metricGroup, String name) {
        return create(metricGroup, name, 0);
    }

    public static OperatorMetrics create(MetricGroup metricGroup, String name, int printIntervalSeconds) {
        OperatorMetrics m = new OperatorMetrics(name, printIntervalSeconds);
        m.register(metricGroup);
        if (printIntervalSeconds > 0) {
            m.startPrintTask();
        }
        return m;
    }

    private void register(MetricGroup root) {
        MetricGroup g = root.addGroup("etl").addGroup(name);

        // Counter
        inCounter = g.counter("in");
        outCounter = g.counter("out");
        errorCounter = g.counter("error");
        skipCounter = g.counter("skip");
        cacheHitCounter = g.counter("cache_hit");
        cacheMissCounter = g.counter("cache_miss");

        // Meter (60秒滑动窗口)
        inMeter = g.meter("in_qps", new MeterView(inCounter, 60));
        outMeter = g.meter("out_qps", new MeterView(outCounter, 60));

        // Histogram (60秒滑动窗口)
        com.codahale.metrics.Histogram dropwizardHistogram = new com.codahale.metrics.Histogram(
                new SlidingTimeWindowReservoir(60, TimeUnit.SECONDS)
        );
        latencyHistogram = g.histogram("latency", new DropwizardHistogramWrapper(dropwizardHistogram));

        // Gauge - 缓存命中率
        g.gauge("cache_hit_rate", (Gauge<Double>) this::getCacheHitRate);
    }

    // ========== 核心方法 ==========

    public void in() {
        inCounter.inc();
        startTime.set(System.currentTimeMillis());
    }

    public void out() {
        outCounter.inc();
        recordLatency();
    }

    public void error() {
        errorCounter.inc();
        recordLatency();
    }

    public void skip() {
        skipCounter.inc();
    }

    public void cacheHit() {
        cacheHitCounter.inc();
    }

    public void cacheMiss() {
        cacheMissCounter.inc();
    }

    private void recordLatency() {
        Long start = startTime.get();
        if (start != null) {
            long latencyMs = System.currentTimeMillis() - start;
            latencyHistogram.update(latencyMs);
        }
    }

    // ========== 获取指标 ==========

    public long getIn() { return inCounter.getCount(); }
    public long getOut() { return outCounter.getCount(); }
    public long getError() { return errorCounter.getCount(); }
    public long getSkip() { return skipCounter.getCount(); }
    public double getInQps() { return inMeter.getRate(); }
    public double getOutQps() { return outMeter.getRate(); }

    public double getCacheHitRate() {
        long hits = cacheHitCounter.getCount();
        long total = hits + cacheMissCounter.getCount();
        return total == 0 ? 0.0 : (double) hits / total * 100;
    }

    // 延迟统计
    public long getLatencyCount() { return latencyHistogram.getCount(); }
    public double getLatencyMean() { return latencyHistogram.getStatistics().getMean(); }
    public long getLatencyMin() { return latencyHistogram.getStatistics().getMin(); }
    public long getLatencyMax() { return latencyHistogram.getStatistics().getMax(); }
    public double getLatencyP50() { return latencyHistogram.getStatistics().getQuantile(0.50); }
    public double getLatencyP95() { return latencyHistogram.getStatistics().getQuantile(0.95); }
    public double getLatencyP99() { return latencyHistogram.getStatistics().getQuantile(0.99); }

    // ========== 定时打印 ==========

    private void startPrintTask() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-printer-" + name);
            t.setDaemon(true);
            return t;
        });
        printTask = scheduler.scheduleAtFixedRate(this::printMetrics, printIntervalSeconds, printIntervalSeconds, TimeUnit.SECONDS);
        LOG.info("[{}] 定时打印已启动，间隔 {}s", name, printIntervalSeconds);
    }

    private void printMetrics() {
        try {
            LOG.info("┌──────────────────────────────────────────────────────────┐");
            LOG.info("│ [{}] 指标报告", name);
            LOG.info("├──────────────────────────────────────────────────────────┤");
            LOG.info("│ 吞吐: in={} ({}/s), out={} ({}/s)",
                    getIn(), fmt(getInQps()), getOut(), fmt(getOutQps()));
            LOG.info("│ 状态: error={}, skip={}", getError(), getSkip());
            LOG.info("├──────────────────────────────────────────────────────────┤");
            LOG.info("│ 延迟: avg={}ms, min={}ms, max={}ms",
                    fmt(getLatencyMean()), getLatencyMin(), getLatencyMax());
            LOG.info("│       p50={}ms, p95={}ms, p99={}ms",
                    fmt(getLatencyP50()), fmt(getLatencyP95()), fmt(getLatencyP99()));
            LOG.info("├──────────────────────────────────────────────────────────┤");
            LOG.info("│ 缓存: hit={}% ({}/{})",
                    fmt(getCacheHitRate()), cacheHitCounter.getCount(), cacheMissCounter.getCount());
            LOG.info("└──────────────────────────────────────────────────────────┘");
        } catch (Exception e) {
            LOG.warn("[{}] 打印失败", name, e);
        }
    }

    private String fmt(double v) { return String.format("%.2f", v); }

    public void shutdown() {
        if (printTask != null) printTask.cancel(false);
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
        LOG.info("[{}] 最终: in={}, out={}, error={}, skip={}, cacheHit={}%, latency(avg={}ms, p99={}ms)",
                name, getIn(), getOut(), getError(), getSkip(),
                fmt(getCacheHitRate()), fmt(getLatencyMean()), fmt(getLatencyP99()));
    }
}