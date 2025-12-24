package com.zhugeio.etl.pipeline.operator.dw;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.model.UserAgentInfo;
import com.zhugeio.etl.common.util.UserAgentParser;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * User-Agent 解析富化算子 - 性能优化版
 *
 * 优化点:
 * 1. 使用专用线程池
 * 2. 预分配 HashMap 容量
 */
public class UserAgentEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserAgentEnrichOperator.class);

    private transient UserAgentParser parser;
    private transient Cache<String, UserAgentInfo> uaCache;
    private transient OperatorMetrics metrics;

    // 【优化】专用线程池
    private transient ExecutorService executorService;

    private final int cacheSize;
    private final long expireMinutes;

    public UserAgentEnrichOperator() {
        this(10000, 60);
    }

    public UserAgentEnrichOperator(int cacheSize, long expireMinutes) {
        this.cacheSize = cacheSize;
        this.expireMinutes = expireMinutes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ua_enrich", 30);

        parser = new UserAgentParser();

        uaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();

        // 【优化】创建专用线程池
        int corePoolSize = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        executorService = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5000),
                r -> {
                    Thread t = new Thread(r, "ua-enrich-" + subtaskIndex);
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        LOG.info("[UA富化算子-{}] 初始化成功, 缓存={}, 过期={}分钟, 线程池={}",
                subtaskIndex, cacheSize, expireMinutes, corePoolSize);
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        metrics.in();

        // 【优化】使用专用线程池
        CompletableFuture.supplyAsync(() -> {
            try {
                Map<String, Object> originalData = message.getData();
                if (originalData == null) {
                    metrics.skip();
                    return message;
                }

                Object uaObj = originalData.get("ua");
                if (uaObj == null) {
                    metrics.skip();
                    return message;
                }

                String ua = String.valueOf(uaObj);
                if (ua.isEmpty() || "null".equals(ua)) {
                    metrics.skip();
                    return message;
                }

                // 先查缓存
                UserAgentInfo uaInfo = uaCache.getIfPresent(ua);

                if (uaInfo == null) {
                    metrics.cacheMiss();
                    uaInfo = parser.parse(ua);
                    uaCache.put(ua, uaInfo);
                } else {
                    metrics.cacheHit();
                }

                if (uaInfo != null) {
                    // 【优化】预分配容量
                    Map<String, Object> newData = new HashMap<>(originalData.size() + 8);
                    newData.putAll(originalData);

                    newData.put("os", uaInfo.getOsName());
                    newData.put("os_version", uaInfo.getOsVersion());
                    newData.put("browser", uaInfo.getBrowserName());
                    newData.put("browser_version", uaInfo.getBrowserVersion());
                    newData.put("device_type", uaInfo.getDeviceType());

                    if (uaInfo.getDeviceBrand() != null) {
                        newData.put("brand", uaInfo.getDeviceBrand());
                    }
                    if (uaInfo.getDeviceModel() != null) {
                        newData.put("model", uaInfo.getDeviceModel());
                    }

                    message.setData(newData);
                }

                metrics.out();

            } catch (Exception e) {
                LOG.error("[UA富化算子] 处理失败: {}", e.getMessage());
                metrics.error();
            }

            return message;

        }, executorService).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOG.error("[UA富化算子] 异步处理异常", throwable);
                metrics.error();
                resultFuture.complete(Collections.singleton(message));
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[UA富化算子] 处理超时, offset={}", message.getOffset());
        metrics.error();
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        LOG.info("[UA富化算子] Metrics: in={}, out={}, error={}, skip={}, cacheHitRate={}%",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip(),
                String.format("%.2f", metrics.getCacheHitRate()));

        // 关闭线程池
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }

        if (uaCache != null) {
            CacheStats stats = uaCache.stats();
            LOG.info("[UA富化算子] 缓存统计: 命中率={}, 请求数={}, 命中数={}, 未命中数={}",
                    String.format("%.2f%%", stats.hitRate() * 100),
                    stats.requestCount(),
                    stats.hitCount(),
                    stats.missCount());
            uaCache.invalidateAll();
        }
    }
}
