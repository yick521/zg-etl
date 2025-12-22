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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * User-Agent 解析富化算子
 */
public class UserAgentEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserAgentEnrichOperator.class);

    private transient UserAgentParser parser;
    private transient Cache<String, UserAgentInfo> uaCache;

    // ========== 新增: Metrics ==========
    private transient OperatorMetrics metrics;

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

        // ========== 新增: 初始化 Metrics ==========
        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ua_enrich");

        parser = new UserAgentParser();

        uaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();

        LOG.info("[UA富化算子-{}] 初始化成功, 缓存大小={}, 过期时间={}分钟",
                subtaskIndex, cacheSize, expireMinutes);
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        // ========== 新增: 记录输入 ==========
        metrics.in();

        CompletableFuture.supplyAsync(() -> {
            try {
                Map<String, Object> data = message.getData();
                if (data == null) {
                    metrics.skip();  // 新增
                    return message;
                }

                Object uaObj = data.get("ua");
                if (uaObj == null) {
                    metrics.skip();  // 新增
                    return message;
                }

                String ua = String.valueOf(uaObj);
                if (ua.isEmpty() || "null".equals(ua)) {
                    metrics.skip();  // 新增
                    return message;
                }

                // 先查缓存
                UserAgentInfo uaInfo = uaCache.getIfPresent(ua);

                if (uaInfo == null) {
                    metrics.cacheMiss();  // 新增
                    uaInfo = parser.parse(ua);
                    uaCache.put(ua, uaInfo);
                } else {
                    metrics.cacheHit();  // 新增
                }

                // 将结果写入 data
                if (uaInfo != null) {
                    data.put("os", uaInfo.getOsName());
                    data.put("os_version", uaInfo.getOsVersion());
                    data.put("browser", uaInfo.getBrowserName());
                    data.put("browser_version", uaInfo.getBrowserVersion());
                    data.put("device_type", uaInfo.getDeviceType());

                    if (uaInfo.getDeviceBrand() != null) {
                        data.put("brand", uaInfo.getDeviceBrand());
                    }
                    if (uaInfo.getDeviceModel() != null) {
                        data.put("model", uaInfo.getDeviceModel());
                    }
                }

                metrics.out();  // 新增

            } catch (Exception e) {
                LOG.error("[UA富化算子] 处理失败: {}", e.getMessage());
                metrics.error();  // 新增
            }

            return message;

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOG.error("[UA富化算子] 异步处理异常", throwable);
                metrics.error();  // 新增
                resultFuture.complete(Collections.singleton(message));
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[UA富化算子] 处理超时, offset={}", message.getOffset());
        metrics.error();  // 新增
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        // ========== 新增: 打印 Metrics ==========
        LOG.info("[UA富化算子] Metrics: in={}, out={}, error={}, skip={}, cacheHitRate={}%",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip(),
                String.format("%.2f", metrics.getCacheHitRate()));

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