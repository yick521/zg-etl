package com.zhugeio.etl.pipeline.operator.dw;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.model.UserAgentInfo;
import com.zhugeio.etl.common.util.UserAgentParser;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * User-Agent 解析富化算子
 *
 * 设计说明:
 * UA 解析是纯 CPU 操作，配合 Caffeine 缓存（命中率通常 90%+）
 * 改为 RichMapFunction，简化代码，减少线程切换开销。
 *
 */
public class UserAgentEnrichOperator extends RichMapFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UserAgentEnrichOperator.class);

    private transient UserAgentParser parser;
    private transient Cache<String, UserAgentInfo> uaCache;
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

        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ua_enrich", 30);

        parser = new UserAgentParser();

        uaCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();

        LOG.info("[UA富化算子-{}] 初始化成功（同步版）", subtaskIndex);
    }

    @Override
    public ZGMessage map(ZGMessage message) throws Exception {
        metrics.in();

        Map<String, Object> originalData = message.getData();

        // 跳过条件1: data 为空
        if (originalData == null) {
            metrics.skip();
            return message;
        }

        Object uaObj = originalData.get("ua");

        // 跳过条件2: ua 为空
        if (uaObj == null) {
            metrics.skip();
            return message;
        }

        String ua = String.valueOf(uaObj);

        // 跳过条件3: ua 为空字符串或 "null"
        if (ua.isEmpty() || "null".equals(ua)) {
            metrics.skip();
            return message;
        }

        try {
            // 使用 Caffeine 的 get 方法，缓存命中直接返回，未命中则解析并缓存
            UserAgentInfo uaInfo = uaCache.get(ua, key -> {
                metrics.cacheMiss();
                return parser.parse(key);
            });

            LOG.debug("DEBUG UA解析结果: ua={}, browser={}, browserVersion={}, os={}, osVersion={}",
                    ua,
                    uaInfo != null ? uaInfo.getBrowserName() : "null",
                    uaInfo != null ? uaInfo.getBrowserVersion() : "null",
                    uaInfo != null ? uaInfo.getOsName() : "null",
                    uaInfo != null ? uaInfo.getOsVersion() : "null");

            if (uaInfo != null) {
                Map<String, Object> newData = new HashMap<>(originalData.size() + 8);
                newData.putAll(originalData);

                putIfNotUnknown(newData, "os", uaInfo.getOsName());
                putIfNotUnknown(newData, "os_version", uaInfo.getOsVersion());
                putIfNotUnknown(newData, "browser", uaInfo.getBrowserName());
                putIfNotUnknown(newData, "browser_version", uaInfo.getBrowserVersion());
                putIfNotUnknown(newData, "device_type", uaInfo.getDeviceType());

                LOG.debug("DEBUG UA写入newData: browser_version={}",
                        newData.get("browser_version"));

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
            LOG.error("[UA富化算子] 解析失败: {}", e.getMessage());
            metrics.error();
        }

        return message;
    }

    /**
     * 仅当值不为 UNKNOWN 且不为空时才写入
     */
    private void putIfNotUnknown(Map<String, Object> map, String key, String value) {
        if (value != null && !value.isEmpty() && !"UNKNOWN".equals(value)) {
            map.put(key, value);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("[UA富化算子] Metrics: in={}, out={}, error={}, skip={}",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip());

        if (uaCache != null) {
            CacheStats stats = uaCache.stats();
            LOG.info("[UA富化算子] 缓存统计: 命中率={}, 请求数={}",
                    String.format("%.2f%%", stats.hitRate() * 100),
                    stats.requestCount());
            uaCache.invalidateAll();
        }
    }
}