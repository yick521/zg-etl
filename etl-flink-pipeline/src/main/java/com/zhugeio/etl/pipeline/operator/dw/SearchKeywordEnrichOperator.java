package com.zhugeio.etl.pipeline.operator.dw;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.model.BaiduKeyword;
import com.zhugeio.etl.common.util.SearchKeywordParser;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 搜索关键词富化算子
 */
public class SearchKeywordEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SearchKeywordEnrichOperator.class);

    private transient SearchKeywordParser parser;
    private transient KvrocksClient kvrocksClient;
    private transient Cache<String, BaiduKeyword> localCache;

    // ========== 新增: Metrics ==========
    private transient OperatorMetrics metrics;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean isCluster;
    private final int localCacheSize;
    private final long localCacheExpireMinutes;

    private transient long l1Hits;
    private transient long l2Hits;
    private transient long l3Hits;

    public SearchKeywordEnrichOperator(String kvrocksHost, int kvrocksPort, boolean isCluster,
                                       int localCacheSize, long localCacheExpireMinutes) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.isCluster = isCluster;
        this.localCacheSize = localCacheSize;
        this.localCacheExpireMinutes = localCacheExpireMinutes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // ========== 新增: 初始化 Metrics ==========
        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "keyword_enrich");

        parser = new SearchKeywordParser();

        localCache = Caffeine.newBuilder()
                .maximumSize(localCacheSize)
                .expireAfterWrite(localCacheExpireMinutes, TimeUnit.MINUTES)
                .build();

        kvrocksClient = new KvrocksClient(kvrocksHost, kvrocksPort, isCluster);
        kvrocksClient.init();

        l1Hits = l2Hits = l3Hits = 0;

        LOG.info("[关键词富化算子-{}] 初始化成功", getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        // ========== 新增: 记录输入 ==========
        metrics.in();

        Map<String, Object> data = message.getData();
        if (data == null) {
            metrics.skip();  // 新增
            resultFuture.complete(Collections.singleton(message));
            return;
        }

        Object dataArrayObj = data.get("data");
        if (!(dataArrayObj instanceof List)) {
            metrics.skip();  // 新增
            resultFuture.complete(Collections.singleton(message));
            return;
        }

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataArrayObj;

        CompletableFuture.runAsync(() -> {
            try {
                for (Map<String, Object> dataItem : dataList) {
                    Object prObj = dataItem.get("pr");
                    if (!(prObj instanceof Map)) {
                        continue;
                    }

                    Map<String, Object> pr = (Map<String, Object>) prObj;
                    String ref = getStringValue(pr, "$ref");

                    if (ref == null || ref.isEmpty()) {
                        continue;
                    }

                    // L1 缓存查询
                    BaiduKeyword cached = localCache.getIfPresent(ref);
                    if (cached != null) {
                        l1Hits++;
                        metrics.cacheHit();  // 新增
                        applyKeyword(pr, cached);
                        continue;
                    }

                    metrics.cacheMiss();  // 新增

                    // L2 + L3: KVRocks 或实时解析
                    try {
                        String cacheKey = "sk:" + ref;
                        String kvValue = kvrocksClient.asyncGet(cacheKey).get(100, TimeUnit.MILLISECONDS);

                        BaiduKeyword keyword;
                        if (kvValue != null && !kvValue.isEmpty()) {
                            l2Hits++;
                            keyword = parseCacheValue(kvValue, ref);
                        } else {
                            l3Hits++;
                            keyword = parser.parse(ref);
                            if (keyword.isParsed()) {
                                kvrocksClient.asyncSet(cacheKey, buildCacheValue(keyword));
                            }
                        }

                        localCache.put(ref, keyword);
                        applyKeyword(pr, keyword);

                    } catch (Exception e) {
                        l3Hits++;
                        BaiduKeyword keyword = parser.parse(ref);
                        localCache.put(ref, keyword);
                        applyKeyword(pr, keyword);
                    }
                }

                metrics.out();  // 新增

            } catch (Exception e) {
                LOG.error("[关键词富化算子] 处理失败: {}", e.getMessage());
                metrics.error();  // 新增
            }
        }).whenComplete((r, t) -> {
            if (t != null) {
                metrics.error();  // 新增
            }
            resultFuture.complete(Collections.singleton(message));
        });
    }

    private void applyKeyword(Map<String, Object> pr, BaiduKeyword keyword) {
        if (keyword != null && keyword.isParsed()) {
            pr.put("$utm_term", keyword.getKeyword());
            pr.put("$search_engine", keyword.getSearchEngine());
        }
    }

    private BaiduKeyword parseCacheValue(String value, String url) {
        String[] parts = value.split("\\|", 2);
        if (parts.length == 2) {
            return new BaiduKeyword(parts[1], parts[0], url);
        }
        return new BaiduKeyword("", "unknown", url);
    }

    private String buildCacheValue(BaiduKeyword kw) {
        return kw.getSearchEngine() + "|" + kw.getKeyword();
    }

    private String getStringValue(Map<String, Object> map, String key) {
        Object v = map.get(key);
        return v == null ? null : String.valueOf(v);
    }

    @Override
    public void close() throws Exception {
        // ========== 新增: 打印 Metrics ==========
        LOG.info("[关键词富化算子] Metrics: in={}, out={}, error={}, skip={}, cacheHitRate={}%",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip(),
                String.format("%.2f", metrics.getCacheHitRate()));

        if (kvrocksClient != null) {
            kvrocksClient.shutdown();
        }
        LOG.info("[关键词富化算子] 统计: L1={}, L2={}, L3={}", l1Hits, l2Hits, l3Hits);
    }
}