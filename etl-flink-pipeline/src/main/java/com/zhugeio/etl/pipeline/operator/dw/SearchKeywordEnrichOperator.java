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

import java.util.*;
import java.util.concurrent.*;

/**
 * 搜索关键词富化算子
 *
 * 优化点:
 * 1. 使用专用线程池
 * 2. 预分配容量
 * 3. 减少对象创建
 */
public class SearchKeywordEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SearchKeywordEnrichOperator.class);

    private transient SearchKeywordParser parser;
    private transient KvrocksClient kvrocksClient;
    private transient Cache<String, BaiduKeyword> localCache;
    private transient OperatorMetrics metrics;

    // 专用线程池
    private transient ExecutorService executorService;

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
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "keyword_enrich", 30);

        parser = new SearchKeywordParser();

        localCache = Caffeine.newBuilder()
                .maximumSize(localCacheSize)
                .expireAfterWrite(localCacheExpireMinutes, TimeUnit.MINUTES)
                .build();

        kvrocksClient = new KvrocksClient(kvrocksHost, kvrocksPort, isCluster);
        kvrocksClient.init();

        // 创建专用线程池
        int corePoolSize = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        executorService = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize * 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(5000),
                r -> {
                    Thread t = new Thread(r, "keyword-enrich-" + subtaskIndex);
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        l1Hits = l2Hits = l3Hits = 0;

        LOG.info("[关键词富化算子-{}] 初始化成功, 线程池={}", subtaskIndex, corePoolSize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        metrics.in();

        Map<String, Object> originalData = message.getData();
        if (originalData == null) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(message));
            return;
        }

        Object dataArrayObj = originalData.get("data");
        if (!(dataArrayObj instanceof List)) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(message));
            return;
        }

        List<Map<String, Object>> originalDataList = (List<Map<String, Object>>) dataArrayObj;

        // 【使用专用线程池
        CompletableFuture.runAsync(() -> {
            try {
                boolean modified = false;

                // 预分配容量
                Map<String, Object> newData = new HashMap<>(originalData.size() + 2);
                newData.putAll(originalData);

                List<Map<String, Object>> newDataList = new ArrayList<>(originalDataList.size());

                for (Map<String, Object> dataItem : originalDataList) {
                    Map<String, Object> newDataItem = new HashMap<>(dataItem.size() + 2);
                    newDataItem.putAll(dataItem);

                    Object prObj = dataItem.get("pr");
                    if (prObj instanceof Map) {
                        Map<String, Object> originalPr = (Map<String, Object>) prObj;
                        String ref = getStringValue(originalPr, "$ref");

                        if (ref != null && !ref.isEmpty()) {
                            Map<String, Object> newPr = new HashMap<>(originalPr.size() + 2);
                            newPr.putAll(originalPr);

                            // L1 缓存查询
                            BaiduKeyword cached = localCache.getIfPresent(ref);
                            if (cached != null) {
                                l1Hits++;
                                metrics.cacheHit();
                                if (applyKeyword(newPr, cached)) {
                                    modified = true;
                                }
                            } else {
                                metrics.cacheMiss();

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
                                    if (applyKeyword(newPr, keyword)) {
                                        modified = true;
                                    }

                                } catch (Exception e) {
                                    l3Hits++;
                                    BaiduKeyword keyword = parser.parse(ref);
                                    localCache.put(ref, keyword);
                                    if (applyKeyword(newPr, keyword)) {
                                        modified = true;
                                    }
                                }
                            }

                            newDataItem.put("pr", newPr);
                        }
                    }

                    newDataList.add(newDataItem);
                }

                if (modified) {
                    newData.put("data", newDataList);
                    message.setData(newData);
                }

                metrics.out();

            } catch (Exception e) {
                LOG.error("[关键词富化算子] 处理失败: {}", e.getMessage());
                metrics.error();
            }
        }, executorService).whenComplete((r, t) -> {
            if (t != null) {
                metrics.error();
            }
            resultFuture.complete(Collections.singleton(message));
        });
    }

    private boolean applyKeyword(Map<String, Object> pr, BaiduKeyword keyword) {
        if (keyword != null && keyword.isParsed()) {
            pr.put("$utm_term", keyword.getKeyword());
            pr.put("$search_engine", keyword.getSearchEngine());
            return true;
        }
        return false;
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
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[关键词富化算子] 处理超时");
        metrics.error();
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        LOG.info("[关键词富化算子] Metrics: in={}, out={}, error={}, skip={}, cacheHitRate={}%",
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

        if (kvrocksClient != null) {
            kvrocksClient.shutdown();
        }

        LOG.info("[关键词富化算子] 统计: L1={}, L2={}, L3={}", l1Hits, l2Hits, l3Hits);
    }
}
