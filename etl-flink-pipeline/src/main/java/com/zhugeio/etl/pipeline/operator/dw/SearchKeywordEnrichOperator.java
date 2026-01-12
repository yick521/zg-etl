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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 搜索关键词富化算子
 *
 * 设计说明:
 * KVRocks 客户端提供异步接口 asyncGet()，直接使用，无需额外线程池。
 * 参考 IdJob 中 ZgidAsyncOperator 的写法。
 *
 * 三级缓存:
 * L1: 本地 Caffeine 缓存（命中直接返回）
 * L2: KVRocks 分布式缓存（异步查询）
 * L3: SearchKeywordParser 本地解析
 */
public class SearchKeywordEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SearchKeywordEnrichOperator.class);

    private transient SearchKeywordParser parser;
    private transient KvrocksClient kvrocksClient;
    private transient Cache<String, BaiduKeyword> localCache;
    private transient OperatorMetrics metrics;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean isCluster;
    private final int localCacheSize;
    private final long localCacheExpireMinutes;

    // ★★★ 新增：白名单配置 ★★★
    private final String whiteAppIds;
    private transient Set<String> whiteAppIdSet;

    private transient AtomicLong l1Hits;
    private transient AtomicLong l2Hits;
    private transient AtomicLong l3Hits;

    public SearchKeywordEnrichOperator(String kvrocksHost, int kvrocksPort, boolean isCluster,
                                       int localCacheSize, long localCacheExpireMinutes,
                                       String whiteAppIds) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.isCluster = isCluster;
        this.localCacheSize = localCacheSize;
        this.localCacheExpireMinutes = localCacheExpireMinutes;
        this.whiteAppIds = whiteAppIds;
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

        l1Hits = new AtomicLong(0);
        l2Hits = new AtomicLong(0);
        l3Hits = new AtomicLong(0);

        // ★★★ 初始化白名单 ★★★
        whiteAppIdSet = new HashSet<>();
        if (whiteAppIds != null && !whiteAppIds.trim().isEmpty()) {
            for (String appId : whiteAppIds.split(",")) {
                String trimmed = appId.trim();
                if (!trimmed.isEmpty()) {
                    whiteAppIdSet.add(trimmed);
                }
            }
        }

        LOG.info("[关键词富化算子-{}] 初始化成功, 白名单AppIds: {}", subtaskIndex, whiteAppIdSet);
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

        // ★★★白名单检查 ★★★
        if (!isAppIdInWhiteList(originalData)) {
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

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataArrayObj;

        // 收集所有需要查询的 ref
        List<RefQuery> refQueries = collectRefQueries(dataList);

        if (refQueries.isEmpty()) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(message));
            return;
        }

        // 参考 IdJob 的 ZgidAsyncOperator：收集 futures，统一等待
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (RefQuery query : refQueries) {
            CompletableFuture<Void> future = processRefAsync(query);
            futures.add(future);
        }

        // 等待所有异步操作完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        LOG.error("[关键词富化算子] 批量处理失败", ex);
                        metrics.error();
                    } else {
                        metrics.out();
                    }
                    resultFuture.complete(Collections.singleton(message));
                });
    }

    /**
     * 检查 appId 是否在白名单中
     */
    private boolean isAppIdInWhiteList(Map<String, Object> originalData) {
        // 如果白名单为空，不处理任何应用
        if (whiteAppIdSet == null || whiteAppIdSet.isEmpty()) {
            return false;
        }

        // 如果白名单包含 "-1"，表示处理所有应用（禁用白名单功能）
        if (whiteAppIdSet.contains("-1")) {
            return false;  // -1 表示不启用关键词富化
        }

        // 如果白名单包含 "*" 或 "all"，表示处理所有应用
        if (whiteAppIdSet.contains("*") || whiteAppIdSet.contains("all")) {
            return true;
        }

        // 获取 appId
        Object appIdObj = originalData.get("app_id");
        if (appIdObj == null) {
            return false;
        }

        String appId = String.valueOf(appIdObj);
        return whiteAppIdSet.contains(appId);
    }


    /**
     * 异步处理单个 ref
     */
    private CompletableFuture<Void> processRefAsync(RefQuery query) {
        String ref = query.ref;
        Map<String, Object> pr = query.pr;

        // L1 本地缓存检查（同步，微秒级）
        BaiduKeyword cached = localCache.getIfPresent(ref);
        if (cached != null) {
            l1Hits.incrementAndGet();
            applyKeyword(pr, cached);
            return CompletableFuture.completedFuture(null);
        }

        // L2 KVRocks 异步查询
        String cacheKey = "sk:" + ref;

        return kvrocksClient.asyncGet(cacheKey)
                .thenAccept(kvValue -> {
                    BaiduKeyword keyword;

                    if (kvValue != null && !kvValue.isEmpty()) {
                        // L2 命中
                        l2Hits.incrementAndGet();
                        keyword = parseCacheValue(kvValue, ref);
                    } else {
                        // L3 本地解析
                        l3Hits.incrementAndGet();
                        keyword = parser.parse(ref);

                        // 异步写回 KVRocks
                        if (keyword.isParsed()) {
                            kvrocksClient.asyncSet(cacheKey, buildCacheValue(keyword));
                        }
                    }

                    localCache.put(ref, keyword);
                    applyKeyword(pr, keyword);
                })
                .exceptionally(ex -> {
                    // 降级：KVRocks 失败时本地解析
                    LOG.warn("[关键词富化算子] KVRocks查询失败，降级: {}", ex.getMessage());
                    l3Hits.incrementAndGet();
                    BaiduKeyword keyword = parser.parse(ref);
                    localCache.put(ref, keyword);
                    applyKeyword(pr, keyword);
                    return null;
                });
    }

    @SuppressWarnings("unchecked")
    private List<RefQuery> collectRefQueries(List<Map<String, Object>> dataList) {
        List<RefQuery> queries = new ArrayList<>();

        for (Map<String, Object> dataItem : dataList) {
            if (dataItem == null) continue;

            Object prObj = dataItem.get("pr");
            if (!(prObj instanceof Map)) continue;

            Map<String, Object> pr = (Map<String, Object>) prObj;
            Object refObj = pr.get("$ref");
            if (refObj == null) continue;

            String ref = String.valueOf(refObj);
            if (ref.isEmpty() || "null".equals(ref)) continue;

            queries.add(new RefQuery(ref, pr));
        }

        return queries;
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

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[关键词富化算子] 处理超时");
        metrics.error();
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        LOG.info("[关键词富化算子] Metrics: in={}, out={}, error={}, skip={}",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip());
        LOG.info("[关键词富化算子] 缓存统计: L1={}, L2={}, L3={}",
                l1Hits.get(), l2Hits.get(), l3Hits.get());

        if (kvrocksClient != null) {
            kvrocksClient.shutdown();
        }
    }

    private static class RefQuery {
        final String ref;
        final Map<String, Object> pr;

        RefQuery(String ref, Map<String, Object> pr) {
            this.ref = ref;
            this.pr = pr;
        }
    }
}