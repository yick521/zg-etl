package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.OneIdService;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 用户ID异步映射算子 (优化版)
 * 
 * ✅ 优化点:
 * 1. 使用 OneIdService 统一ID管理
 * 2. 使用 Hash 结构存储，与原 Scala 一致
 * 3. 使用雪花算法生成ID
 * 4. 通过 CacheServiceFactory 管理单例
 * 5. 集成 OperatorMetrics 监控
 * 
 * Hash结构: user_id:{appId} field={cuid} value={zgUserId}
 */
public class UserIdAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(UserIdAsyncOperator.class);

    private transient OneIdService oneIdService;
    private transient OperatorMetrics metrics;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final int metricsInterval;

    public UserIdAsyncOperator() {
        this(null, 0, true, 60);
    }

    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort) {
        this(kvrocksHost, kvrocksPort, true, 60);
    }

    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, 60);
    }

    public UserIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, int metricsInterval) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.metricsInterval = metricsInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 Metrics
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "user_id_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .build();

        int workerId = generateWorkerId();
        oneIdService = CacheServiceFactory.getOneIdService("user-id", cacheConfig, workerId);

        LOG.info("[UserIdAsyncOperator-{}] 初始化成功, workerId={}", 
                getRuntimeContext().getIndexOfThisSubtask(), workerId);
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        metrics.in();

        if (input.getResult() == -1) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        try {
            JSONObject data = (JSONObject) input.getData();
            if (data == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            Integer appId = input.getAppId();
            if (appId == null || appId == 0) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 获取 data 数组
            Object dataListObj = data.get("data");
            if (!(dataListObj instanceof JSONArray)) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            JSONArray dataArray = (JSONArray) dataListObj;
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            int processedCount = 0;

            for (int i = 0; i < dataArray.size(); i++) {
                JSONObject item = dataArray.getJSONObject(i);
                if (item == null) continue;

                JSONObject pr = item.getJSONObject("pr");
                if (pr == null) continue;

                // 检查是否有 $cuid
                if (!pr.containsKey("$cuid") || pr.get("$cuid") == null) {
                    pr.remove("$cuid");
                    continue;
                }

                String cuid = String.valueOf(pr.get("$cuid")).trim();
                if (StringUtils.isBlank(cuid)) {
                    pr.remove("$cuid");
                    continue;
                }

                // 规范化 $cuid
                pr.put("$cuid", cuid);
                processedCount++;

                // 异步获取或创建用户ID
                CompletableFuture<Void> future = oneIdService.getOrCreateUserId(appId, cuid)
                        .thenAccept(zgUserId -> {
                            if (zgUserId != null) {
                                pr.put("$zg_uid", zgUserId);
                            }
                        })
                        .exceptionally(throwable -> {
                            LOG.error("[UserIdAsyncOperator] 获取用户ID失败: appId={}, cuid={}", 
                                    appId, cuid, throwable);
                            return null;
                        });

                futures.add(future);
            }

            // 等待所有异步操作完成
            if (futures.isEmpty()) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
            } else {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .whenComplete((v, throwable) -> {
                            if (throwable != null) {
                                LOG.error("[UserIdAsyncOperator] 批量处理用户ID时出错", throwable);
                                metrics.error();
                            } else {
                                metrics.out();
                            }
                            resultFuture.complete(Collections.singleton(input));
                        });
            }

        } catch (Exception e) {
            LOG.error("[UserIdAsyncOperator] 处理异常", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[UserIdAsyncOperator] 处理超时");
        metrics.error();
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        if (metrics != null) {
            metrics.shutdown();
        }
        LOG.info("[UserIdAsyncOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }

    private int generateWorkerId() {
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            int slotId = getRuntimeContext().getIndexOfThisSubtask();
            return Math.abs(("user_" + hostName + "_" + slotId).hashCode()) % 1024;
        } catch (Exception e) {
            return getRuntimeContext().getIndexOfThisSubtask() % 1024;
        }
    }
}
