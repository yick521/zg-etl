package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.OneIdService;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
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
 * 诸葛ID异步映射算子 (优化版)
 * 
 * ✅ 优化点:
 * 1. 使用 OneIdService 统一ID管理
 * 2. 使用 Hash 结构存储，与原 Scala 一致
 * 3. 完整的用户-设备绑定逻辑
 * 4. 通过 CacheServiceFactory 管理单例
 * 5. 集成 OperatorMetrics 监控
 * 
 * Hash结构:
 * - device_zgid:{appId} field={zgDeviceId} value={zgId}
 * - user_zgid:{appId} field={zgUserId} value={zgId}
 * - zgid_user:{appId} field={zgId} value={zgUserId} (反向映射)
 * 
 * 核心逻辑 (与原Scala一致):
 * 1. 有 zgUserId:
 *    - 用户已有zgId → 绑定设备到用户的zgId
 *    - 用户无zgId，设备有zgId → 绑定用户到设备的zgId
 *    - 都没有 → 创建新zgId，绑定用户和设备
 * 2. 无 zgUserId (匿名):
 *    - 设备有zgId → 返回
 *    - 设备无zgId → 创建新zgId
 */
public class ZgidAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ZgidAsyncOperator.class);

    private transient OneIdService oneIdService;
    private transient OperatorMetrics metrics;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final int metricsInterval;

    public ZgidAsyncOperator() {
        this(null, 0, true, 60);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort) {
        this(kvrocksHost, kvrocksPort, true, 60);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, 60);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, int metricsInterval) {
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
                "zgid_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .build();

        int workerId = generateWorkerId();
        oneIdService = CacheServiceFactory.getOneIdService("zgid", cacheConfig, workerId);

        LOG.info("[ZgidAsyncOperator-{}] 初始化成功, workerId={}", 
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

            for (int i = 0; i < dataArray.size(); i++) {
                JSONObject item = dataArray.getJSONObject(i);
                if (item == null) continue;

                JSONObject pr = item.getJSONObject("pr");
                if (pr == null) continue;

                // 获取 $zg_did (必须)
                Long zgDeviceId = pr.getLong("$zg_did");
                if (zgDeviceId == null) {
                    continue;
                }

                // 获取 $zg_uid (可选)
                Long zgUserId = pr.getLong("$zg_uid");

                // 异步获取或创建诸葛ID
                CompletableFuture<Void> future = oneIdService.getOrCreateZgid(appId, zgDeviceId, zgUserId)
                        .thenAccept(zgId -> {
                            if (zgId != null) {
                                pr.put("$zg_zgid", zgId);
                            }
                        })
                        .exceptionally(throwable -> {
                            LOG.error("[ZgidAsyncOperator] 获取诸葛ID失败: appId={}, deviceId={}, userId={}", 
                                    appId, zgDeviceId, zgUserId, throwable);
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
                                LOG.error("[ZgidAsyncOperator] 批量处理诸葛ID时出错", throwable);
                                metrics.error();
                            } else {
                                metrics.out();
                            }
                            resultFuture.complete(Collections.singleton(input));
                        });
            }

        } catch (Exception e) {
            LOG.error("[ZgidAsyncOperator] 处理异常", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[ZgidAsyncOperator] 处理超时");
        metrics.error();
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        if (metrics != null) {
            metrics.shutdown();
        }
        LOG.info("[ZgidAsyncOperator-{}] 关闭, stats: {}", 
                getRuntimeContext().getIndexOfThisSubtask(),
                oneIdService != null ? oneIdService.getStats() : "N/A");
    }

    private int generateWorkerId() {
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            int slotId = getRuntimeContext().getIndexOfThisSubtask();
            return Math.abs(("zgid_" + hostName + "_" + slotId).hashCode()) % 1024;
        } catch (Exception e) {
            return getRuntimeContext().getIndexOfThisSubtask() % 1024;
        }
    }
}
