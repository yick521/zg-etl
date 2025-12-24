package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.pipeline.service.OneIdService;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.pipeline.archive.ArchiveKafkaService;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ArchiveType;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
    private final ArchiveKafkaService archiveKafkaService = ArchiveKafkaService.getInstance();

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


        OneIdService.initialize(kvrocksHost, kvrocksPort, kvrocksCluster);
        oneIdService = OneIdService.getInstance();

        LOG.info("[UserIdAsyncOperator-{}] 初始化成功",
                getRuntimeContext().getIndexOfThisSubtask());
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

            // 第一步：收集所有需要处理的cuid，并记录对应的pr对象
            Map<String, List<JSONObject>> cuidToPrMap = new HashMap<>();
            Set<String> cuidSet = new HashSet<>();

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

                // 去重：只处理第一次出现的cuid
                if (cuidSet.add(cuid)) {
                    // 记录这个cuid对应的所有pr对象
                    cuidToPrMap.computeIfAbsent(cuid, k -> new ArrayList<>()).add(pr);
                } else {
                    // 如果是重复的cuid，直接添加到已有列表中
                    cuidToPrMap.get(cuid).add(pr);
                }
            }

            // 如果没有需要处理的cuid
            if (cuidToPrMap.isEmpty()) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 第二步：批量获取用户ID（去重后的cuid）
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (Map.Entry<String, List<JSONObject>> entry : cuidToPrMap.entrySet()) {
                String cuid = entry.getKey();
                List<JSONObject> prList = entry.getValue();

                // 异步获取或创建用户ID
                CompletableFuture<Void> future = oneIdService.getOrCreateUserId(appId, cuid)
                        .thenAccept(oneIdResult -> {
                            if (oneIdResult != null) {
                                Long zgUserId = oneIdResult.getId();

                                // 第三步：回填到所有对应的pr对象中
                                for (JSONObject pr : prList) {
                                    pr.put("$zg_uid", zgUserId);
                                }

                                // 判断是否是新的，如果是新的，发送到Kafka（只发送一次）
                                Boolean isNew = oneIdResult.getIsNew();
                                if (isNew != null && isNew) {
                                    archiveKafkaService.sendToKafka(ArchiveType.USER, appId, cuid, zgUserId);
                                }
                            }
                        })
                        .exceptionally(throwable -> {
                            LOG.error("[UserIdAsyncOperator] 获取用户ID失败: appId={}, cuid={}",
                                    appId, cuid, throwable);
                            return null;
                        });

                futures.add(future);
            }

            // 第四步：等待所有异步操作完成
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

}
