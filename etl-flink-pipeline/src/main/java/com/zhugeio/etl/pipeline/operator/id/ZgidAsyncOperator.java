package com.zhugeio.etl.pipeline.operator.id;

import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.pipeline.archive.ArchiveKafkaService;
import com.zhugeio.etl.pipeline.enums.ArchiveType;
import com.zhugeio.etl.pipeline.service.OneIdService;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 诸葛ID异步映射算子 - 修复版
 *
 * 修复点:
 * 1. 使用 Map<String, Object> 访问数据，而非强转 JSONObject
 * 2. 与 UserPropAsyncOperator 保持一致的数据访问方式
 */
public class ZgidAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ZgidAsyncOperator.class);
    private final ArchiveKafkaService archiveKafkaService = ArchiveKafkaService.getInstance();
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
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "zgid_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        OneIdService.initialize(kvrocksHost, kvrocksPort, kvrocksCluster);
        oneIdService = OneIdService.getInstance();

        LOG.info("[ZgidAsyncOperator-{}] 初始化成功",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        metrics.in();

        if (input.getResult() == -1) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        try {
            //  修复: 使用 Map 方式访问数据
            Map<String, Object> data = (Map<String, Object>) input.getData();
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
            if (!(dataListObj instanceof List)) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            List<Map<String, Object>> dataArray = (List<Map<String, Object>>) dataListObj;
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (Map<String, Object> item : dataArray) {
                if (item == null) continue;

                Map<String, Object> pr = (Map<String, Object>) item.get("pr");
                if (pr == null) continue;

                // 获取 $zg_did (必须)
                Long zgDeviceId = parseLong(pr.get("$zg_did"));
                if (zgDeviceId == null) {
                    continue;
                }

                // 获取 $zg_uid (可选)
                Long zgUserId = parseLong(pr.get("$zg_uid"));
                LOG.debug("$zg_did: {}, zg_uid: {}", zgDeviceId, zgUserId);
                // 异步获取或创建诸葛ID
                CompletableFuture<Void> future = oneIdService.getOrCreateZgid(appId, zgDeviceId, zgUserId)
                        .thenAccept(oneIdResult -> {
                            if (oneIdResult != null) {
                                LOG.debug("$zg_did: {}, zg_uid: {},oneIdResult: {}", zgDeviceId, zgUserId,oneIdResult);
                                Long zgId = oneIdResult.getId();
                                // 消息添加 zgId
                                pr.put("$zg_zgid", zgId);
                                // 之前的逻辑就是这么增加zgid的
                                input.setZgId(zgId);

                                // 是否需要输出 id 之间的映射关系至 kafka
                                Boolean isNew = oneIdResult.getIsNew();
                                if(isNew){
                                    for (ArchiveType archiveType : oneIdResult.getArchiveTypes()) {
                                        switch (archiveType){
                                            case DEVICE_ZGID: {
                                                archiveKafkaService.sendToKafka(ArchiveType.DEVICE_ZGID, appId,String.valueOf(zgDeviceId), zgId);
                                                break;
                                            }
                                            case USER_ZGID: {
                                                archiveKafkaService.sendToKafka(ArchiveType.USER_ZGID, appId,String.valueOf(zgUserId), zgId);
                                                break;
                                            }
                                            case ZGID_USER: {
                                                archiveKafkaService.sendToKafka(ArchiveType.ZGID_USER, appId,String.valueOf(zgId),zgUserId);
                                                break;
                                            }
                                        }
                                    }
                                }
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

    /**
     * 安全解析Long值
     */
    private Long parseLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
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
}
