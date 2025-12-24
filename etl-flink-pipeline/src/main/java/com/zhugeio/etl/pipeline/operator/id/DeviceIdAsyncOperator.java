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

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * 设备ID异步映射算子 (优化版)
 * 
 * ✅ 优化点:
 * 1. 使用 OneIdService 统一ID管理
 * 2. 使用 Hash 结构存储，与原 Scala 一致
 * 3. 使用雪花算法生成ID
 * 4. 通过 CacheServiceFactory 管理单例
 * 5. 集成 OperatorMetrics 监控
 * 
 * Hash结构: device_id:{appId} field={deviceMd5} value={zgDeviceId}
 */
public class DeviceIdAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DeviceIdAsyncOperator.class);

    private transient OneIdService oneIdService;
    private transient OperatorMetrics metrics;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final Properties configProperties;
    private final int metricsInterval;

    public DeviceIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, null, 60);
    }

    public DeviceIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, Properties configProperties) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, configProperties, 60);
    }

    public DeviceIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, 
                                  Properties configProperties, int metricsInterval) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.configProperties = configProperties;
        this.metricsInterval = metricsInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化 Metrics
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "device_id_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        // 使用 CacheServiceFactory 获取或创建 OneIdService
        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .build();

        // 生成 workerId，确保分布式环境下唯一
        int workerId = generateWorkerId();
        
        oneIdService = CacheServiceFactory.getOneIdService("device-id", cacheConfig, workerId);

        LOG.info("[DeviceIdAsyncOperator-{}] 初始化成功, KVRocks: {}:{}, workerId={}",
                getRuntimeContext().getIndexOfThisSubtask(), kvrocksHost, kvrocksPort, workerId);
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        metrics.in();

        // 跳过错误消息
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

            // 获取设备标识
            JSONObject usr = data.getJSONObject("usr");
            if (usr == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            String did = usr.getString("did");
            if (StringUtils.isBlank(did)) {
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

            // 使用 OneIdService 获取或创建设备ID
            oneIdService.getOrCreateDeviceId(appId, did)
                    .thenAccept(zgDeviceId -> {
                        if (zgDeviceId != null) {
                            // 设置 $zg_did 到 usr 对象
                            usr.put("$zg_did", zgDeviceId);

                            // 同时设置到 data 数组中的每个 pr 对象
                            Object dataListObj = data.get("data");
                            if (dataListObj instanceof JSONArray) {
                                JSONArray dataArray = (JSONArray) dataListObj;
                                for (int i = 0; i < dataArray.size(); i++) {
                                    JSONObject item = dataArray.getJSONObject(i);
                                    if (item != null) {
                                        JSONObject pr = item.getJSONObject("pr");
                                        if (pr != null) {
                                            pr.put("$zg_did", zgDeviceId);
                                        }
                                    }
                                }
                            }
                            metrics.out();
                        } else {
                            metrics.skip();
                        }
                        resultFuture.complete(Collections.singleton(input));
                    })
                    .exceptionally(throwable -> {
                        LOG.error("[DeviceIdAsyncOperator] 获取设备ID失败: appId={}, did={}", 
                                appId, did, throwable);
                        metrics.error();
                        resultFuture.complete(Collections.singleton(input));
                        return null;
                    });

        } catch (Exception e) {
            LOG.error("[DeviceIdAsyncOperator] 处理异常", e);
            metrics.error();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[DeviceIdAsyncOperator] 处理超时: partition={}, offset={}", 
                input.getPartition(), input.getOffset());
        metrics.error();
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void close() throws Exception {
        if (metrics != null) {
            metrics.shutdown();
        }
        // 注意: 不要在这里关闭 oneIdService，因为它是由 CacheServiceFactory 管理的单例
        LOG.info("[DeviceIdAsyncOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }

    /**
     * 生成 workerId
     */
    private int generateWorkerId() {
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            int slotId = getRuntimeContext().getIndexOfThisSubtask();
            // 使用 "device" 前缀区分不同类型的算子
            return Math.abs(("device_" + hostName + "_" + slotId).hashCode()) % 1024;
        } catch (Exception e) {
            return getRuntimeContext().getIndexOfThisSubtask() % 1024;
        }
    }
}
