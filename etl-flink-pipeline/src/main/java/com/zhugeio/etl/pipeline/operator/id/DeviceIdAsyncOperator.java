package com.zhugeio.etl.pipeline.operator.id;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 设备ID异步映射算子 - 修复版
 * 
 * 修复点:
 * 1. 使用 Map<String, Object> 访问数据，而非强转 JSONObject
 * 2. 与 UserPropAsyncOperator 保持一致的数据访问方式
 */
public class DeviceIdAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DeviceIdAsyncOperator.class);

    private transient OneIdService oneIdService;
    private transient OperatorMetrics metrics;

    private final ArchiveKafkaService archiveKafkaService = ArchiveKafkaService.getInstance();

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
        metrics = OperatorMetrics.create(
                getRuntimeContext().getMetricGroup(),
                "device_id_" + getRuntimeContext().getIndexOfThisSubtask(),
                metricsInterval
        );

        OneIdService.initialize(kvrocksHost, kvrocksPort, kvrocksCluster);
        oneIdService = OneIdService.getInstance();

        LOG.info("[DeviceIdAsyncOperator-{}] 初始化成功, KVRocks: {}:{}, workerId={}",
                getRuntimeContext().getIndexOfThisSubtask(), kvrocksHost, kvrocksPort);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        metrics.in();
        LOG.debug("DeviceIdAsyncOperator input: {}",  input);
        // 跳过错误消息
        if (input.getResult() == -1) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        try {
            //  修复: 使用 Map 方式访问数据
            Map<String, Object> data = (Map<String, Object>) input.getData();
            LOG.debug("DeviceIdAsyncOperator data: {}",  data);
            if (data == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 获取设备标识
            Map<String, Object> usr = (Map<String, Object>) data.get("usr");
            LOG.debug("DeviceIdAsyncOperator usr: {}",  usr);
            if (usr == null) {
                metrics.skip();
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            String did = String.valueOf(usr.get("did"));
            LOG.debug("DeviceIdAsyncOperator did: {}",  did);
            if (StringUtils.isBlank(did) || "null".equals(did)) {
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
                    .thenAccept(oneIdResult -> {
                        LOG.debug("DeviceIdAsyncOperator oneIdResult: {}",  oneIdResult);
                        if (oneIdResult != null) {
                            Long zgDeviceId = oneIdResult.getId();
                            // 设置 $zg_did 到 usr 对象
                            usr.put("$zg_did", zgDeviceId);

                            // 同时设置到 data 数组中的每个 pr 对象
                            Object dataListObj = data.get("data");
                            if (dataListObj instanceof List) {
                                List<Map<String, Object>> dataArray = (List<Map<String, Object>>) dataListObj;
                                for (Map<String, Object> item : dataArray) {
                                    if (item != null) {
                                        Map<String, Object> pr = (Map<String, Object>) item.get("pr");
                                        if (pr != null) {
                                            pr.put("$zg_did", zgDeviceId);
                                        }
                                    }
                                }
                            }

                            Boolean isNew = oneIdResult.getIsNew();
                            if (isNew) {
                                archiveKafkaService.sendToKafka(ArchiveType.DEVICE, appId, did, zgDeviceId);
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
                        input.setResult(-1);
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
        LOG.info("[DeviceIdAsyncOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }
}
