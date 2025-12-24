package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.Dims;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 设备属性处理算子 - 改造版 (只读)
 * 
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 
 * KVRocks Key 对照 (与同步服务一致):
 * - appIdDevicePropIdMap (Hash): field=appId_owner_propName
 * - devicePropPlatform (Set): member=propId_platform
 */
public class DevicePropertyOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DevicePropertyOperator.class);
    private static final int MAX_DEV_ATTR_LENGTH = Config.getInt("max_dev_prop_length");

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;

    private CacheConfig cacheConfig;

    public DevicePropertyOperator(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public void open(Configuration parameters) {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);

        LOG.info("[DevicePropertyOperator-{}] 初始化成功 (使用统一缓存服务)",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        try {
            if (input.getResult() != -1) {
                JSONObject data = (JSONObject) input.getData();
                if (data == null) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }

                Integer appId = input.getAppId();
                String owner = data.getString("owner");
                Integer sdk = Dims.sdk(String.valueOf(data.get("pl")));

                Object dataListObj = data.get("data");
                if (dataListObj instanceof Iterable) {
                    List<CompletableFuture<Void>> futures = new ArrayList<>();

                    for (Object dataItemObj : (Iterable<?>) dataListObj) {
                        if (dataItemObj instanceof JSONObject) {
                            JSONObject dataItem = (JSONObject) dataItemObj;

                            // 只处理设备属性类型(dt=pl)
                            if ("pl".equals(dataItem.getString("dt"))) {
                                JSONObject pr = dataItem.getJSONObject("pr");
                                if (pr != null) {
                                    CompletableFuture<Void> future = handleCustomProps(pr, appId, owner, sdk);
                                    futures.add(future);
                                }
                            }
                        }
                    }

                    if (!futures.isEmpty()) {
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                .whenComplete((v, ex) -> {
                                    if (ex != null) {
                                        LOG.error("处理设备属性失败", ex);
                                    }
                                    resultFuture.complete(Collections.singleton(input));
                                });
                        return;
                    }
                }
            }

            resultFuture.complete(Collections.singleton(input));
        } catch (Exception e) {
            LOG.error("处理设备属性时发生错误", e);
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 处理自定义属性
     */
    private CompletableFuture<Void> handleCustomProps(JSONObject pr, Integer appId, String owner, Integer sdk) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, Object> entry : new HashSet<>(pr.entrySet())) {
            String key = entry.getKey();
            if (key.startsWith("_")) {
                String propName = key.substring(1).trim();

                String propType = getObjectType(entry.getValue());

                if ("null".equals(propType)) {
                    pr.remove(key);
                    continue;
                } else if (propName.length() > MAX_DEV_ATTR_LENGTH) {
                    pr.remove(key);
                    continue;
                }

                final String finalKey = key;
                final String finalPropName = propName;

                // 使用统一缓存服务获取设备属性ID
                CompletableFuture<Void> future = cacheService.getDevicePropId(appId, owner, propName)
                        .thenCompose(propId -> {
                            if (propId != null) {
                                // 检查平台
                                return cacheService.checkDevicePropPlatform(propId, sdk)
                                        .thenAccept(valid -> {
                                            if (valid) {
                                                pr.put("$zg_dpid#_" + finalPropName, propId);
                                                pr.put("$zg_dptp#_" + finalPropName, propType);
                                            }
                                        });
                            }
                            return CompletableFuture.completedFuture(null);
                        });

                futures.add(future);
            }
        }

        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    private String getObjectType(Object obj) {
        if (obj == null) {
            return "null";
        } else if (obj instanceof Number) {
            return "number";
        } else if (obj instanceof String) {
            return "string";
        } else {
            return "object";
        }
    }

    @Override
    public void close() {
        LOG.info("[DevicePropertyOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }
}
