package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.model.DeviceProp;
import com.zhugeio.etl.common.util.Dims;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 设备属性处理算子
 * <p>
 * 实现逻辑:
 * 1. ✅ 使用缓存减少重复查询
 * 2. ✅ 所有操作都是异步的
 * 3. ✅ 支持Kvrocks集群和单机模式
 */
public class DevicePropertyOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, DeviceProp> devicePropCache;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    
    // 最大设备属性名称长度
    private static final int MAX_DEV_ATTR_LENGTH = Config.getInt("max_dev_prop_length");

    private static final Logger LOG = LoggerFactory.getLogger(DevicePropertyOperator.class);

    public DevicePropertyOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
    }

    @Override
    public void open(Configuration parameters) {
        // 初始化KVRocks客户端
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        // 测试连接
        if (!kvrocks.testConnection()) {
            throw new RuntimeException("KVRocks连接失败!");
        }

        // 初始化Caffeine缓存
        devicePropCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        try {
            // 只处理成功的消息
            if (input.getResult() != -1) {
                JSONObject data = (JSONObject) input.getData();
                if (data == null) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }

                // 获取设备属性相关信息
                Integer appId = input.getAppId();
                String owner = data.getString("owner");
                Integer sdk = Dims.sdk(String.valueOf(data.get("pl")));

                // 处理data数组中的每个元素
                Object dataListObj = data.get("data");
                if (dataListObj instanceof Iterable) {

                    for (Object dataItemObj : (Iterable<?>) dataListObj) {
                        if (dataItemObj instanceof JSONObject) {
                            JSONObject dataItem = (JSONObject) dataItemObj;

                            // 只处理设备属性类型(dt=pl)
                            if ("pl".equals(dataItem.getString("dt"))) {
                                JSONObject pr = dataItem.getJSONObject("pr");
                                if (pr != null) {
                                    // 处理以_开头的自定义属性
                                    handleCustomProps(pr, appId, owner, sdk);
                                }
                            }
                        }
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
    private void handleCustomProps(JSONObject pr, Integer appId, String owner, Integer sdk) {
        for (Map.Entry<String, Object> entry : pr.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("_")) {
                String propName = key.substring(1); // 去掉前缀_

                String trimmedPropName = propName.trim();
                if (!trimmedPropName.equals(propName)) {
                    Object value = pr.remove(key);
                    pr.put("_" + trimmedPropName, value);
                    propName = trimmedPropName;
                    key = "_" + propName;
                }

                String propType = getObjectType(entry.getValue());

                if ("null".equals(propType)) {
                    pr.remove(key);
                    continue;
                } else if (propName.length() > MAX_DEV_ATTR_LENGTH) {
                    pr.remove(key);
                    continue;
                }

                String cacheKey = appId + "_" + owner + "_" + propName;

                DeviceProp deviceProp = devicePropCache.getIfPresent(cacheKey);

                if (deviceProp == null) {
                    String hashKey = "device_prop";
                    CompletableFuture<Map<String, String>> future = kvrocks.asyncHGetAll(hashKey);

                    try {
                        Map<String, String> map = future.get(5, TimeUnit.SECONDS);
                        if (map != null) {
                            // 遍历hash中的所有条目查找匹配的属性
                            for (Map.Entry<String, String> hashEntry : map.entrySet()) {
                                String propStr = hashEntry.getValue();
                                try {
                                    DeviceProp prop = DeviceProp.fromJson(propStr);
                                    String propCacheKey = prop.getAppId() + "_" + prop.getOwner() + "_" + prop.getName();

                                    devicePropCache.put(propCacheKey, prop);

                                    if (cacheKey.equals(propCacheKey)) {
                                        deviceProp = prop;
                                    }
                                } catch (Exception e) {
                                    LOG.warn("解析DeviceProp JSON失败: {}", propStr, e);
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("从KVRocks查询设备属性失败", e);
                    }
                }

                // 如果找到了设备属性，添加到pr中
                if (deviceProp != null) {
                    pr.put("$zg_dpid#_" + propName, deviceProp.getId());
                    pr.put("$zg_dptp#_" + propName, propType);
                }
            }
        }
    }

    /**
     * 解析属性类型
     */
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
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (devicePropCache != null) {
            System.out.printf(
                    "[DevicePropertyOperator-%d] 缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    devicePropCache.stats()
            );
        }
    }
}