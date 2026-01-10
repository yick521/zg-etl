package com.zhugeio.etl.pipeline.operator.id;

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

/**
 * 设备属性处理算子 - 修复版
 * 
 * 修复点:
 * 1. 使用 Map<String, Object> 访问数据，而非强转 JSONObject
 * 2. 与 UserPropAsyncOperator 保持一致的数据访问方式
 */
public class DevicePropertyOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DevicePropertyOperator.class);
    private static final int MAX_DEV_ATTR_LENGTH = Config.getInt("max_dev_prop_length");

    private transient ConfigCacheService cacheService;

    private CacheConfig cacheConfig;

    public DevicePropertyOperator(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public void open(Configuration parameters) {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);

        LOG.info("[DevicePropertyOperator-{}] 初始化成功",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        try {
            if (input.getResult() != -1) {
                //  修复: 使用 Map 方式访问数据
                Map<String, Object> data = (Map<String, Object>) input.getData();
                if (data == null) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }

                Integer appId = input.getAppId();
                String owner = String.valueOf(data.get("owner"));
                Integer sdk = Dims.sdk(String.valueOf(data.get("pl")));
                LOG.debug("处理设备属性: appId={}, owner={}, sdk={}", appId, owner, sdk);
                Object dataListObj = data.get("data");
                if (dataListObj instanceof List) {
                    List<CompletableFuture<Void>> futures = new ArrayList<>();
                    List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataListObj;

                    for (Map<String, Object> dataItem : dataList) {
                        if (dataItem == null)
                            continue;

                        // 只处理设备属性类型(dt=pl)
                        if ("pl".equals(dataItem.get("dt"))) {
                            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
                            LOG.debug("处理设备属性: pr={}", pr);
                            if (pr != null) {
                                CompletableFuture<Void> future = handleCustomProps(pr, appId, owner, sdk);
                                futures.add(future);
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
    private CompletableFuture<Void> handleCustomProps(Map<String, Object> pr, Integer appId, 
                                                       String owner, Integer sdk) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String key : new HashSet<>(pr.keySet())) {
            if (key.startsWith("_")) {
                String propName = key.substring(1).trim();
                Object propValue = pr.get(key);

                String propType = getObjectType(propValue);

                if ("null".equals(propType)) {
                    LOG.warn("设备属性类型 propType:{} 为空，remove 原始值 key={} ,处理后 propName={} ,propValue={}",propType,key,propName,propValue);
                    pr.remove(key);
                    continue;
                } else if (propName.length() > MAX_DEV_ATTR_LENGTH) {
                    LOG.warn("设备属性名 propName 长度过长，remove 原始值 key={} ,处理后 propName={} ,propValue={}",key,propName,propValue);
                    pr.remove(key);
                    continue;
                }

                CompletableFuture<Void> future = cacheService.getDevicePropId(appId, owner, propName)
                        .thenCompose(propId -> {
                            LOG.debug("处理设备属性: appId={}, owner={}, propName={}, 查询propId={}", appId, owner,propName, propId);
                            if (propId != null) {
                                return cacheService.checkDevicePropPlatform(propId, sdk)
                                        .thenAccept(valid -> {
                                            LOG.debug("设备属性: appId={}, propName={}, propId={}, sdk={}, 是否存在={}", appId, propName, propId, sdk, valid);
                                            if (valid) {
                                                pr.put("$zg_dpid#_" + propName, propId);
                                                pr.put("$zg_dptp#_" + propName, propType);
                                                LOG.debug("pr 添加映射 ($zg_dpid#_{} -> {}), ($zg_dptp#_{} -> {}) , 添加后的pr : {}",propName,propId,propName,propType,pr);
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
