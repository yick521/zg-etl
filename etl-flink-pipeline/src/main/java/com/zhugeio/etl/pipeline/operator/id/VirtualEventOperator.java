package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.OperatorUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 虚拟事件处理算子 - 改造版 (只读)
 * 
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 * 
 * KVRocks Key 对照 (与同步服务一致):
 * - virtualEventAppidsSet (Set): member=appId - 检查应用是否有虚拟事件
 * - virtualEventMap (Hash): field=appId_owner_eventName - 虚拟事件配置
 * - virtualEventAttrMap (Hash): field=appId_virtualEventName_owner_eventName - 虚拟事件属性
 */
public class VirtualEventOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualEventOperator.class);

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;

    private CacheConfig cacheConfig = null;

    public VirtualEventOperator(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public void open(Configuration parameters) {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);

        LOG.info("[VirtualEventOperator-{}] 初始化成功 (使用统一缓存服务)",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        try {
            if (input.getResult() == -1) {
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            int appId = input.getAppId();
            if (appId <= 0) {
                resultFuture.complete(Collections.singleton(input));
                return;
            }

            // 使用统一缓存服务检查应用是否有虚拟事件
            cacheService.hasVirtualEvent(appId)
                    .thenCompose(hasVirtualEvent -> {
                        if (!hasVirtualEvent) {
                            return CompletableFuture.completedFuture(Collections.singletonList(input));
                        }

                        // 处理虚拟事件
                        return processVirtualEvents(input, appId);
                    })
                    .whenComplete((outputs, throwable) -> {
                        if (throwable != null) {
                            LOG.error("处理虚拟事件失败", throwable);
                            resultFuture.complete(Collections.singleton(input));
                        } else {
                            resultFuture.complete(outputs);
                        }
                    });

        } catch (Exception e) {
            LOG.error("VirtualEventOperator处理异常", e);
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 处理虚拟事件
     */
    private CompletableFuture<Collection<ZGMessage>> processVirtualEvents(ZGMessage input, Integer appId) {
        JSONObject inputData = (JSONObject) input.getData();
        if (inputData == null) {
            return CompletableFuture.completedFuture(Collections.singletonList(input));
        }

        String oldOwner = inputData.getString("owner");
        Object dataObj = inputData.get("data");

        if (!(dataObj instanceof JSONArray)) {
            return CompletableFuture.completedFuture(Collections.singletonList(input));
        }

        JSONArray dataArray = (JSONArray) dataObj;
        List<CompletableFuture<List<ZGMessage>>> eventFutures = new ArrayList<>();

        // 遍历每个事件数据项
        for (int i = 0; i < dataArray.size(); i++) {
            JSONObject dataItem = dataArray.getJSONObject(i);
            if (dataItem == null) {
                continue;
            }

            String dt = dataItem.getString("dt");
            JSONObject pr = dataItem.getJSONObject("pr");

            if (("evt".equals(dt) || "abp".equals(dt)) && pr != null) {
                String eventName = pr.getString("$eid");
                String owner = getOwner(dt, oldOwner);

                if (eventName != null) {
                    CompletableFuture<List<ZGMessage>> future = processOneEvent(
                            input, appId, owner, eventName, pr, dataItem);
                    eventFutures.add(future);
                }
            }
        }

        if (eventFutures.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.singletonList(input));
        }

        return CompletableFuture.allOf(eventFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    List<ZGMessage> results = new ArrayList<>();
                    results.add(input); // 原始消息

                    for (CompletableFuture<List<ZGMessage>> future : eventFutures) {
                        try {
                            List<ZGMessage> virtualMessages = future.join();
                            results.addAll(virtualMessages);
                        } catch (Exception e) {
                            LOG.warn("获取虚拟事件结果失败", e);
                        }
                    }

                    return results;
                });
    }

    /**
     * 处理单个事件的虚拟事件匹配
     */
    private CompletableFuture<List<ZGMessage>> processOneEvent(ZGMessage input, Integer appId, 
                                                                String owner, String eventName, 
                                                                JSONObject pr, JSONObject dataItem) {
        // 使用统一缓存服务获取虚拟事件配置
        return cacheService.getVirtualEvents(appId, owner, eventName)
                .thenCompose(virtualEvents -> {
                    if (virtualEvents == null || virtualEvents.isEmpty()) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }

                    List<CompletableFuture<ZGMessage>> messageFutures = new ArrayList<>();

                    for (Map<String, Object> virtualEvent : virtualEvents) {
                        try {
                            String virtualName = (String) virtualEvent.get("virtual_name");
                            String virtualAlias = (String) virtualEvent.get("virtual_alias");
                            Object filtersObj = virtualEvent.get("filters");

                            // 检查过滤条件
                            if (matchFilters(pr, filtersObj)) {
                                // 获取虚拟事件的属性集合
                                CompletableFuture<ZGMessage> msgFuture = 
                                        cacheService.getVirtualEventAttrs(appId, virtualName, owner, eventName)
                                                .thenApply(attrs -> {
                                                    ZGMessage virtualMsg = createVirtualEventMessage(
                                                            input, virtualName, virtualAlias, pr, dataItem, attrs);
                                                    return virtualMsg;
                                                });

                                messageFutures.add(msgFuture);
                            }
                        } catch (Exception e) {
                            LOG.warn("处理虚拟事件配置失败", e);
                        }
                    }

                    if (messageFutures.isEmpty()) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }

                    return CompletableFuture.allOf(messageFutures.toArray(new CompletableFuture[0]))
                            .thenApply(v -> {
                                List<ZGMessage> results = new ArrayList<>();
                                for (CompletableFuture<ZGMessage> future : messageFutures) {
                                    try {
                                        ZGMessage msg = future.join();
                                        if (msg != null) {
                                            results.add(msg);
                                        }
                                    } catch (Exception e) {
                                        LOG.warn("获取虚拟事件消息失败", e);
                                    }
                                }
                                return results;
                            });
                });
    }

    /**
     * 检查过滤条件
     */
    private boolean matchFilters(JSONObject pr, Object filtersObj) {
        if (filtersObj == null) {
            return true;
        }

        try {
            JSONObject filters;
            if (filtersObj instanceof String) {
                filters = JSON.parseObject((String) filtersObj);
            } else if (filtersObj instanceof JSONObject) {
                filters = (JSONObject) filtersObj;
            } else {
                return true;
            }

            if (filters == null || filters.isEmpty()) {
                return true;
            }

            // 实现过滤逻辑
            for (String key : filters.keySet()) {
                Object filterValue = filters.get(key);
                Object prValue = pr.get("_" + key);
                if (prValue == null) {
                    prValue = pr.get(key);
                }

                if (!Objects.equals(filterValue, prValue)) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            LOG.warn("解析过滤条件失败", e);
            return true;
        }
    }

    /**
     * 创建虚拟事件消息
     */
    private ZGMessage createVirtualEventMessage(ZGMessage input, String virtualName, 
                                                  String virtualAlias, JSONObject pr, 
                                                  JSONObject dataItem, Set<String> attrs) {
        try {
            // 创建新的虚拟事件消息
            ZGMessage virtualMsg = new ZGMessage();
            virtualMsg.setAppId(input.getAppId());
            virtualMsg.setSdk(input.getSdk());
            virtualMsg.setBusiness(input.getBusiness());
            virtualMsg.setResult(input.getResult());
            virtualMsg.setTopic(input.getTopic());
            virtualMsg.setPartition(input.getPartition());
            virtualMsg.setOffset(input.getOffset());
            virtualMsg.setKey(input.getKey());
            virtualMsg.setRawData(input.getRawData());
            virtualMsg.setAppKey(input.getAppKey());
            virtualMsg.setJson(input.getJson());
            virtualMsg.setError(input.getError());
            virtualMsg.setErrorCode(input.getErrorCode());
            virtualMsg.setErrorDescribe(input.getErrorDescribe());

            JSONObject virtualData = (JSONObject) virtualMsg.getData();
            JSONArray newDataArray = new JSONArray();

            JSONObject newDataItem = new JSONObject();
            newDataItem.put("dt", dataItem.getString("dt"));
            newDataItem.put("ct", dataItem.get("ct"));

            // 创建新的属性对象
            JSONObject newPr = new JSONObject();
            newPr.put("$eid", virtualName);
            if (virtualAlias != null) {
                newPr.put("$alias", virtualAlias);
            }

            // 复制指定的属性
            if (attrs != null && !attrs.isEmpty()) {
                for (String attrName : attrs) {
                    Object value = pr.get("_" + attrName);
                    if (value == null) {
                        value = pr.get(attrName);
                    }
                    if (value != null) {
                        newPr.put("_" + attrName, value);
                    }
                }
            }

            newDataItem.put("pr", newPr);
            newDataArray.add(newDataItem);
            virtualData.put("data", newDataArray);

            return virtualMsg;
        } catch (Exception e) {
            LOG.error("创建虚拟事件消息失败", e);
            return null;
        }
    }

    private String getOwner(String dt, String defaultOwner) {
        if ("abp".equals(dt)) {
            return "zg";
        }
        return defaultOwner;
    }

    @Override
    public void close() {
        LOG.info("[VirtualEventOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }
}
