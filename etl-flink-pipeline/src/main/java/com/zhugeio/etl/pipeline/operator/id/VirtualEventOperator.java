package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 虚拟事件处理算子 - 修复版
 * 
 * 修复点:
 * 1. 使用 Map<String, Object> 访问数据，而非强转 JSONObject
 * 2. 与 UserPropAsyncOperator 保持一致的数据访问方式
 */
public class VirtualEventOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualEventOperator.class);

    private transient ConfigCacheService cacheService;

    private CacheConfig cacheConfig = null;

    public VirtualEventOperator(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public void open(Configuration parameters) {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);

        LOG.info("[VirtualEventOperator-{}] 初始化成功",
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    @SuppressWarnings("unchecked")
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

            cacheService.hasVirtualEvent(appId)
                    .thenCompose(hasVirtualEvent -> {
                        LOG.info("是否存在虚拟事件 appId: {}, hasVirtualEvent: {}",appId,!hasVirtualEvent);
                        if (!hasVirtualEvent) {
                            return CompletableFuture.completedFuture(Collections.singletonList(input));
                        }

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
    @SuppressWarnings("unchecked")
    private CompletableFuture<Collection<ZGMessage>> processVirtualEvents(ZGMessage input, Integer appId) {
        //  修复: 使用 Map 方式访问数据
        Map<String, Object> inputData = (Map<String, Object>) input.getData();
        if (inputData == null) {
            return CompletableFuture.completedFuture(Collections.singletonList(input));
        }

        String oldOwner = String.valueOf(inputData.get("owner"));
        Object dataObj = inputData.get("data");
        LOG.debug("处理虚拟事件 appId: {}, owner: {}, data: {}", appId, oldOwner,dataObj);

        if (!(dataObj instanceof List)) {
            return CompletableFuture.completedFuture(Collections.singletonList(input));
        }

        List<Map<String, Object>> dataArray = (List<Map<String, Object>>) dataObj;
        List<CompletableFuture<List<ZGMessage>>> eventFutures = new ArrayList<>();

        for (Map<String, Object> dataItem : dataArray) {
            if (dataItem == null) {
                continue;
            }

            String dt = String.valueOf(dataItem.get("dt"));
            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");

            if (("evt".equals(dt) || "abp".equals(dt)) && pr != null) {
                String eventName = String.valueOf(pr.get("$eid"));
                String owner = getOwner(dt, oldOwner);
                LOG.debug("处理虚拟事件 appId: {}, owner: {}, eventName: {}, pr: {}", appId, owner, eventName, pr);
                if (eventName != null && !"null".equals(eventName)) {
                    CompletableFuture<List<ZGMessage>> future = processOneEvent(input, appId, owner, eventName, pr, dataItem);
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
                    results.add(input);

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
                                                                Map<String, Object> pr, 
                                                                Map<String, Object> dataItem) {
        return cacheService.getVirtualEvents(appId, owner, eventName)
                .thenCompose(virtualEvents -> {
                    LOG.info("处理虚拟事件 appId: {}, owner: {}, eventName: {}, virtualEvents: {}", appId, owner, eventName, virtualEvents);
                    if (virtualEvents == null || virtualEvents.isEmpty()) {
                        return CompletableFuture.completedFuture(Collections.emptyList());
                    }

                    List<CompletableFuture<ZGMessage>> messageFutures = new ArrayList<>();

                    for (Map<String, Object> virtualEvent : virtualEvents) {
                        try {
                            String virtualName = (String) virtualEvent.get("virtual_name");
                            String virtualAlias = (String) virtualEvent.get("virtual_alias");
                            Object filtersObj = virtualEvent.get("filters");
                            LOG.debug("处理虚拟事件 appId: {}, owner: {}, eventName: {}, virtualName: {}, virtualAlias: {}, filters: {}", appId, owner, eventName, virtualName, virtualAlias, filtersObj);
                            boolean matchFilters = matchFilters(pr, filtersObj);
                            LOG.debug("处理虚拟事件 是否满足过滤条件 matchFilters: {}", matchFilters);
                            if (matchFilters) {
                                CompletableFuture<ZGMessage> msgFuture = 
                                        cacheService
                                                .getVirtualEventAttrs(appId, virtualName, owner, eventName)
                                                .thenApply(attrs -> {
                                                            LOG.debug("获取虚拟事件属性集合 attrs: {}", attrs);
                                                            return createVirtualEventMessage(input, virtualName, virtualAlias, pr, dataItem, attrs);
                                                        }
                                                );

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
    private boolean matchFilters(Map<String, Object> pr, Object filtersObj) {
        if (filtersObj == null) {
            return true;
        }

        try {
            Map<String, Object> filters = null;
            
            if (filtersObj instanceof String) {
                JSONObject jsonFilters = JSON.parseObject((String) filtersObj);
                if (jsonFilters != null) {
                    filters = new HashMap<>(jsonFilters);
                }
            } else if (filtersObj instanceof Map) {
                filters = (Map<String, Object>) filtersObj;
            }

            if (filters == null || filters.isEmpty()) {
                return true;
            }

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
    @SuppressWarnings("unchecked")
    private ZGMessage createVirtualEventMessage(ZGMessage input, String virtualName, 
                                                  String virtualAlias, Map<String, Object> pr, 
                                                  Map<String, Object> dataItem, Set<String> attrs) {
        try {
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

            // 创建新的数据结构
            Map<String, Object> virtualData = new HashMap<>((Map<String, Object>) input.getData());
            List<Map<String, Object>> newDataArray = new ArrayList<>();

            Map<String, Object> newDataItem = new HashMap<>();
            newDataItem.put("dt", dataItem.get("dt"));
            newDataItem.put("ct", dataItem.get("ct"));

            // 创建新的属性对象
            Map<String, Object> newPr = new HashMap<>();
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
            
            virtualMsg.setData(virtualData);

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
