package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.tool.commons.JsonUtil;
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
                        LOG.debug("是否存在虚拟事件 appId: {}, hasVirtualEvent: {}",appId,hasVirtualEvent);
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
                    CompletableFuture<List<ZGMessage>> future = processOneEvent(input, appId, dt,owner, eventName, pr, dataItem);
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
                    LOG.debug("处理虚拟事件结果  size:{} ,results: {}", results.size(),results);
                    return results;
                });
    }

    /**
     * 处理单个事件的虚拟事件匹配
     */
    private CompletableFuture<List<ZGMessage>> processOneEvent(ZGMessage input, Integer appId,
                                                               String dt,String owner, String eventName,
                                                                Map<String, Object> pr, 
                                                                Map<String, Object> dataItem) {
        return cacheService
                .getVirtualEvents(appId, owner, eventName)
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
                                                            return createVirtualEventMessage(input, virtualName,dt,owner,eventName,virtualAlias, pr, dataItem, attrs);
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
    private ZGMessage createVirtualEventMessage(ZGMessage input, String virtualName,String dt,String owner,String eventName,
                                                  String virtualAlias, Map<String, Object> pr, 
                                                  Map<String, Object> dataItem, Set<String> attrs) {

        try {
            // 满足虚拟事件的条件-生成一条虚拟事件
            String jsonStr = JsonUtil.toJson(input.getData());
            JSONObject jsonObj = JSON.parseObject(jsonStr);
            jsonObj.put("owner", "zg_vtl");

            JSONArray dataArr = jsonObj.getJSONArray("data");
            dataArr.clear();

            String dataJsonStr = JsonUtil.toJson(dataItem);
            JSONObject dataJsonObj = JSON.parseObject(dataJsonStr);
            dataJsonObj.put("dt", "vtl");

            JSONObject prJson = dataJsonObj.getJSONObject("pr");
            prJson.put("$eid", virtualName);
            prJson.put("$virtual_alias", virtualAlias);
            prJson.put("$event_owner", owner);
            prJson.put("$event_name", eventName);
            prJson.put("$event_dt", dt);

            dataArr.add(dataJsonObj);

            String json = jsonObj.toJSONString();
            ZGMessage virtualMessage = new ZGMessage();
            virtualMessage.setTopic("");
            virtualMessage.setPartition(0);
            virtualMessage.setOffset(0L);
            virtualMessage.setKey("");
            virtualMessage.setRawData( json);
            virtualMessage.setAppId(input.getAppId());
            virtualMessage.setSdk(input.getSdk());
            virtualMessage.setZgId(input.getZgId());
            virtualMessage.setZgEid(input.getZgEid());

            java.util.Map<String, Object> map = JsonUtil.mapFromJson(virtualMessage.getRawData());
            if (map == null) {
                virtualMessage.setResult(-1);
                virtualMessage.setError("msg not json");
                virtualMessage.setErrorCode(virtualMessage.getErrorCode() + ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorCode());

                // json解析异常
                String errorInfo = ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorMessage();
                virtualMessage.setErrorDescribe(virtualMessage.getErrorDescribe() + errorInfo);
            } else {
                // 产生的虚拟事件重新生成uuid，事件数据要保证uuid的唯一性
                java.util.List<Object> dataList = (java.util.List<Object>) map.get("data");
                if (dataList != null) {
                    dataList.forEach(dataItemObj -> {
                        java.util.Map<String, Object> dataItemMap = (java.util.Map<String, Object>) dataItemObj;
                        java.util.Map<String, Object> prObject = (java.util.Map<String, Object>) dataItemMap.get("pr");
                        if (prObject != null) {
                            prObject.put("$uuid", java.util.UUID.randomUUID().toString().replaceAll("-", ""));
                        }
                    });
                }
                virtualMessage.setData(map);
            }
            return virtualMessage;
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
