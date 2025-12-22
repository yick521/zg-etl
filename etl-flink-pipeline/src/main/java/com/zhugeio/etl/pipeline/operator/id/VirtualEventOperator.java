package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.model.VirtualEvent;
import com.zhugeio.etl.common.util.OperatorUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 虚拟事件处理算子
 * <p>
 * 根据配置的虚拟事件规则，动态生成符合条件的虚拟事件消息
 */
public class VirtualEventOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, Object> virtualEventCache;
    
    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    // 虚拟事件相关的KV键
    private static final String VIRTUAL_EVENT_HASH_KEY = "virtual_event";

    public VirtualEventOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
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
        virtualEventCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .recordStats()
                .build();

        System.out.printf(
                "[VirtualEvent算子-%d] 初始化成功, KVRocks: %s:%d%n",
                getRuntimeContext().getIndexOfThisSubtask(), kvrocksHost, kvrocksPort
        );
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

            // 获取所有虚拟事件配置
            kvrocks.asyncHGetAll(VIRTUAL_EVENT_HASH_KEY).thenCompose(virtualEventMap -> {
                if (virtualEventMap == null || virtualEventMap.isEmpty()) {
                    return CompletableFuture.completedFuture(Collections.singletonList(input));
                }

                // 实现SQL查询逻辑:
                // 1. SQL: select app_id from virtual_event where is_delete=0 and event_status=0 group by app_id
                Set<Integer> validAppIds = new HashSet<>();
                // 2. SQL: select event_name,alias_name,app_id,event_json from virtual_event where is_delete=0 and event_status=0
                List<VirtualEvent> validVirtualEvents = new ArrayList<>();

                for (Map.Entry<String, String> entry : virtualEventMap.entrySet()) {
                    try {
                        VirtualEvent virtualEvent = VirtualEvent.fromJson(entry.getValue());
                        // 实现两个SQL的过滤逻辑
                        if (virtualEvent.getIsDelete() == 0 && virtualEvent.getEventStatus() == 0) {
                            validAppIds.add(virtualEvent.getAppId());
                            validVirtualEvents.add(virtualEvent);
                        }
                    } catch (Exception e) {
                        System.err.println("解析虚拟事件配置失败: " + entry.getValue() + ", 错误: " + e.getMessage());
                    }
                }

                // 检查当前appId是否在有效的虚拟事件appId集合中
                if (!validAppIds.contains(appId)) {
                    return CompletableFuture.completedFuture(Collections.singletonList(input));
                }

                // 过滤出当前appId的虚拟事件
                List<VirtualEvent> appVirtualEvents = validVirtualEvents.stream()
                        .filter(ve -> ve.getAppId() == appId)
                        .collect(Collectors.toList());

                if (appVirtualEvents.isEmpty()) {
                    return CompletableFuture.completedFuture(Collections.singletonList(input));
                }

                // 异步处理虚拟事件逻辑
                return processVirtualEvents(input, appVirtualEvents);
            }).whenComplete((outputs, throwable) -> {
                if (throwable != null) {
                    resultFuture.completeExceptionally(throwable);
                } else {
                    resultFuture.complete(outputs);
                }
            });

        } catch (Exception e) {
            System.err.println("VirtualEventOperator处理异常: " + e.getMessage());
            e.printStackTrace();
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 处理虚拟事件逻辑
     *
     * @param input 原始消息
     * @param virtualEvents 该应用的虚拟事件列表
     * @return 处理后的消息列表（包括原始消息和可能生成的虚拟事件消息）
     */
    private CompletableFuture<Collection<ZGMessage>> processVirtualEvents(ZGMessage input, List<VirtualEvent> virtualEvents) {
        CompletableFuture<Collection<ZGMessage>> future = new CompletableFuture<>();
        
        try {
            Collection<ZGMessage> results = new ArrayList<>();
            results.add(input); // 添加原始消息

            JSONObject inputData = (JSONObject) input.getData();
            if (inputData == null) {
                future.complete(results);
                return future;
            }

            String oldOwner = inputData.getString("owner");
            Object dataObj = inputData.get("data");

            if (!(dataObj instanceof JSONArray)) {
                future.complete(results);
                return future;
            }

            JSONArray dataArray = (JSONArray) dataObj;

            // 遍历每个事件数据项
            for (int i = 0; i < dataArray.size(); i++) {
                JSONObject dataItem = dataArray.getJSONObject(i);
                if (dataItem == null) continue;

                String dt = dataItem.getString("dt");
                JSONObject pr = dataItem.getJSONObject("pr");

                if (("evt".equals(dt) || "abp".equals(dt)) && pr != null) {
                    String eventName = pr.getString("$eid");
                    String owner = oldOwner;

                    // 对于特定的时间格式进行处理
                    if (dataItem.containsKey("dt")) {
                        String dtValue = dataItem.getString("dt");
                        owner = getOwner(dataItem.getString("dt"), oldOwner);
                    }

                    // 遍历该应用的所有虚拟事件配置
                    for (VirtualEvent virtualEvent : virtualEvents) {
                        try {
                            JSONObject eventJsonObj = JSON.parseObject(virtualEvent.getEventJson());
                            if (eventJsonObj == null) continue;

                            String bindOwner = eventJsonObj.getString("owner");
                            String bindEventName = eventJsonObj.getString("eventName");

                            // 检查是否匹配
                            if (Objects.equals(bindOwner, owner) && Objects.equals(bindEventName, eventName)) {
                                String virtualName = virtualEvent.getEventName();
                                String virtualAlias = virtualEvent.getAliasName();
                                JSONObject filters = eventJsonObj.getJSONObject("filters");

                                boolean compareResult = true;
                                if (filters != null) {
                                    JSONArray matchJsonArr = filters.getJSONArray("conditions");
                                    if (matchJsonArr != null && !matchJsonArr.isEmpty()) {
                                        compareResult = getCompareResult(dt, pr, matchJsonArr, input, dataItem);
                                    }
                                }

                                if (compareResult) {
                                    // 满足虚拟事件条件，生成虚拟事件消息
                                    ZGMessage virtualMessage = createVirtualMessage(input, dataItem, virtualName, virtualAlias, owner, eventName, dt);
                                    if (virtualMessage != null) {
                                        results.add(virtualMessage);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("处理虚拟事件配置时出错: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            }

            future.complete(results);
        } catch (Exception e) {
            System.err.println("处理虚拟事件时出错: " + e.getMessage());
            e.printStackTrace();
            future.complete(Collections.singletonList(input));
        }

        return future;
    }

    /**
     * 根据dt和owner获取正确的owner值
     *
     * @param dt    事件类型
     * @param owner 原始owner
     * @return 正确的owner值
     */
    private String getOwner(String dt, String owner) {
        // 这里可以根据业务逻辑进行处理
        // 当前简化处理，直接返回原owner
        return owner;
    }

    /**
     * 比较属性值是否满足条件
     *
     * @param dt            事件类型
     * @param pr            事件属性
     * @param matchJsonArr  匹配条件数组
     * @param msg           原始消息
     * @param dataItem      数据项
     * @return 是否满足条件
     */
    private boolean getCompareResult(String dt, JSONObject pr, JSONArray matchJsonArr, ZGMessage msg, JSONObject dataItem) {
        for (int i = 0; i < matchJsonArr.size(); i++) {
            JSONObject matchJson = matchJsonArr.getJSONObject(i);
            if (matchJson == null) continue;

            String label = matchJson.getString("label");
            String eventAttValue = "";

            if ("abp".equals(dt)) {
                eventAttValue = getStringValue(pr.get("$" + label));
            } else {
                eventAttValue = getStringValue(pr.get("_" + label));
            }

            if ("业务".equals(label)) {
                eventAttValue = msg.getBusiness();
            }

            boolean matchResult = OperatorUtil.compareValue(eventAttValue, matchJson, label);
            if (!matchResult) {
                return false;
            }
        }
        return true;
    }

    /**
     * 安全地获取对象的字符串值
     *
     * @param obj 对象
     * @return 字符串值
     */
    private String getStringValue(Object obj) {
        return obj == null ? "" : String.valueOf(obj);
    }

    /**
     * 创建虚拟事件消息
     *
     * @param originalMsg   原始消息
     * @param originalData  原始数据项
     * @param virtualName   虚拟事件名称
     * @param virtualAlias  虚拟事件别名
     * @param owner         owner
     * @param eventName     原始事件名称
     * @param dt            事件类型
     * @return 虚拟事件消息
     */
    private ZGMessage createVirtualMessage(ZGMessage originalMsg, JSONObject originalData, 
                                          String virtualName, String virtualAlias, 
                                          String owner, String eventName, String dt) {
        try {
            // 创建新的虚拟事件消息
            ZGMessage virtualMessage = new ZGMessage();
            virtualMessage.setAppId(originalMsg.getAppId());
            virtualMessage.setSdk(originalMsg.getSdk());
            virtualMessage.setBusiness(originalMsg.getBusiness());
            virtualMessage.setResult(originalMsg.getResult());
            virtualMessage.setTopic(originalMsg.getTopic());
            virtualMessage.setPartition(originalMsg.getPartition());
            virtualMessage.setOffset(originalMsg.getOffset());
            virtualMessage.setKey(originalMsg.getKey());
            virtualMessage.setRawData(originalMsg.getRawData());

            // 复制原始消息数据并进行修改
            JSONObject virtualData = originalData.clone();
            
            // 修改owner为虚拟事件标识
            virtualData.put("owner", "zg_vtl");

            // 创建新的data数组，只包含当前事件项
            JSONArray newDataArray = new JSONArray();
            JSONObject newDataItem = originalData.clone();
            
            // 修改事件类型为vtl(虚拟事件)
            newDataItem.put("dt", "vtl");
            
            // 修改事件属性
            JSONObject newPr = newDataItem.getJSONObject("pr");
            if (newPr != null) {
                newPr.put("$eid", virtualName);
                newPr.put("$virtual_alias", virtualAlias);
                newPr.put("$event_owner", owner);
                newPr.put("$event_name", eventName);
                newPr.put("$event_dt", dt);
                
                // 重新生成UUID确保唯一性
                newPr.put("$uuid", UUID.randomUUID().toString().replace("-", ""));
            }
            
            newDataArray.add(newDataItem);
            virtualData.put("data", newDataArray);

            virtualMessage.setData(virtualData);
            return virtualMessage;
        } catch (Exception e) {
            System.err.println("创建虚拟事件消息时出错: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (virtualEventCache != null) {
            System.out.printf(
                    "[VirtualEvent算子-%d] 缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    virtualEventCache.stats()
            );
        }
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<ZGMessage> resultFuture) throws Exception {
        System.err.println("VirtualEventOperator处理超时");
        resultFuture.complete(Collections.singleton(input));
    }
}