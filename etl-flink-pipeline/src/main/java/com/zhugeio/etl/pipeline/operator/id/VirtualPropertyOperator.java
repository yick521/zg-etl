package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.model.Event;
import com.zhugeio.etl.pipeline.model.EventAttr;
import com.zhugeio.etl.pipeline.model.UserPropMeta;
import com.zhugeio.etl.pipeline.operator.id.virtualAttribute.VirtualAttributeExpressionEvaluator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 虚拟属性处理算子
 * <p>
 * 实现逻辑:
 * 1. ✅ 使用缓存减少重复查询
 * 2. ✅ 所有操作都是异步的
 * 3. ✅ 支持Kvrocks集群和单机模式
 * 4. ✅ 实现虚拟事件属性和虚拟用户属性处理
 */
public class VirtualPropertyOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, EventAttr> eventAttrCache;
    private transient Cache<String, Event> eventCache;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private transient ThreadLocal<VirtualAttributeExpressionEvaluator> evaluatorThreadLocal;
    private transient ThreadLocal<ObjectMapper> objectMapperThreadLocal;
    
    // 虚拟属性应用ID集合缓存
    private transient Cache<String, Boolean> virtualPropAppIdsCache;

    private static final Logger LOG = LoggerFactory.getLogger(VirtualPropertyOperator.class);

    public VirtualPropertyOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
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
        eventAttrCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();
        
        eventCache = Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();
        
        virtualPropAppIdsCache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();

        evaluatorThreadLocal = ThreadLocal.withInitial(VirtualAttributeExpressionEvaluator::new);
        objectMapperThreadLocal = ThreadLocal.withInitial(ObjectMapper::new);
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

                // 获取应用ID
                Integer appId = input.getAppId();
                if (appId == null || appId == 0) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }
                
                // 检查应用是否包含虚拟属性
                if (!isVirtualPropApp(appId)) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }

                // 处理data数组中的每个元素
                Object dataListObj = data.get("data");
                if (dataListObj instanceof Iterable) {

                    for (Object dataItemObj : (Iterable<?>) dataListObj) {
                        if (dataItemObj instanceof JSONObject) {
                            JSONObject dataItem = (JSONObject) dataItemObj;

                            String dt = dataItem.getString("dt");
                            JSONObject pr = dataItem.getJSONObject("pr");

                            if (pr != null) {
                                // 处理事件类型的虚拟属性 (evt 或 abp)
                                if ("evt".equals(dt) || "abp".equals(dt)) {
                                    handleVirtualEventProps(pr, appId);
                                }
                                // 处理用户类型的虚拟属性 (usr)
                                else if ("usr".equals(dt)) {
                                    handleVirtualUserProps(pr, appId);
                                }
                            }
                        }
                    }
                }
            }
            
            resultFuture.complete(Collections.singleton(input));
        } catch (Exception e) {
            LOG.error("处理虚拟属性时发生错误", e);
            resultFuture.complete(Collections.singleton(input));
        }
    }

    /**
     * 处理虚拟事件属性
     */
    private void handleVirtualEventProps(JSONObject pr, Integer appId) {
        String eventName = pr.getString("$eid");
        if (eventName == null) {
            return;
        }

        String virtualEventKey = appId + "_eP_" + eventName;
        
        // 查询event_attr表中的虚拟属性
        String eventAttrHashKey = "event_attr";
        CompletableFuture<Map<String, String>> eventAttrFuture = kvrocks.asyncHGetAll(eventAttrHashKey);
        
        // 查询event表中的事件信息
        String eventHashKey = "event";
        CompletableFuture<Map<String, String>> eventFuture = kvrocks.asyncHGetAll(eventHashKey);
        
        try {
            Map<String, String> eventAttrMap = eventAttrFuture.get(5, TimeUnit.SECONDS);
            Map<String, String> eventMap = eventFuture.get(5, TimeUnit.SECONDS);
            
            if (eventAttrMap != null && eventMap != null) {
                // 创建一个临时的event映射，用于模拟SQL关联
                Map<Integer, Event> eventIdToEventMap = new java.util.HashMap<>();
                
                // 先加载event数据
                for (Map.Entry<String, String> entry : eventMap.entrySet()) {
                    String eventStr = entry.getValue();
                    try {
                        Event event = Event.fromJson(eventStr);
                        if (event.getId() != null && event.getAppId() != null && event.getAppId().equals(appId) 
                            && event.getIsDelete() != null && event.getIsDelete() == 0) {
                            eventIdToEventMap.put(event.getId(), event);
                            
                            String eventCacheKey = event.getAppId() + "_" + event.getOwner() + "_" + event.getEventName();
                            eventCache.put(eventCacheKey, event);
                        }
                    } catch (Exception e) {
                        LOG.warn("解析Event JSON失败: {}", eventStr, e);
                    }
                }
                
                // 遍历event_attr hash中的所有条目查找匹配的虚拟属性
                for (Map.Entry<String, String> entry : eventAttrMap.entrySet()) {
                    String attrStr = entry.getValue();
                    try {
                        EventAttr eventAttr = EventAttr.fromJson(attrStr);
                        
                        // 检查是否为虚拟属性且未删除
                        if (eventAttr.getAttrType() != null && eventAttr.getAttrType() == 1
                            && eventAttr.getIsDelete() != null && eventAttr.getIsDelete() == 0) {
                            
                            // 模拟SQL关联: ea.left join e on e.id = ea.event_id
                            Event relatedEvent = eventIdToEventMap.get(eventAttr.getEventId());
                            if (relatedEvent != null) {
                                // 补充关联字段
                                eventAttr.setAppId(relatedEvent.getAppId());
                                eventAttr.setEventName(relatedEvent.getEventName());
                                
                                // 构造缓存key并加入缓存
                                String cacheKey = eventAttr.getAppId() + "_" + eventAttr.getEventId() + "_" 
                                    + eventAttr.getOwner() + "_" + eventAttr.getAttrName();
                                eventAttrCache.put(cacheKey, eventAttr);
                                
                                // 检查是否是我们需要的事件的虚拟属性
                                String key = eventAttr.getAppId() + "_eP_" + eventAttr.getEventName();
                                if (virtualEventKey.equals(key) && eventAttr.getSqlJson() != null) {
                                    // 处理虚拟属性
                                    handleVirtualProp(eventAttr.getAttrName(), eventAttr.getSqlJson(), pr);
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("解析EventAttr JSON失败: {}", attrStr, e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("从KVRocks查询事件属性失败", e);
        }
    }

    /**
     * 处理虚拟用户属性
     */
    private void handleVirtualUserProps(JSONObject pr, Integer appId) {

        // 查询user_prop_meta表中的虚拟属性
        String userPropHashKey = "user_prop_meta";
        CompletableFuture<Map<String, String>> userPropFuture = kvrocks.asyncHGetAll(userPropHashKey);
        
        try {
            Map<String, String> userPropMap = userPropFuture.get(5, TimeUnit.SECONDS);
            if (userPropMap != null) {
                // 遍历user_prop_meta hash中的所有条目查找匹配的虚拟属性
                for (Map.Entry<String, String> entry : userPropMap.entrySet()) {
                    String propStr = entry.getValue();
                    try {
                        UserPropMeta userPropMeta = UserPropMeta.fromJson(propStr);
                        
                        // 检查是否为虚拟属性且属于当前应用
                        if (userPropMeta.getAttrType() != null && userPropMeta.getAttrType() == 1 
                            && userPropMeta.getIsDelete() != null && userPropMeta.getIsDelete() == 0
                            && userPropMeta.getAppId() != null && userPropMeta.getAppId().equals(appId)) {
                            
                            // 检查当前用户数据是否包含所有虚拟属性字段
                            if (isAllProp(userPropMeta.getTableFields(), pr)) {
                                // 处理虚拟属性
                                handleVirtualProp(userPropMeta.getName(), userPropMeta.getSqlJson(), pr);
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn("解析UserProp JSON失败: {}", propStr, e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("从KVRocks查询用户属性失败", e);
        }
    }

    /**
     * 判断当前数据中是否包含所有虚拟属性字段
     */
    private boolean isAllProp(String tableFields, JSONObject pr) {
        if (tableFields == null || tableFields.isEmpty()) {
            return false;
        }

        String[] tableFieldArr = tableFields.split(",");
        for (String field : tableFieldArr) {
            String trimmedField = field.trim();
            if (!trimmedField.isEmpty()) {
                String[] parts = trimmedField.split("\\.");
                String key = "_" + parts[parts.length - 1];
                if (!pr.containsKey(key)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 检查应用是否包含虚拟属性
     */
    private boolean isVirtualPropApp(Integer appId) {
        String appIdStr = appId.toString();
        Boolean cachedResult = virtualPropAppIdsCache.getIfPresent(appIdStr);
        
        if (cachedResult != null) {
            return cachedResult;
        }
        
        // 查询event_attr表中是否存在该应用的虚拟属性
        String eventAttrHashKey = "event_attr";
        CompletableFuture<Map<String, String>> eventAttrFuture = kvrocks.asyncHGetAll(eventAttrHashKey);
        
        // 查询user_prop_meta表中是否存在该应用的虚拟属性
        String userPropMetaHashKey = "user_prop_meta";
        CompletableFuture<Map<String, String>> userPropMetaFuture = kvrocks.asyncHGetAll(userPropMetaHashKey);
        
        try {
            Map<String, String> eventAttrMap = eventAttrFuture.get(5, TimeUnit.SECONDS);
            Map<String, String> userPropMetaMap = userPropMetaFuture.get(5, TimeUnit.SECONDS);
            
            boolean hasVirtualProps = false;
            
            // 检查event_attr中是否存在该应用的虚拟属性
            if (eventAttrMap != null) {
                for (Map.Entry<String, String> entry : eventAttrMap.entrySet()) {
                    String attrStr = entry.getValue();
                    try {
                        EventAttr eventAttr = EventAttr.fromJson(attrStr);
                        
                        if (eventAttr.getAppId() != null && eventAttr.getAppId().equals(appId) 
                            && eventAttr.getAttrType() != null && eventAttr.getAttrType() == 1 
                            && eventAttr.getIsDelete() != null && eventAttr.getIsDelete() == 0) {
                            hasVirtualProps = true;
                            break;
                        }
                    } catch (Exception e) {
                        LOG.warn("解析EventAttr JSON失败: {}", attrStr, e);
                    }
                }
            }
            
            // 如果event_attr中没找到，检查user_prop_meta
            if (!hasVirtualProps && userPropMetaMap != null) {
                for (Map.Entry<String, String> entry : userPropMetaMap.entrySet()) {
                    String propStr = entry.getValue();
                    try {
                        UserPropMeta userPropMeta = UserPropMeta.fromJson(propStr);
                        
                        if (userPropMeta.getAppId() != null && userPropMeta.getAppId().equals(appId) 
                            && userPropMeta.getAttrType() != null && userPropMeta.getAttrType() == 1 
                            && userPropMeta.getIsDelete() != null && userPropMeta.getIsDelete() == 0) {
                            hasVirtualProps = true;
                            break;
                        }
                    } catch (Exception e) {
                        LOG.warn("解析UserPropMeta JSON失败: {}", propStr, e);
                    }
                }
            }
            
            virtualPropAppIdsCache.put(appIdStr, hasVirtualProps);
            return hasVirtualProps;
        } catch (Exception e) {
            LOG.error("检查应用是否包含虚拟属性失败", e);
            return false;
        }
    }
    
    /**
     * 处理单个虚拟属性
     */
    private void handleVirtualProp(String virtualPropName, String virtualPropRule, JSONObject pr) {
        try {
            VirtualAttributeExpressionEvaluator evaluator = evaluatorThreadLocal.get();
            ObjectMapper mapper = objectMapperThreadLocal.get();

            String jsonData = pr.toJSONString();
            JsonNode jsonNode = mapper.readTree(jsonData);
            Object value = evaluator.evaluate(virtualPropRule, jsonNode);

            Object convertedValue = convertValue(value);
            pr.put("_" + virtualPropName, convertedValue);
            
            LOG.debug("处理虚拟属性: {} 规则: {} 结果: {}", virtualPropName, virtualPropRule, convertedValue);
        } catch (Exception e) {
            LOG.error("处理虚拟属性失败: {} 规则: {}", virtualPropName, virtualPropRule, e);
        }
    }
    
    /**
     * 转换值的类型，处理布尔值和数值格式
     */
    private Object convertValue(Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        } else if (value instanceof Double) {
            Double doubleValue = (Double) value;
            if (doubleValue.isInfinite() || doubleValue.isNaN()) {
                return null;
            } else if (doubleValue == doubleValue.longValue()) {
                // 如果是整数，转换为Long避免科学计数法
                return BigDecimal.valueOf(doubleValue.longValue());
            } else {
                // 保留小数，但避免科学计数法
                return BigDecimal.valueOf(doubleValue);
            }
        } else if (value instanceof Float) {
            Float floatValue = (Float) value;
            double doubleVal = floatValue.doubleValue();
            if (floatValue.isInfinite() || floatValue.isNaN()) {
                return null;
            } else if (doubleVal == (long) doubleVal) {
                // 如果是整数，转换为Long避免科学计数法
                return BigDecimal.valueOf((long) doubleVal);
            } else {
                // 保留小数，但避免科学计数法
                return BigDecimal.valueOf(doubleVal);
            }
        } else if (value instanceof BigDecimal) {
            BigDecimal bigDecimalValue = (BigDecimal) value;
            if (bigDecimalValue.stripTrailingZeros().scale() <= 0) {
                // 去除尾随零后，如果小数位数 <= 0，说明是整数
                if (bigDecimalValue.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0 &&
                    bigDecimalValue.compareTo(new BigDecimal(Long.MIN_VALUE)) >= 0) {
                    return bigDecimalValue.longValue();
                } else {
                    return bigDecimalValue.toBigInteger();
                }
            } else {
                return bigDecimalValue;
            }
        }
        
        return value;
    }

    @Override
    public void close() {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (eventAttrCache != null) {
            System.out.printf(
                    "[VirtualPropertyOperator-%d] EventAttr缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    eventAttrCache.stats()
            );
        }
        
        if (eventCache != null) {
            System.out.printf(
                    "[VirtualPropertyOperator-%d] Event缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    eventCache.stats()
            );
        }
        
        if (virtualPropAppIdsCache != null) {
            System.out.printf(
                    "[VirtualPropertyOperator-%d] VirtualPropAppIds缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    virtualPropAppIdsCache.stats()
            );
        }
    }
}