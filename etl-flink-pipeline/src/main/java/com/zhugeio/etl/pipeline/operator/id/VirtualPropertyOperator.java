package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.operator.id.virtualAttribute.VirtualAttributeExpressionEvaluator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 虚拟属性处理算子 - 改造版 (只读)
 *
 * 改造点:
 * 1. 移除独立的本地缓存，使用统一的 ConfigCacheService
 * 2. Key/Field 格式与 cache-sync 同步服务保持一致
 *
 * KVRocks Key 对照 (与同步服务一致):
 * - virtualPropAppIdsSet (Set): member=appId - 检查应用是否有虚拟属性
 * - virtualEventPropMap (Hash): field=appId_eP_eventName - 虚拟事件属性
 * - virtualUserPropMap (Hash): field=appId - 虚拟用户属性
 * - eventVirtualAttrIdsSet (Set): member=attrId - 检查是否虚拟属性
 */
public class VirtualPropertyOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualPropertyOperator.class);

    // 使用统一缓存服务
    private transient ConfigCacheService cacheService;
    private CacheConfig cacheConfig;

    private transient ThreadLocal<VirtualAttributeExpressionEvaluator> evaluatorThreadLocal;
    private transient ThreadLocal<ObjectMapper> objectMapperThreadLocal;

    public VirtualPropertyOperator(CacheConfig cacheConfig) {
        this.cacheConfig = cacheConfig;
    }

    @Override
    public void open(Configuration parameters) {
        cacheService = CacheServiceFactory.getInstance(cacheConfig);

        evaluatorThreadLocal = ThreadLocal.withInitial(VirtualAttributeExpressionEvaluator::new);
        objectMapperThreadLocal = ThreadLocal.withInitial(ObjectMapper::new);

        LOG.info("[VirtualPropertyOperator-{}] 初始化成功 (使用统一缓存服务)",
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
                if (appId == null || appId == 0) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }

                // 使用统一缓存服务检查应用是否有虚拟属性
                cacheService.hasVirtualProp(appId)
                        .thenCompose(hasVirtualProp -> {
                            if (!hasVirtualProp) {
                                return CompletableFuture.completedFuture(null);
                            }

                            List<CompletableFuture<Void>> futures = new ArrayList<>();

                            Object dataListObj = data.get("data");
                            if (dataListObj instanceof Iterable) {
                                for (Object dataItemObj : (Iterable<?>) dataListObj) {
                                    if (dataItemObj instanceof JSONObject) {
                                        JSONObject dataItem = (JSONObject) dataItemObj;

                                        String dt = dataItem.getString("dt");
                                        JSONObject pr = dataItem.getJSONObject("pr");

                                        if (pr != null) {
                                            if ("evt".equals(dt) || "abp".equals(dt)) {
                                                CompletableFuture<Void> future = handleVirtualEventProps(pr, appId);
                                                futures.add(future);
                                            } else if ("usr".equals(dt)) {
                                                CompletableFuture<Void> future = handleVirtualUserProps(pr, appId);
                                                futures.add(future);
                                            }
                                        }
                                    }
                                }
                            }

                            if (futures.isEmpty()) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                        })
                        .whenComplete((v, ex) -> {
                            if (ex != null) {
                                LOG.error("处理虚拟属性失败", ex);
                            }
                            resultFuture.complete(Collections.singleton(input));
                        });
                return;
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
    private CompletableFuture<Void> handleVirtualEventProps(JSONObject pr, Integer appId) {
        String eventName = pr.getString("$eid");
        if (eventName == null) {
            return CompletableFuture.completedFuture(null);
        }

        // 使用统一缓存服务获取虚拟事件属性
        return cacheService.getVirtualEventProps(appId, eventName)
                .thenAccept(virtualProps -> {
                    if (virtualProps != null && !virtualProps.isEmpty()) {
                        for (Map<String, Object> virtualProp : virtualProps) {
                            try {
                                String name = (String) virtualProp.get("name");
                                String sqlJson = (String) virtualProp.get("sql_json");

                                if (name != null && sqlJson != null) {
                                    // 处理虚拟属性
                                    handleVirtualProp(name, sqlJson, pr);
                                }
                            } catch (Exception e) {
                                LOG.warn("计算虚拟事件属性失败", e);
                            }
                        }
                    }
                });
    }

    /**
     * 处理虚拟用户属性
     */
    private CompletableFuture<Void> handleVirtualUserProps(JSONObject pr, Integer appId) {
        // 使用统一缓存服务获取虚拟用户属性
        return cacheService.getVirtualUserProps(appId)
                .thenAccept(virtualProps -> {
                    if (virtualProps != null && !virtualProps.isEmpty()) {
                        for (Map<String, Object> virtualProp : virtualProps) {
                            try {
                                String name = (String) virtualProp.get("name");
                                String sqlJson = (String) virtualProp.get("sql_json");
                                String tableFields = (String) virtualProp.get("table_fields");

                                if (name != null && sqlJson != null) {
                                    // 检查是否包含所有必要字段
                                    if (isAllProp(tableFields, pr)) {
                                        // 处理虚拟属性
                                        handleVirtualProp(name, sqlJson, pr);
                                    }
                                }
                            } catch (Exception e) {
                                LOG.warn("计算虚拟用户属性失败", e);
                            }
                        }
                    }
                });
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
     * 处理单个虚拟属性 (与原始代码保持一致)
     */
    private void handleVirtualProp(String virtualPropName, String virtualPropRule, JSONObject pr) {
        try {
            VirtualAttributeExpressionEvaluator evaluator = evaluatorThreadLocal.get();
            ObjectMapper mapper = objectMapperThreadLocal.get();

            String jsonData = pr.toJSONString();
            JsonNode jsonNode = mapper.readTree(jsonData);

            // 调用 evaluator.evaluate(sqlJson, JsonNode)
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
                return BigDecimal.valueOf(doubleValue.longValue());
            } else {
                return BigDecimal.valueOf(doubleValue);
            }
        } else if (value instanceof Float) {
            Float floatValue = (Float) value;
            double doubleVal = floatValue.doubleValue();
            if (floatValue.isInfinite() || floatValue.isNaN()) {
                return null;
            } else if (doubleVal == (long) doubleVal) {
                return BigDecimal.valueOf((long) doubleVal);
            } else {
                return BigDecimal.valueOf(doubleVal);
            }
        } else if (value instanceof BigDecimal) {
            BigDecimal bigDecimalValue = (BigDecimal) value;
            if (bigDecimalValue.stripTrailingZeros().scale() <= 0) {
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
        LOG.info("[VirtualPropertyOperator-{}] 关闭", getRuntimeContext().getIndexOfThisSubtask());
    }
}