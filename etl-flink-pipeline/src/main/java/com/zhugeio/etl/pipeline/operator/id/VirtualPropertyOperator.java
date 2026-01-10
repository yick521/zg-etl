package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSON;
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
 * 虚拟属性处理算子 - 修复版
 *
 * 修复点:
 * 1. 使用 Map<String, Object> 访问数据，而非强转 JSONObject
 * 2. 与 UserPropAsyncOperator 保持一致的数据访问方式
 */
public class VirtualPropertyOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualPropertyOperator.class);

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

        LOG.info("[VirtualPropertyOperator-{}] 初始化成功",
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
                if (appId == null || appId == 0) {
                    resultFuture.complete(Collections.singleton(input));
                    return;
                }

                cacheService.hasVirtualProp(appId)
                        .thenCompose(hasVirtualProp -> {
                            LOG.debug("appId: {}, 是否有虚拟属性: {}",appId,hasVirtualProp);
                            if (!hasVirtualProp) {
                                return CompletableFuture.completedFuture(null);
                            }

                            List<CompletableFuture<Void>> futures = new ArrayList<>();

                            Object dataListObj = data.get("data");
                            if (dataListObj instanceof List) {
                                List<Map<String, Object>> dataList = (List<Map<String, Object>>) dataListObj;

                                for (Map<String, Object> dataItem : dataList) {
                                    if (dataItem == null) continue;

                                    String dt = String.valueOf(dataItem.get("dt"));
                                    Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
                                    LOG.debug("appId: {}, dt: {}, pr: {}", appId, dt, pr);

                                    if (pr != null) {
                                        if ("evt".equals(dt) || "abp".equals(dt)) { // 处理事件属性
                                            CompletableFuture<Void> future = handleVirtualEventProps(pr, appId);
                                            futures.add(future);
                                        } else if ("usr".equals(dt)) { // 处理用户属性
                                            CompletableFuture<Void> future = handleVirtualUserProps(pr, appId);
                                            futures.add(future);
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
    private CompletableFuture<Void> handleVirtualEventProps(Map<String, Object> pr, Integer appId) {
        String eventName = String.valueOf(pr.get("$eid"));
        LOG.debug("appId: {}, eventName: {}", appId, eventName);
        if (eventName == null || "null".equals(eventName)) {
            return CompletableFuture.completedFuture(null);
        }

        return cacheService.getVirtualEventProps(appId, eventName)
                .thenAccept(virtualProps -> {
                    LOG.debug("appId: {}, eventName: {}, virtualProps: {}", appId, eventName, virtualProps);
                    if (virtualProps != null && !virtualProps.isEmpty()) {
                        for (Map<String, Object> virtualProp : virtualProps) {
                            try {
                                String name = (String) virtualProp.get("name");
                                String sqlJson = (String) virtualProp.get("sql_json");

                                if (name != null && sqlJson != null) {
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
    private CompletableFuture<Void> handleVirtualUserProps(Map<String, Object> pr, Integer appId) {
        return cacheService.getVirtualUserProps(appId)
                .thenAccept(virtualProps -> {
                    LOG.debug("appId: {}, virtualProps: {}", appId, virtualProps);
                    if (virtualProps != null && !virtualProps.isEmpty()) {
                        for (Map<String, Object> virtualProp : virtualProps) {
                            try {
                                String name = (String) virtualProp.get("name");
                                String sqlJson = (String) virtualProp.get("sql_json");
                                String tableFields = (String) virtualProp.get("table_fields");
                                LOG.debug("appId: {}, name: {}, sqlJson: {}, tableFields: {}", appId, name, sqlJson, tableFields);
                                if (name != null && sqlJson != null) {
                                    boolean allProp = isAllProp(tableFields, pr);
                                    LOG.debug("appId: {}, name: {}, allProp: {}, pr: {}", appId, name, allProp,pr);
                                    if (allProp) {
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
    private boolean isAllProp(String tableFields, Map<String, Object> pr) {
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
     * 处理单个虚拟属性
     */
    private void handleVirtualProp(String virtualPropName, String virtualPropRule, Map<String, Object> pr) {
        try {
            VirtualAttributeExpressionEvaluator evaluator = evaluatorThreadLocal.get();
            ObjectMapper mapper = objectMapperThreadLocal.get();

            // 将 Map 转为 JSON 字符串再解析
            String jsonData = JSON.toJSONString(pr);
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
