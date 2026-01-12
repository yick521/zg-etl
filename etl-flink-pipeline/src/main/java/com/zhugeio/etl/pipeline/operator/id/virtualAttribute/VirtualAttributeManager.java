package com.zhugeio.etl.pipeline.operator.id.virtualAttribute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zhugeio.etl.common.util.DateTimeResult;


import java.util.HashMap;
import java.util.Map;

/**
 * 虚拟属性管理器 - 仅支持JSON对象的完整解决方案
 */
public class VirtualAttributeManager {

    private final SqlToJsonExpressionConverter converter;
    private final VirtualAttributeExpressionEvaluator evaluator;
    private final ObjectMapper objectMapper;

    public VirtualAttributeManager() {
        this.converter = new SqlToJsonExpressionConverter();
        this.evaluator = new VirtualAttributeExpressionEvaluator();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 从SQL表达式计算虚拟属性值 - 支持JSON字符串
     */
    public Object calculateVirtualAttributeFromJson(String sqlExpression, String jsonData) throws Exception {
        JsonNode jsonNode = objectMapper.readTree(jsonData);
        Object result = calculateVirtualAttribute(sqlExpression, jsonNode);

        // 特殊处理DateTimeResult对象
        if (result instanceof DateTimeResult) {
            return result.toString(); // 调用DateTimeResult的toString()
        }

        return result;
    }

    /**
     * 核心计算方法 - 仅支持JsonNode
     */
    public Object calculateVirtualAttribute(String sqlExpression, JsonNode jsonNode) throws Exception {
        // 转换SQL到JSON
        String jsonExpression = converter.convertSqlToJson(sqlExpression);

        // 执行表达式
        return evaluator.evaluate(jsonExpression, jsonNode);
    }



    /**
     * 获取SQL表达式对应的JSON
     */
    public String getJsonExpression(String sqlExpression) throws Exception {
        return converter.convertSqlToJson(sqlExpression);
    }

    /**
     * 验证SQL表达式是否可以转换
     */
    public boolean validateSqlExpression(String sqlExpression) {
        try {
            converter.convertSqlToJson(sqlExpression);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 批量计算虚拟属性并添加到JSON数据中 - 使用预转换的JSON表达式
     * @param virtualAttributes Map<String, String> key为虚拟属性名称，value为SQL转换后的JSON表达式字符串
     * @param jsonData 原始JSON数据字符串
     * @return 添加了虚拟属性的新JSON字符串
     * 
     */
    public String calculateAndAddVirtualAttributesWithJsonExpressions(Map<String, String> virtualAttributes, String jsonData) throws Exception {
        // 解析原始JSON数据
        JsonNode originalJsonNode = objectMapper.readTree(jsonData);
        ObjectNode resultNode = objectMapper.createObjectNode();
        
        // 复制原始数据
        resultNode.setAll((ObjectNode) originalJsonNode);
        
        // 计算每个虚拟属性
        for (Map.Entry<String, String> entry : virtualAttributes.entrySet()) {
            String attributeName = entry.getKey();
            String jsonExpression = entry.getValue();
            
            try {
                // 直接使用JSON表达式计算虚拟属性值
                Object calculatedValue = evaluator.evaluate(jsonExpression, originalJsonNode);
                
                // 将计算结果添加到JSON中
                if (calculatedValue instanceof Number) {
                    resultNode.putPOJO(attributeName, calculatedValue);
                } else if (calculatedValue instanceof Boolean) {
                    resultNode.put(attributeName, (Boolean) calculatedValue);
                } else {
                    resultNode.put(attributeName, calculatedValue.toString());
                }
                
            } catch (Exception e) {
                // 如果某个虚拟属性计算失败，记录错误但不中断整个处理
                System.err.println("计算虚拟属性 '" + attributeName + "' 失败: " + e.getMessage());
                resultNode.put(attributeName, "计算错误: " + e.getMessage());
            }
        }
        
        // 返回格式化的JSON字符串
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(resultNode);
    }

    /**
     * 使用示例和测试
     */
    public static void main(String[] args) throws Exception {
        VirtualAttributeManager manager = new VirtualAttributeManager();

        // ==================== 基础算术运算测试 ====================
        System.out.println("=== 基础算术运算测试 ===");
        String jsonData = "{\n" +
                "  \"_transaction_price\": 1500.0,\n" +
                "  \"_cost_price\": 1000.0,\n" +
                "  \"_quantity\": 20,\n" +
                "  \"_price\": 1200.0,\n" +
                "  \"_shipping_cost\": 50.0,\n" +
                "  \"_status\": \"completed\",\n" +
                "  \"_amount\": 150.0,\n" +
                "  \"_first_name\": \"张\",\n" +
                "  \"_last_name\": \"三\",\n" +
                "  \"_name\": \"zhangsan\",\n" +
                "  \"_created_at\": \"2023-01-01T00:00:00\",\n" +
                "  \"_profile_json\": \"{\\\"name\\\":\\\"张三\\\",\\\"age\\\":25,\\\"city\\\":\\\"北京\\\"}\",\n" +
                "  \"_details_json\": \"{\\\"profile\\\":{\\\"name\\\":\\\"李四\\\",\\\"level\\\":\\\"VIP\\\"},\\\"address\\\":{\\\"city\\\":\\\"上海\\\"}}\",\n" +
                "  \"_items_json\": \"{\\\"items\\\":[{\\\"id\\\":1,\\\"name\\\":\\\"商品1\\\"},{\\\"id\\\":2,\\\"name\\\":\\\"商品2\\\"}]}\",\n" +
                "  \"_timestamp\": 1704067200,\n" +        // 秒级时间戳
                "  \"_timestamp_ms\": 1704067200000\n" +  // 毫秒级时间戳
                "}";

        // ==================== from_unixtime 函数测试 ====================
        System.out.println("\n=== from_unixtime 函数测试 ===");

        // 测试1: 基本用法 - 返回LocalDateTime对象
//        String sql1 = "from_unixtime(1704067200)";
//        String sql1 = "DATE_FORMAT(from_unixtime(b_user_event_attr_42.分期时间),'yyyy-MM-dd HH:mm:ss')";
        String sql1 = "NOT b_user_event_attr_42.信用卡额度>50000 ";
        System.out.println("\n--- 测试1: 基本用法 ---");
        System.out.println("原始SQL: " + sql1);
        String jsonExpr1 = manager.getJsonExpression(sql1);
        System.out.println("转换后的JSON表达式: " + jsonExpr1);
        Object result1 = manager.calculateVirtualAttributeFromJson(sql1, jsonData);
        System.out.println("计算结果: " + result1);

        // 测试2: 带格式化 - 返回格式化的日期字符串
        String sql2 = "from_unixtime(1704067200, 'yyyy-MM-dd')";
        System.out.println("\n--- 测试2: 带格式化 ---");
        System.out.println("原始SQL: " + sql2);
        String jsonExpr2 = manager.getJsonExpression(sql2);
        System.out.println("转换后的JSON表达式: " + jsonExpr2);
        Object result2 = manager.calculateVirtualAttributeFromJson(sql2, jsonData);
        System.out.println("计算结果: " + result2);

        // 测试3: 使用字段作为参数
        String sql3 = "from_unixtime(event.timestamp)";
        System.out.println("\n--- 测试3: 使用字段参数 ---");
        System.out.println("原始SQL: " + sql3);
        String jsonExpr3 = manager.getJsonExpression(sql3);
        System.out.println("转换后的JSON表达式: " + jsonExpr3);
        Object result3 = manager.calculateVirtualAttributeFromJson(sql3, jsonData);
        System.out.println("计算结果: " + result3);

        // 测试4: 毫秒级时间戳带格式化
        String sql4 = "from_unixtime(event.timestamp_ms, 'yyyy年MM月dd日 HH时mm分ss秒')";
        System.out.println("\n--- 测试4: 毫秒级时间戳带格式化 ---");
        System.out.println("原始SQL: " + sql4);
        String jsonExpr4 = manager.getJsonExpression(sql4);
        System.out.println("转换后的JSON表达式: " + jsonExpr4);
        Object result4 = manager.calculateVirtualAttributeFromJson(sql4, jsonData);
        System.out.println("计算结果: " + result4);

        // 测试5: 与其他函数组合使用
        String sql5 = "CONCAT('日期: ', from_unixtime(event.timestamp, 'yyyy-MM-dd'), ' 时间: ', from_unixtime(event.timestamp, 'HH:mm:ss'))";
        System.out.println("\n--- 测试5: 复杂嵌套使用 ---");
        System.out.println("原始SQL: " + sql5);
        String jsonExpr5 = manager.getJsonExpression(sql5);
        System.out.println("转换后的JSON表达式: " + jsonExpr5);
        Object result5 = manager.calculateVirtualAttributeFromJson(sql5, jsonData);
        System.out.println("计算结果: " + result5);

        // 测试6: 在CASE WHEN条件中使用
        String sql6 = "CASE WHEN from_unixtime(event.timestamp) > from_unixtime(1672531200) THEN '新记录' ELSE '旧记录' END";
        System.out.println("\n--- 测试6: 在CASE WHEN条件中使用 ---");
        System.out.println("原始SQL: " + sql6);
        String jsonExpr6 = manager.getJsonExpression(sql6);
        System.out.println("转换后的JSON表达式: " + jsonExpr6);
        Object result6 = manager.calculateVirtualAttributeFromJson(sql6, jsonData);
        System.out.println("计算结果: " + result6);

    }

    private static Map<String, String> getStringStringMap() {
        Map<String, String> virtualAttributesWithJson = new HashMap<>();
        virtualAttributesWithJson.put("profit", "{\"type\":\"operation\",\"operator\":\"subtract\",\"operands\":[{\"type\":\"field\",\"field\":\"transaction_price\"},{\"type\":\"field\",\"field\":\"cost_price\"}]}");
        virtualAttributesWithJson.put("profit_rate", "{\"type\":\"operation\",\"operator\":\"multiply\",\"operands\":[{\"type\":\"operation\",\"operator\":\"divide\",\"operands\":[{\"type\":\"operation\",\"operator\":\"subtract\",\"operands\":[{\"type\":\"field\",\"field\":\"transaction_price\"},{\"type\":\"field\",\"field\":\"cost_price\"}]},{\"type\":\"field\",\"field\":\"cost_price\"}]},{\"type\":\"constant\",\"dataType\":\"number\",\"value\":100}]}");
        virtualAttributesWithJson.put("full_name", "{\"type\":\"function\",\"function\":\"concat\",\"parameters\":[{\"type\":\"field\",\"field\":\"first_name\"},{\"type\":\"constant\",\"dataType\":\"string\",\"value\":\" \"},{\"type\":\"field\",\"field\":\"last_name\"}]}");
        virtualAttributesWithJson.put("name_upper", "{\"type\":\"function\",\"function\":\"upper\",\"parameters\":[{\"type\":\"field\",\"field\":\"name\"}]}");
        virtualAttributesWithJson.put("price_level", "{\"type\":\"condition\",\"conditions\":[{\"condition\":{\"type\":\"operation\",\"operator\":\"greater_than\",\"operands\":[{\"type\":\"field\",\"field\":\"transaction_price\"},{\"type\":\"constant\",\"dataType\":\"number\",\"value\":1000}]},\"value\":{\"type\":\"constant\",\"dataType\":\"string\",\"value\":\"high\"}}],\"defaultValue\":{\"type\":\"constant\",\"dataType\":\"string\",\"value\":\"low\"}}");
        virtualAttributesWithJson.put("extracted_name", "{\"type\":\"function\",\"function\":\"get_json_string\",\"parameters\":[{\"type\":\"field\",\"field\":\"profile_json\"},{\"type\":\"constant\",\"dataType\":\"string\",\"value\":\"$.name\"}]}");
        return virtualAttributesWithJson;
    }
}

