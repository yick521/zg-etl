package com.zhugeio.etl.pipeline.operator.id.virtualAttribute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhugeio.etl.common.util.DateTimeResult;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 虚拟属性表达式解析器 - 仅支持JsonNode对象
 */
public class VirtualAttributeExpressionEvaluator {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 表达式节点接口
     */
    public interface ExpressionNode {
        Object evaluate(JsonNode dataObject) throws Exception;
    }

    /**
     * 字段引用节点 - 仅支持JsonNode
     */
    public static class FieldNode implements ExpressionNode {
        private final String fieldPath;

        public FieldNode(String fieldPath) {
            this.fieldPath = fieldPath;
        }

        @Override
        public Object evaluate(JsonNode dataObject) throws Exception {
            return getFieldValue(dataObject, fieldPath);
        }

        private Object getFieldValue(JsonNode obj, String path) throws Exception {
            String fieldName = "_" + path;
            JsonNode current = obj;

            if (current == null || current.isNull()) return null;

            if (current.has(fieldName)) {
                current = current.get(fieldName);
            } else {
                return null;
            }

            return convertJsonNodeValue(current);
        }

        private Object convertJsonNodeValue(JsonNode node) {
            if (node.isNull()) return null;
            if (node.isBoolean()) return node.asBoolean();
            if (node.isInt()) return node.asInt();
            if (node.isLong()) return node.asLong();
            if (node.isDouble()) return node.asDouble();
            if (node.isTextual()) return node.asText();
            if (node.isArray() || node.isObject()) return node; // 保持为JsonNode用于进一步处理
            return node.asText(); // 默认转为文本
        }
    }

    /**
     * 常量节点
     */
    public static class ConstantNode implements ExpressionNode {
        private final Object value;

        public ConstantNode(Object value) {
            this.value = value;
        }

        @Override
        public Object evaluate(JsonNode dataObject) {
            return value;
        }
    }

    /**
     * 操作节点
     */
    public static class OperationNode implements ExpressionNode {
        private final String operator;
        private final List<ExpressionNode> operands;

        public OperationNode(String operator, List<ExpressionNode> operands) {
            this.operator = operator;
            this.operands = operands;
        }

        @Override
        public Object evaluate(JsonNode dataObject) throws Exception {
            List<Object> values = new ArrayList<>();
            for (ExpressionNode operand : operands) {
                values.add(operand.evaluate(dataObject));
            }

            return executeOperation(operator, values);
        }

        private int compareDates(Object a, Object b) {
            LocalDateTime aDate = extractDateTime(a);
            LocalDateTime bDate = extractDateTime(b);

            if (aDate != null && bDate != null) {
                return aDate.compareTo(bDate);
            }

            // 处理混合类型比较
            if (aDate != null) {
                String bStr = b.toString();
                try {
                    bDate = parseDateTimeString(bStr);
                    if (bDate != null) return aDate.compareTo(bDate);
                } catch (Exception ignore) {}
            }

            if (bDate != null) {
                String aStr = a.toString();
                try {
                    aDate = parseDateTimeString(aStr);
                    if (aDate != null) return aDate.compareTo(bDate);
                } catch (Exception ignore) {}
            }

            // 回退到数值比较
            return compareNumbers(a, b);
        }

        private LocalDateTime parseDateTimeString(String dateTimeStr) {
            if (dateTimeStr == null || dateTimeStr.trim().isEmpty()) {
                return null;
            }

            // 支持多种常见日期时间格式
            DateTimeFormatter[] formatters = {
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                    DateTimeFormatter.ofPattern("yyyy-M-d H:m:s"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                    DateTimeFormatter.ofPattern("yyyy/M/d H:m:s"),
                    DateTimeFormatter.ofPattern("yyyy/M/d"),
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
            };

            for (DateTimeFormatter formatter : formatters) {
                try {
                    return LocalDateTime.parse(dateTimeStr, formatter);
                } catch (Exception e) {
                    // 继续尝试下一个格式
                }
            }

            return null;
        }

        // 提取日期时间对象的辅助方法
        private LocalDateTime extractDateTime(Object obj) {
            if (obj instanceof LocalDateTime) {
                return (LocalDateTime) obj;
            }
            // 添加这一段来处理 DateTimeResult
            if (obj instanceof DateTimeResult) {
                return ((DateTimeResult) obj).getDateTime();
            }
            if (obj instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) obj;
                if ("datetime".equals(map.get("type")) && map.get("value") instanceof LocalDateTime) {
                    return (LocalDateTime) map.get("value");
                }
            }
            return null;
        }


        private Object executeOperation(String op, List<Object> values) {
            switch (op) {
                // 算术运算
                case "add":
                    return addNumbers(values);
                case "subtract":
                    return subtractNumbers(values);
                case "multiply":
                    return multiplyNumbers(values);
                case "divide":
                    return divideNumbers(values);
                case "modulo":
                    return moduloNumbers(values);

                // 比较运算
                case "equals":
                    return isEqual(values.get(0), values.get(1));
                case "not_equals":
                    return !isEqual(values.get(0), values.get(1));
                case "greater_than":
                    return compareDates(values.get(0), values.get(1)) > 0;
                case "less_than":
                    return compareDates(values.get(0), values.get(1)) < 0;
                case "greater_equal":
                    return compareDates(values.get(0), values.get(1)) >= 0;
                case "less_equal":
                    return compareDates(values.get(0), values.get(1)) <= 0;

                // 逻辑运算
                case "and":
                    return values.stream().allMatch(Boolean.TRUE::equals);
                case "or":
                    return values.stream().anyMatch(Boolean.TRUE::equals);
                case "not":
                    return !Boolean.TRUE.equals(values.get(0));

                default:
                    throw new IllegalArgumentException("Unsupported operator: " + op);
            }
        }

        private boolean isEqual(Object a, Object b) {
            if (a instanceof LocalDateTime && b instanceof LocalDateTime) {
                return ((LocalDateTime) a).isEqual((LocalDateTime) b);
            }
            // 处理null值
            if (a == null && b == null) return true;
            if (a == null || b == null) return false;

            // 如果两个对象完全相同
            if (a == b) return true;

            // 如果两个都是数字类型，转换为BigDecimal进行比较
            if (isNumeric(a) && isNumeric(b)) {
                try {
                    BigDecimal bdA = toBigDecimal(a);
                    BigDecimal bdB = toBigDecimal(b);
                    return bdA.compareTo(bdB) == 0;
                } catch (Exception e) {
                    // 如果转换失败，回退到字符串比较
                    return a.toString().equals(b.toString());
                }
            }

            // 对于字符串类型，直接比较
            if (a instanceof String && b instanceof String) {
                return a.equals(b);
            }

            // 对于布尔类型
            if (a instanceof Boolean && b instanceof Boolean) {
                return a.equals(b);
            }

            // 其他情况，转换为字符串进行比较
            return a.toString().equals(b.toString());
        }

        /**
         * 判断对象是否为数字类型
         */
        private boolean isNumeric(Object obj) {
            if (obj instanceof Number) return true;
            if (obj instanceof String) {
                try {
                    new BigDecimal(obj.toString());
                    return true;
                } catch (NumberFormatException e) {
                    return false;
                }
            }
            return false;
        }

        private BigDecimal addNumbers(List<Object> values) {
            return values.stream()
                    .map(this::toBigDecimal)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
        }

        private BigDecimal subtractNumbers(List<Object> values) {
            if (values.size() != 2) {
                throw new IllegalArgumentException("Subtract requires exactly 2 operands");
            }
            return toBigDecimal(values.get(0)).subtract(toBigDecimal(values.get(1)));
        }

        private BigDecimal multiplyNumbers(List<Object> values) {
            return values.stream()
                    .map(this::toBigDecimal)
                    .reduce(BigDecimal.ONE, BigDecimal::multiply);
        }

        private BigDecimal divideNumbers(List<Object> values) {
            if (values.size() != 2) {
                throw new IllegalArgumentException("Divide requires exactly 2 operands");
            }
            BigDecimal divisor = toBigDecimal(values.get(1));
            if (divisor.equals(BigDecimal.ZERO)) {
                throw new ArithmeticException("Division by zero");
            }
            return toBigDecimal(values.get(0)).divide(divisor, 10, RoundingMode.HALF_UP);
        }

        private BigDecimal moduloNumbers(List<Object> values) {
            if (values.size() != 2) {
                throw new IllegalArgumentException("Modulo requires exactly 2 operands");
            }
            return toBigDecimal(values.get(0)).remainder(toBigDecimal(values.get(1)));
        }

        private int compareNumbers(Object a, Object b) {
            return toBigDecimal(a).compareTo(toBigDecimal(b));
        }

        private BigDecimal toBigDecimal(Object value) {
            if (value == null) return BigDecimal.ZERO;

            // 防止 DateTimeResult 被转换为 BigDecimal
            if (value instanceof DateTimeResult) {
                throw new IllegalArgumentException("不能将日期时间对象用于数值运算");
            }

            // 避免将 LocalDateTime 转换为 BigDecimal
            if (value instanceof LocalDateTime) {
                throw new IllegalArgumentException("不能将日期时间对象用于数值运算");
            }
            if (value instanceof BigDecimal) return (BigDecimal) value;
            if (value instanceof Number) {
                return new BigDecimal(value.toString());
            }
            if (value instanceof String) {
                String str = (String) value;
                if (str.trim().isEmpty()) return BigDecimal.ZERO;
                return new BigDecimal(str);
            }
            return new BigDecimal(value.toString());
        }
    }

    /**
     * 函数调用节点
     */
    public static class FunctionNode implements ExpressionNode {
        private final String function;
        private final List<ExpressionNode> parameters;

        public FunctionNode(String function, List<ExpressionNode> parameters) {
            this.function = function;
            this.parameters = parameters;
        }

        @Override
        public Object evaluate(JsonNode dataObject) throws Exception {
            List<Object> paramValues = new ArrayList<>();
            for (ExpressionNode param : parameters) {
                paramValues.add(param.evaluate(dataObject));
            }

            return executeFunction(function, paramValues);
        }

        private Object executeFunction(String func, List<Object> params) {
            switch (func) {
                // 字符串函数
                case "upper":
                    return params.get(0).toString().toUpperCase();
                case "lower":
                    return params.get(0).toString().toLowerCase();
                case "length":
                    return params.get(0).toString().length();
                case "concat":
                    return params.stream()
                            .map(Object::toString)
                            .reduce("", String::concat);
                case "substring":
                    String str = params.get(0).toString();
                    int start = ((Number) params.get(1)).intValue();
                    if (params.size() > 2) {
                        int length = ((Number) params.get(2)).intValue();
                        return str.substring(start - 1, Math.min(start - 1 + length, str.length()));
                    } else {
                        return str.substring(start - 1);
                    }

                // 日期函数
                case "now":
                    return LocalDateTime.now();
                case "date_diff":
                    // 处理第一个参数
                    LocalDateTime date1;
                    Object param1 = params.get(0);
                    if (param1 instanceof LocalDateTime) {
                        date1 = (LocalDateTime) param1;
                    } else if (param1 instanceof DateTimeResult) {
                        date1 = ((DateTimeResult) param1).getDateTime();
                    } else if (param1 instanceof String) {
                        // 尝试将字符串转换为日期时间
                        date1 = parseDateTimeString((String) param1);
                        if (date1 == null) {
                            throw new IllegalArgumentException("无法解析日期字符串: " + param1);
                        }
                    } else if (param1 instanceof Number) {
                        // 新增：处理毫秒时间戳
                        long timestamp = ((Number) param1).longValue();
                        date1 = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
                    } else {
                        throw new IllegalArgumentException("date_diff 的第一个参数必须是日期时间类型或时间戳");
                    }

                    // 处理第二个参数
                    LocalDateTime date2;
                    Object param2 = params.get(1);
                    if (param2 instanceof LocalDateTime) {
                        date2 = (LocalDateTime) param2;
                    } else if (param2 instanceof DateTimeResult) {
                        date2 = ((DateTimeResult) param2).getDateTime();
                    } else if (param2 instanceof String) {
                        // 尝试将字符串转换为日期时间
                        date2 = parseDateTimeString((String) param2);
                        if (date2 == null) {
                            throw new IllegalArgumentException("无法解析日期字符串: " + param2);
                        }
                    } else if (param2 instanceof Number) {
                        // 新增：处理毫秒时间戳
                        long timestamp = ((Number) param2).longValue();
                        date2 = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
                    } else {
                        throw new IllegalArgumentException("date_diff 的第二个参数必须是日期时间类型或时间戳");
                    }

                    return ChronoUnit.DAYS.between(date2, date1);
                case "date_format":
                    // 处理时间戳或LocalDateTime对象
                    LocalDateTime date;
                    Object dateParam = params.get(0);
                    if (dateParam instanceof LocalDateTime) {
                        date = (LocalDateTime) dateParam;
                    } else if (dateParam instanceof Number) {
                        // 处理时间戳（毫秒）
                        long timestamp = ((Number) dateParam).longValue();
                        // 判断是秒级还是毫秒级时间戳
                        if (timestamp > 10000000000L) {
                            // 毫秒级时间戳
                            date = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
                        } else {
                            // 秒级时间戳
                            date = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
                        }
                    }else if (dateParam instanceof DateTimeResult) {
                        date = ((DateTimeResult) dateParam).getDateTime();
                    } else if (dateParam instanceof String) {
                        String dateString = dateParam.toString();
                        // 尝试解析为时间戳
                        try {
                            long timestamp = Long.parseLong(dateString);
                            if (timestamp > 10000000000L) {
                                date = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
                            } else {
                                date = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
                            }
                        } catch (NumberFormatException e) {
                            // 不是时间戳，尝试解析为日期时间字符串
                            // 支持多种常见日期时间格式
                            DateTimeFormatter[] formatters = {
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                                    DateTimeFormatter.ofPattern("yyyy-M-d H:m:s"),
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                                    DateTimeFormatter.ofPattern("yyyy/M/d H:m:s"),
                                    DateTimeFormatter.ofPattern("yyyy/M/d"),
                                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
                            };

                            LocalDateTime parsedDate = null;
                            for (DateTimeFormatter formatter : formatters) {
                                try {
                                    parsedDate = LocalDateTime.parse(dateString, formatter);
                                    break;
                                } catch (Exception ignored) {
                                    // 继续尝试下一个格式
                                }
                            }

                            if (parsedDate != null) {
                                date = parsedDate;
                            } else {
                                throw new IllegalArgumentException("无法解析日期字符串: " + dateString);
                            }
                        }
                    } else {
                        throw new IllegalArgumentException("Unsupported date type: " + dateParam.getClass());
                    }
                    String pattern = params.get(1).toString();
                    return date.format(DateTimeFormatter.ofPattern(pattern));
                case "from_unixtime":
                    if (params.isEmpty() || params.size() > 2) {
                        throw new IllegalArgumentException("from_unixtime函数需要1个或2个参数");
                    }

                    // 解析时间戳参数
                    Object timestampParam = params.get(0);
                    long timestamp;

                    if (timestampParam instanceof Number) {
                        timestamp = ((Number) timestampParam).longValue();
                    } else if (timestampParam instanceof String) {
                        try {
                            timestamp = Long.parseLong((String) timestampParam);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("无效的时间戳格式: " + timestampParam);
                        }
                    } else {
                        throw new IllegalArgumentException("时间戳必须是数字或字符串: " + timestampParam);
                    }

                    // 转换时间戳为LocalDateTime
                    LocalDateTime convertedDate;
                    if (timestamp > 10000000000L) {
                        convertedDate = LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(timestamp),
                                ZoneId.systemDefault()
                        );
                    } else {
                        convertedDate = LocalDateTime.ofInstant(
                                Instant.ofEpochSecond(timestamp),
                                ZoneId.systemDefault()
                        );
                    }

                    // 如果没有格式化参数，返回包含完整信息的 DateTimeResult 对象
                    if (params.size() == 1) {
                        return new DateTimeResult(convertedDate);
                    } else {
                        String pattern1 = params.get(1).toString();
                        return convertedDate.format(DateTimeFormatter.ofPattern(pattern1));
                    }

                // 数学函数
                case "abs":
                    return toBigDecimal(params.get(0)).abs();
                case "round":
                    BigDecimal value = toBigDecimal(params.get(0));
                    int scale = params.size() > 1 ? ((Number) params.get(1)).intValue() : 0;
                    return value.setScale(scale, RoundingMode.HALF_UP);
                case "ceil":
                    return toBigDecimal(params.get(0)).setScale(0, RoundingMode.CEILING);
                case "floor":
                    return toBigDecimal(params.get(0)).setScale(0, RoundingMode.FLOOR);

                // 空值处理函数
                case "coalesce":
                    for (Object param : params) {
                        if (param != null) {
                            return param;
                        }
                    }
                    return null;
                case "nullif":
                    Object val1 = params.get(0);
                    Object val2 = params.get(1);
                    return Objects.equals(val1, val2) ? null : val1;

                // JSON 字符串解析函数：从 JSON 文本中提取指定路径的字符串
                case "get_json_string":
                    if (params.size() != 2) {
                        throw new IllegalArgumentException("get_json_string requires exactly 2 parameters: (jsonText, jsonPath)");
                    }
                    String jsonText = params.get(0) == null ? null : params.get(0).toString();
                    String jsonPath = params.get(1) == null ? null : params.get(1).toString();
                    if (jsonText == null || jsonText.isEmpty() || jsonPath == null || jsonPath.isEmpty()) {
                        return null;
                    }
                    return extractJsonPathAsString(jsonText, jsonPath);

                default:
                    throw new IllegalArgumentException("Unsupported function: " + func);
            }
        }

        private LocalDateTime parseDateTimeString(String dateTimeStr) {
            if (dateTimeStr == null || dateTimeStr.trim().isEmpty()) {
                return null;
            }

            // 支持多种常见日期时间格式
            DateTimeFormatter[] formatters = {
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                    DateTimeFormatter.ofPattern("yyyy-M-d H:m:s"),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                    DateTimeFormatter.ofPattern("yyyy/M/d H:m:s"),
                    DateTimeFormatter.ofPattern("yyyy/M/d"),
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME
            };

            for (DateTimeFormatter formatter : formatters) {
                try {
                    // 尝试解析为 LocalDateTime
                    return LocalDateTime.parse(dateTimeStr, formatter);
                } catch (Exception e) {
                    // 继续尝试下一个格式
                }
            }

            // 如果上面的格式都失败了，尝试解析为纯日期然后添加默认时间
            try {
                // 尝试解析为 LocalDate
                LocalDate date = LocalDate.parse(dateTimeStr);
                // 转换为 LocalDateTime，时间设为 00:00:00
                return date.atStartOfDay();
            } catch (Exception e) {
                // 所有尝试都失败
                return null;
            }
        }

        private String extractJsonPathAsString(String jsonText, String jsonPath) {
            try {
                // 仅支持简单的 $.a.b.c 路径
                if (!jsonPath.startsWith("$")) {
                    return null;
                }
                String trimmedPath = jsonPath.startsWith("$") ? jsonPath.substring(1) : jsonPath;
                if (trimmedPath.startsWith(".")) {
                    trimmedPath = trimmedPath.substring(1);
                }
                if (trimmedPath.isEmpty()) {
                    return jsonText;
                }
                ObjectMapper localMapper = new ObjectMapper();
                JsonNode root = localMapper.readTree(jsonText);
                String[] parts = trimmedPath.split("\\.");
                JsonNode current = root;
                for (String part : parts) {
                    if (current == null || current.isNull()) {
                        return null;
                    }
                    // 支持简单数组下标: field[0]
                    String fieldName = part;
                    Integer index = null;
                    int bracketPos = part.indexOf('[');
                    if (bracketPos > -1 && part.endsWith("]")) {
                        fieldName = part.substring(0, bracketPos);
                        String idxStr = part.substring(bracketPos + 1, part.length() - 1);
                        try {
                            index = Integer.parseInt(idxStr);
                        } catch (NumberFormatException ignore) {
                            return null;
                        }
                    }

                    if (!fieldName.isEmpty()) {
                        current = current.get(fieldName);
                    }

                    if (index != null) {
                        if (current == null || !current.isArray() || index < 0 || index >= current.size()) {
                            return null;
                        }
                        current = current.get(index);
                    }
                }

                if (current == null || current.isNull()) return null;
                if (current.isTextual()) return current.asText();
                if (current.isNumber()) return current.asText();
                if (current.isBoolean()) return String.valueOf(current.asBoolean());
                // 对于对象或数组，返回其压缩后的 JSON 字符串
                return current.toString();
            } catch (Exception e) {
                return null;
            }
        }

        private BigDecimal toBigDecimal(Object value) {
            if (value == null) return BigDecimal.ZERO;
            if (value instanceof BigDecimal) return (BigDecimal) value;
            if (value instanceof Number) {
                return new BigDecimal(value.toString());
            }
            if (value instanceof String) {
                String str = (String) value;
                if (str.trim().isEmpty()) return BigDecimal.ZERO;
                return new BigDecimal(str);
            }
            return new BigDecimal(value.toString());
        }
    }

    /**
     * 条件判断节点
     */
    public static class ConditionNode implements ExpressionNode {
        private final ExpressionNode condition;
        private final ExpressionNode trueValue;
        private final ExpressionNode falseValue;
        private final List<ConditionalCase> conditions;
        private final ExpressionNode defaultValue;

        // 简单条件构造器
        public ConditionNode(ExpressionNode condition, ExpressionNode trueValue, ExpressionNode falseValue) {
            this.condition = condition;
            this.trueValue = trueValue;
            this.falseValue = falseValue;
            this.conditions = null;
            this.defaultValue = null;
        }

        // 多条件构造器
        public ConditionNode(List<ConditionalCase> conditions, ExpressionNode defaultValue) {
            this.condition = null;
            this.trueValue = null;
            this.falseValue = null;
            this.conditions = conditions;
            this.defaultValue = defaultValue;
        }

        @Override
        public Object evaluate(JsonNode dataObject) throws Exception {
            if (condition != null) {
                // 简单条件
                Object condResult = condition.evaluate(dataObject);
                if (Boolean.TRUE.equals(condResult)) {
                    if (trueValue != null) {
                        return trueValue.evaluate(dataObject);
                    }
                } else {
                    if (falseValue != null) {
                        return falseValue.evaluate(dataObject);
                    }
                }
            } else {
                // 多条件
                if (conditions != null) {
                    for (ConditionalCase cond : conditions) {
                        Object condResult = cond.condition.evaluate(dataObject);
                        if (Boolean.TRUE.equals(condResult)) {
                            return cond.value.evaluate(dataObject);
                        }
                    }
                }
                return defaultValue != null ? defaultValue.evaluate(dataObject) : null;
            }
            return null;
        }

        public static class ConditionalCase {
            public final ExpressionNode condition;
            public final ExpressionNode value;

            public ConditionalCase(ExpressionNode condition, ExpressionNode value) {
                this.condition = condition;
                this.value = value;
            }
        }
    }

    /**
     * 从JSON解析表达式
     */
    public ExpressionNode parseExpression(String jsonExpression) throws Exception {
        JsonNode node = objectMapper.readTree(jsonExpression);
        return parseNode(node);
    }

    private ExpressionNode parseNode(JsonNode node) throws Exception {
        String type = node.get("type").asText();

        switch (type) {
            case "field":
                return new FieldNode(node.get("field").asText());

            case "constant":
                JsonNode valueNode = node.get("value");
                String dataType = node.has("dataType") ? node.get("dataType").asText() : "string";
                Object value = parseConstantValue(valueNode, dataType);
                return new ConstantNode(value);

            case "operation":
                String operator = node.get("operator").asText();
                List<ExpressionNode> operands = new ArrayList<>();
                for (JsonNode operand : node.get("operands")) {
                    operands.add(parseNode(operand));
                }
                return new OperationNode(operator, operands);

            case "function":
                String function = node.get("function").asText();
                List<ExpressionNode> parameters = new ArrayList<>();
                if (node.has("parameters")) {
                    for (JsonNode param : node.get("parameters")) {
                        parameters.add(parseNode(param));
                    }
                }
                return new FunctionNode(function, parameters);

            case "condition":
                if (node.has("condition")) {
                    // 简单条件
                    ExpressionNode condition = parseNode(node.get("condition"));
                    ExpressionNode trueValue = parseNode(node.get("trueValue"));
                    ExpressionNode falseValue = parseNode(node.get("falseValue"));
                    return new ConditionNode(condition, trueValue, falseValue);
                } else {
                    // 多条件
                    List<ConditionNode.ConditionalCase> conditions = new ArrayList<>();
                    for (JsonNode condNode : node.get("conditions")) {
                        ExpressionNode condition = parseNode(condNode.get("condition"));
                        ExpressionNode value1 = parseNode(condNode.get("value"));
                        conditions.add(new ConditionNode.ConditionalCase(condition, value1));
                    }
                    ExpressionNode defaultValue = node.has("defaultValue") ? parseNode(node.get("defaultValue")) : null;
                    return new ConditionNode(conditions, defaultValue);
                }

            default:
                throw new IllegalArgumentException("Unsupported expression type: " + type);
        }
    }

    private Object parseConstantValue(JsonNode valueNode, String dataType) {
        switch (dataType) {
            case "number":
                return valueNode.isInt() ? valueNode.asInt() : valueNode.asDouble();
            case "boolean":
                return valueNode.asBoolean();
            case "date":
                return LocalDateTime.parse(valueNode.asText());
            case "string":
            default:
                return valueNode.asText();
        }
    }

    /**
     * 执行表达式 - 仅接受JsonNode
     */
    public Object evaluate(String jsonExpression, JsonNode dataObject) throws Exception {
        ExpressionNode expression = parseExpression(jsonExpression);
        return expression.evaluate(dataObject);
    }
}

class VirtualAttributeJsonExample {

    public static void main(String[] args) throws Exception {
        VirtualAttributeExpressionEvaluator evaluator = new VirtualAttributeExpressionEvaluator();
        ObjectMapper objectMapper = new ObjectMapper();

        // åˆ›å»ºæµ‹è¯•JSONæ•°æ®
        String jsonData = "{\n" +
                "  \"_transaction_price\": 1200.0,\n" +
                "  \"_cost_price\": 800.0,\n" +
                "  \"_quantity\": 15,\n" +
                "  \"_timestamp\": 1704067200,\n" +
                "  \"_created_time\": 1672531200,\n" +
                "  \"user\": {\n" +
                "    \"first_name\": \"John\",\n" +
                "    \"last_name\": \"Doe\",\n" +
                "    \"created_at\": \"2023-01-01T00:00:00\"\n" +
                "  }\n" +
                "}";

        JsonNode jsonNode = objectMapper.readTree(jsonData);

        // 测试from_unixtime函数
        System.out.println("\n=== FROM_UNIXTIME 函数测试 ===");

        // 基本用法：转换时间戳为LocalDateTime
        String fromUnixtimeExpression = "{ \"type\": \"function\", \"function\": \"from_unixtime\", \"parameters\": [ { \"type\": \"constant\", \"value\": 1704067200, \"dataType\": \"number\" } ] }";
        Object convertedDate = evaluator.evaluate(fromUnixtimeExpression, jsonNode);
        System.out.println("转换时间戳 1704067200: " + convertedDate);

        // 使用字段作为参数
        String fromUnixtimeFieldExpression = "{ \"type\": \"function\", \"function\": \"from_unixtime\", \"parameters\": [ { \"type\": \"field\", \"field\": \"timestamp\" } ] }";
        Object convertedFieldDate = evaluator.evaluate(fromUnixtimeFieldExpression, jsonNode);
        System.out.println("转换字段时间戳: " + convertedFieldDate);

        // 带格式化参数
        String fromUnixtimeFormatExpression = "{ \"type\": \"function\", \"function\": \"from_unixtime\", \"parameters\": [ { \"type\": \"constant\", \"value\": 1704067200, \"dataType\": \"number\" }, { \"type\": \"constant\", \"value\": \"yyyy-MM-dd HH:mm:ss\", \"dataType\": \"string\" } ] }";
        Object formattedDate = evaluator.evaluate(fromUnixtimeFormatExpression, jsonNode);
        System.out.println("格式化后的日期: " + formattedDate);

        // 与其他函数组合使用
        String combinedExpression = "{ \"type\": \"function\", \"function\": \"date_diff\", \"parameters\": [ { \"type\": \"function\", \"function\": \"now\" }, { \"type\": \"function\", \"function\": \"from_unixtime\", \"parameters\": [ { \"type\": \"field\", \"field\": \"created_time\" } ] } ] }";
        Object daysDiff = evaluator.evaluate(combinedExpression, jsonNode);
        System.out.println("距离创建时间的天数: " + daysDiff);

    }
}