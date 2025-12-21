package com.zhugeio.etl.pipeline.operator.id.virtualAttribute;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL表达式转JSON规则转换器
 */
public class SqlToJsonExpressionConverter {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // SQL操作符映射 - 支持大小写
    private static final Map<String, String> OPERATOR_MAP = new HashMap<>();
    static {
        // 算术操作符
        OPERATOR_MAP.put("+", "add");
        OPERATOR_MAP.put("-", "subtract");
        OPERATOR_MAP.put("*", "multiply");
        OPERATOR_MAP.put("/", "divide");
        OPERATOR_MAP.put("%", "modulo");
        
        // 比较操作符
        OPERATOR_MAP.put("=", "equals");
        OPERATOR_MAP.put("!=", "not_equals");
        OPERATOR_MAP.put("<>", "not_equals");
        OPERATOR_MAP.put(">", "greater_than");
        OPERATOR_MAP.put("<", "less_than");
        OPERATOR_MAP.put(">=", "greater_equal");
        OPERATOR_MAP.put("<=", "less_equal");
        
        // 逻辑操作符 - 支持大小写
        OPERATOR_MAP.put("AND", "and");
        OPERATOR_MAP.put("and", "and");
        OPERATOR_MAP.put("OR", "or");
        OPERATOR_MAP.put("or", "or");
        OPERATOR_MAP.put("NOT", "not");
        OPERATOR_MAP.put("not", "not");
    }
    
    // SQL函数映射 (不支持聚合函数) - 支持大小写
    private static final Map<String, String> FUNCTION_MAP = new HashMap<>();
    static {
        // 字符串函数
        FUNCTION_MAP.put("UPPER", "upper");
        FUNCTION_MAP.put("upper", "upper");
        FUNCTION_MAP.put("LOWER", "lower");
        FUNCTION_MAP.put("lower", "lower");
        FUNCTION_MAP.put("LENGTH", "length");
        FUNCTION_MAP.put("length", "length");
        FUNCTION_MAP.put("LEN", "length");
        FUNCTION_MAP.put("len", "length");
        FUNCTION_MAP.put("CONCAT", "concat");
        FUNCTION_MAP.put("concat", "concat");
        FUNCTION_MAP.put("SUBSTRING", "substring");
        FUNCTION_MAP.put("substring", "substring");
        FUNCTION_MAP.put("SUBSTR", "substring");
        FUNCTION_MAP.put("substr", "substring");
        
        // 日期函数
        FUNCTION_MAP.put("NOW", "now");
        FUNCTION_MAP.put("now", "now");
        FUNCTION_MAP.put("CURRENT_TIMESTAMP", "now");
        FUNCTION_MAP.put("current_timestamp", "now");
        FUNCTION_MAP.put("DATEDIFF", "date_diff");
        FUNCTION_MAP.put("datediff", "date_diff");
        FUNCTION_MAP.put("DATE_FORMAT", "date_format");
        FUNCTION_MAP.put("date_format", "date_format");
        FUNCTION_MAP.put("FROM_UNIXTIME", "from_unixtime");
        FUNCTION_MAP.put("from_unixtime", "from_unixtime");
        
        // 数学函数
        FUNCTION_MAP.put("ABS", "abs");
        FUNCTION_MAP.put("abs", "abs");
        FUNCTION_MAP.put("ROUND", "round");
        FUNCTION_MAP.put("round", "round");
        FUNCTION_MAP.put("CEIL", "ceil");
        FUNCTION_MAP.put("ceil", "ceil");
        FUNCTION_MAP.put("FLOOR", "floor");
        FUNCTION_MAP.put("floor", "floor");
        
        // 空值处理函数
        FUNCTION_MAP.put("COALESCE", "coalesce");
        FUNCTION_MAP.put("coalesce", "coalesce");
        FUNCTION_MAP.put("NULLIF", "nullif");
        FUNCTION_MAP.put("nullif", "nullif");
        
        // JSON 相关函数
        FUNCTION_MAP.put("GET_JSON_STRING", "get_json_string");
        FUNCTION_MAP.put("get_json_string", "get_json_string");
    }
    
    // 不支持的聚合函数列表 - 支持大小写
    private static final Set<String> UNSUPPORTED_AGGREGATE_FUNCTIONS = new HashSet<>();
    static {
        // 大写版本
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("SUM");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("AVG");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("COUNT");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("MAX");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("MIN");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("GROUP_CONCAT");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("STDDEV");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("VARIANCE");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("MEDIAN");
        
        // 小写版本
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("sum");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("avg");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("count");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("max");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("min");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("group_concat");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("stddev");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("variance");
        UNSUPPORTED_AGGREGATE_FUNCTIONS.add("median");
    }
    
    /**
     * 词法分析器 - Token类型
     */
    public enum TokenType {
        FIELD, NUMBER, STRING, OPERATOR, FUNCTION, LPAREN, RPAREN, COMMA, 
        CASE, WHEN, THEN, ELSE, END, AND, OR, NOT, EOF
    }
    
    /**
     * Token类
     */
    public static class Token {
        public final TokenType type;
        public final String value;
        public final int position;
        
        public Token(TokenType type, String value, int position) {
            this.type = type;
            this.value = value;
            this.position = position;
        }
        
        @Override
        public String toString() {
            return String.format("Token{%s, '%s', %d}", type, value, position);
        }
    }
    
    /**
     * 词法分析器
     */
    public static class Lexer {
        private final String input;
        private int position = 0;
        private final List<Token> tokens = new ArrayList<>();
    
        // 修改后的正则表达式模式 - 支持中文字符
        private static final Pattern PATTERNS = Pattern.compile(
            "(?i)" + // 忽略大小写
                "(?<CASE>\\bCASE\\b)|" +
                "(?<WHEN>\\bWHEN\\b)|" +
                "(?<THEN>\\bTHEN\\b)|" +
                "(?<ELSE>\\bELSE\\b)|" +
                "(?<END>\\bEND\\b)|" +
                "(?<AND>\\bAND\\b)|" +
                "(?<OR>\\bOR\\b)|" +
                "(?<NOT>\\bNOT\\b)|" +
                "(?<FUNCTION>\\b[A-Z_][A-Z0-9_]*(?=\\s*\\())|" +
//                "(?<FIELD>[\\p{L}_][\\p{L}0-9_.]*)|" +  // 修改这里支持中文
                "(?<FIELD>[\\p{L}_$][\\p{L}0-9_$.]*)|" +  // 修改这里：添加$符号支持
                "(?<NUMBER>\\d+(?:\\.\\d+)?)|" +
                "(?<STRING>\"(?:[^\"]|\"\")*\"|'(?:[^']|'')*')|" +
                "(?<OPERATOR><=|>=|<>|!=|[+\\-*/%=<>])|" +
                "(?<LPAREN>\\()|" +
                "(?<RPAREN>\\))|" +
                "(?<COMMA>,)|" +
                "(?<WHITESPACE>\\s+)"
        );
        
        public Lexer(String input) {
            this.input = input.trim();
        }
        
        public List<Token> tokenize() {
            Matcher matcher = PATTERNS.matcher(input);
            
            while (matcher.find()) {
                if (matcher.group("WHITESPACE") != null) {
                    continue; // 跳过空白字符
                }
                
                TokenType type = null;
                String value = null;
                
                if (matcher.group("CASE") != null) {
                    type = TokenType.CASE;
                    value = matcher.group("CASE");
                } else if (matcher.group("WHEN") != null) {
                    type = TokenType.WHEN;
                    value = matcher.group("WHEN");
                } else if (matcher.group("THEN") != null) {
                    type = TokenType.THEN;
                    value = matcher.group("THEN");
                } else if (matcher.group("ELSE") != null) {
                    type = TokenType.ELSE;
                    value = matcher.group("ELSE");
                } else if (matcher.group("END") != null) {
                    type = TokenType.END;
                    value = matcher.group("END");
                } else if (matcher.group("AND") != null) {
                    type = TokenType.AND;
                    value = matcher.group("AND");
                } else if (matcher.group("OR") != null) {
                    type = TokenType.OR;
                    value = matcher.group("OR");
                } else if (matcher.group("NOT") != null) {
                    type = TokenType.NOT;
                    value = matcher.group("NOT");
                } else if (matcher.group("FUNCTION") != null) {
                    type = TokenType.FUNCTION;
                    value = matcher.group("FUNCTION");
                } else if (matcher.group("FIELD") != null) {
                    type = TokenType.FIELD;
                    value = matcher.group("FIELD");
                } else if (matcher.group("NUMBER") != null) {
                    type = TokenType.NUMBER;
                    value = matcher.group("NUMBER");
                } else if (matcher.group("STRING") != null) {
                    type = TokenType.STRING;
                    value = matcher.group("STRING");
                } else if (matcher.group("OPERATOR") != null) {
                    type = TokenType.OPERATOR;
                    value = matcher.group("OPERATOR");
                } else if (matcher.group("LPAREN") != null) {
                    type = TokenType.LPAREN;
                    value = "(";
                } else if (matcher.group("RPAREN") != null) {
                    type = TokenType.RPAREN;
                    value = ")";
                } else if (matcher.group("COMMA") != null) {
                    type = TokenType.COMMA;
                    value = ",";
                }
                
                if (type != null) {
                    tokens.add(new Token(type, value, matcher.start()));
                }
            }
            
            tokens.add(new Token(TokenType.EOF, "", input.length()));
            return tokens;
        }
    }
    
    /**
     * 语法分析器
     */
    public class Parser {
        private final List<Token> tokens;
        private int position = 0;
        
        public Parser(List<Token> tokens) {
            this.tokens = tokens;
        }
        
        public ObjectNode parse() throws Exception {
            ObjectNode result = parseExpression();
            if (currentToken().type != TokenType.EOF) {
                throw new RuntimeException("Unexpected token: " + currentToken());
            }
            return result;
        }
        
        private Token currentToken() {
            return tokens.get(position);
        }
        
        private Token nextToken() {
            if (position < tokens.size() - 1) {
                position++;
            }
            return currentToken();
        }
        
        private boolean match(TokenType type) {
            if (currentToken().type == type) {
                nextToken();
                return true;
            }
            return false;
        }
        
        private void expect(TokenType type) throws Exception {
            if (!match(type)) {
                throw new RuntimeException("Expected " + type + " but got " + currentToken().type);
            }
        }
        
        // 解析表达式 (最低优先级)
        private ObjectNode parseExpression() throws Exception {
            return parseOrExpression();
        }
        
        // 解析 OR 表达式
        private ObjectNode parseOrExpression() throws Exception {
            ObjectNode left = parseAndExpression();
            
            while (currentToken().type == TokenType.OR) {
                nextToken();
                ObjectNode right = parseAndExpression();
                left = createOperationNode("or", Arrays.asList(left, right));
            }
            
            return left;
        }
        
        // 解析 AND 表达式
        private ObjectNode parseAndExpression() throws Exception {
            ObjectNode left = parseNotExpression(); // 改为调用新的 parseNotExpression

            while (currentToken().type == TokenType.AND) {
                nextToken();
                ObjectNode right = parseNotExpression(); // 改为调用新的 parseNotExpression
                left = createOperationNode("and", Arrays.asList(left, right));
            }

            return left;
        }

        private ObjectNode parseNotExpression() throws Exception {
            if (currentToken().type == TokenType.NOT) {
                nextToken();
                ObjectNode operand = parseEqualityExpression(); // 直接跳到比较表达式级别
                return createOperationNode("not", Arrays.asList(operand));
            }

            return parseEqualityExpression();
        }
        
        // 解析相等比较表达式
        private ObjectNode parseEqualityExpression() throws Exception {
            ObjectNode left = parseRelationalExpression();
            
            while (currentToken().type == TokenType.OPERATOR && 
                   ("=".equals(currentToken().value) || "!=".equals(currentToken().value) || 
                    "<>".equals(currentToken().value))) {
                String op = currentToken().value;
                nextToken();
                ObjectNode right = parseRelationalExpression();
                left = createOperationNode(OPERATOR_MAP.get(op), Arrays.asList(left, right));
            }
            
            return left;
        }
        
        // 解析关系比较表达式
        private ObjectNode parseRelationalExpression() throws Exception {
            ObjectNode left = parseAdditiveExpression();
            
            while (currentToken().type == TokenType.OPERATOR && 
                   (">".equals(currentToken().value) || "<".equals(currentToken().value) ||
                    ">=".equals(currentToken().value) || "<=".equals(currentToken().value))) {
                String op = currentToken().value;
                nextToken();
                ObjectNode right = parseAdditiveExpression();
                left = createOperationNode(OPERATOR_MAP.get(op), Arrays.asList(left, right));
            }
            
            return left;
        }
        
        // 解析加减表达式
        private ObjectNode parseAdditiveExpression() throws Exception {
            ObjectNode left = parseMultiplicativeExpression();
            
            while (currentToken().type == TokenType.OPERATOR && 
                   ("+".equals(currentToken().value) || "-".equals(currentToken().value))) {
                String op = currentToken().value;
                nextToken();
                ObjectNode right = parseMultiplicativeExpression();
                left = createOperationNode(OPERATOR_MAP.get(op), Arrays.asList(left, right));
            }
            
            return left;
        }
        
        // 解析乘除表达式
        private ObjectNode parseMultiplicativeExpression() throws Exception {
            ObjectNode left = parseUnaryExpression();
            
            while (currentToken().type == TokenType.OPERATOR && 
                   ("*".equals(currentToken().value) || "/".equals(currentToken().value) ||
                    "%".equals(currentToken().value))) {
                String op = currentToken().value;
                nextToken();
                ObjectNode right = parseUnaryExpression();
                left = createOperationNode(OPERATOR_MAP.get(op), Arrays.asList(left, right));
            }
            
            return left;
        }
        
        // 解析一元表达式
        private ObjectNode parseUnaryExpression() throws Exception {
            if (currentToken().type == TokenType.OPERATOR && "-".equals(currentToken().value)) {
                nextToken();
                ObjectNode operand = parseUnaryExpression();
                ObjectNode zero = createConstantNode("0", "number");
                return createOperationNode("subtract", Arrays.asList(zero, operand));
            }

            return parsePrimaryExpression();
        }
        
        // 解析基础表达式
        private ObjectNode parsePrimaryExpression() throws Exception {
            Token token = currentToken();
            
            switch (token.type) {
                case NUMBER:
                    nextToken();
                    return createConstantNode(token.value, "number");
                    
                case STRING:
                    nextToken();
                    // 移除字符串的单引号
                    String stringValue = token.value.substring(1, token.value.length() - 1);
                    stringValue = stringValue.replace("''", "'"); // 处理转义的单引号
                    return createConstantNode(stringValue, "string");
                    
                case FIELD:
                    nextToken();
                    return createFieldNode(token.value);
                    
                case FUNCTION:
                    return parseFunctionCall();
                    
                case CASE:
                    return parseCaseExpression();
                    
                case LPAREN:
                    nextToken();
                    ObjectNode expr = parseExpression();
                    expect(TokenType.RPAREN);
                    return expr;
                    
                default:
                    throw new RuntimeException("Unexpected token: " + token);
            }
        }
        
        // 解析函数调用
        private ObjectNode parseFunctionCall() throws Exception {
            String functionName = currentToken().value.toUpperCase();
            
            // 检查是否为不支持的聚合函数
            if (UNSUPPORTED_AGGREGATE_FUNCTIONS.contains(functionName)) {
                throw new RuntimeException("聚合函数 " + functionName + " 不支持在虚拟属性表达式中使用");
            }
            
            nextToken();
            expect(TokenType.LPAREN);
            
            List<ObjectNode> parameters = new ArrayList<>();
            
            if (currentToken().type != TokenType.RPAREN) {
                parameters.add(parseExpression());
                
                while (match(TokenType.COMMA)) {
                    parameters.add(parseExpression());
                }
            }
            
            expect(TokenType.RPAREN);
            
            String mappedFunction = FUNCTION_MAP.get(functionName);
            if (mappedFunction == null) {
                throw new RuntimeException("不支持的函数: " + functionName);
            }
            
            return createFunctionNode(mappedFunction, parameters);
        }
        
        // 解析CASE表达式
        private ObjectNode parseCaseExpression() throws Exception {
            expect(TokenType.CASE);
            
            List<ObjectNode> conditions = new ArrayList<>();
            ObjectNode defaultValue = null;
            
            // 解析WHEN子句
            while (currentToken().type == TokenType.WHEN) {
                nextToken();
                ObjectNode condition = parseExpression();
                expect(TokenType.THEN);
                ObjectNode value = parseExpression();
                
                ObjectNode conditionCase = objectMapper.createObjectNode();
                conditionCase.set("condition", condition);
                conditionCase.set("value", value);
                conditions.add(conditionCase);
            }
            
            // 解析ELSE子句
            if (match(TokenType.ELSE)) {
                defaultValue = parseExpression();
            }
            
            expect(TokenType.END);
            
            return createConditionNode(conditions, defaultValue);
        }
        
        // 创建操作节点
        private ObjectNode createOperationNode(String operator, List<ObjectNode> operands) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("type", "operation");
            node.put("operator", operator);
            ArrayNode operandsArray = objectMapper.createArrayNode();
            for (ObjectNode operand : operands) {
                operandsArray.add(operand);
            }
            node.set("operands", operandsArray);
            return node;
        }
        
        // 创建字段节点
        private ObjectNode createFieldNode(String field) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("type", "field");
            // 去掉表名和嵌套路径，只保留最底层的字段名
            String fieldName = field;
            if (field.contains(".")) {
                fieldName = field.substring(field.lastIndexOf(".") + 1);
            }
            node.put("field", fieldName);
            return node;
        }
        
        // 创建常量节点
        private ObjectNode createConstantNode(String value, String dataType) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("type", "constant");
            node.put("dataType", dataType);
            
            switch (dataType) {
                case "number":
                    if (value.contains(".")) {
                        node.put("value", Double.parseDouble(value));
                    } else {
                        node.put("value", Integer.parseInt(value));
                    }
                    break;
                case "boolean":
                    node.put("value", Boolean.parseBoolean(value));
                    break;
                default:
                    node.put("value", value);
            }
            
            return node;
        }
        
        // 创建函数节点
        private ObjectNode createFunctionNode(String function, List<ObjectNode> parameters) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("type", "function");
            node.put("function", function);
            ArrayNode parametersArray = objectMapper.createArrayNode();
            for (ObjectNode param : parameters) {
                parametersArray.add(param);
            }
            node.set("parameters", parametersArray);
            return node;
        }
        
        // 创建条件节点
        private ObjectNode createConditionNode(List<ObjectNode> conditions, ObjectNode defaultValue) {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("type", "condition");
            ArrayNode conditionsArray = objectMapper.createArrayNode();
            for (ObjectNode condition : conditions) {
                conditionsArray.add(condition);
            }
            node.set("conditions", conditionsArray);
            if (defaultValue != null) {
                node.set("defaultValue", defaultValue);
            }
            return node;
        }
    }
    
    /**
     * 转换SQL表达式到JSON
     */
    public String convertSqlToJson(String sqlExpression) throws Exception {
        // 词法分析
        Lexer lexer = new Lexer(sqlExpression);
        List<Token> tokens = lexer.tokenize();
        
        // 语法分析
        Parser parser = new Parser(tokens);
        ObjectNode jsonExpression = parser.parse();
        
        // 返回JSON字符串
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonExpression);
    }
    
    /**
     * 使用示例和测试
     */
    public static void main(String[] args) {
        SqlToJsonExpressionConverter converter = new SqlToJsonExpressionConverter();
        
        // 测试用例
        String[] testCases = {
            // 基本算术运算
                "from_unixtime(1704067200)"
//            "event.transaction_price - event.cost_price",
//            "event.商品价格 - event._商品类型",
//            // 复杂算术运算
//            "(event.transaction_price - event.cost_price) / event.cost_price * 100",
//
//            // 数学函数
//            "ABS(event.transaction_price - event.cost_price)",
//            "ROUND((event.transaction_price - event.cost_price) / event.cost_price * 100, 2)",
//
//            // 条件表达式
//            "CASE WHEN event.transaction_price > 1000 THEN 'high' ELSE 'low' END",
//
//            // 多条件表达式
//            "CASE WHEN event.price > 1000 AND event.quantity > 10 THEN 'bulk_discount' WHEN event.price > 1000 THEN 'price_discount' ELSE 'no_discount' END",
//
//            // 字符串函数
//            "UPPER(user.name)",
//            "CONCAT(user.first_name, ' ', user.last_name)",
//            "LENGTH(user.name)",
//
//            // 日期函数
//            "DATEDIFF(NOW(), user.created_at)",
//
//            // 复合表达式
//            "event.price * event.quantity + event.shipping_cost",
//
//            // 比较运算
//            "event.status = 'completed' AND event.amount > 100",
//
//            // 空值处理
//            "COALESCE(event.discount, 0)",
//            "NULLIF(event.status, '')"
        };
        
        for (String testCase : testCases) {
            try {
                System.out.println("SQL: " + testCase);
                String json = converter.convertSqlToJson(testCase);
                System.out.println("JSON: " + json);
                System.out.println("---");
            } catch (Exception e) {
                System.err.println("Error converting: " + testCase);
                System.err.println("Error: " + e.getMessage());
                System.out.println("---");
            }
        }
    }
}