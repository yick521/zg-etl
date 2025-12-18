package com.zhugeio.etl.pipeline.util;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OperatorUtil {
    public static final String DATE_STR = "yyyy-MM-dd";
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_STR);

    /**
     * 判断属性是否满足条件
     */
    public static boolean compareProValue(Integer appId, Integer zgId, Map<String, Object> props, JSONObject matchJson, Integer zgDid) throws ParseException {

        String propCategory = matchJson.getString("propCategory");// 属性类型

        if ("userProp".equals(propCategory)) {
            //用户属性条件限制
            Integer attrId = matchJson.getInteger("attrId");

            if (attrId == 0) {
                // 无用户属性id 从 f_user_detail_177 或 f_user_detail_sum_177 获取属性值
                String columName = matchJson.getString("dimensionSub");
                switch (columName) {
                    case "visit_times":
                    case "duration":
                        String sql1 = String.format("select %s from f_user_detail_sum_%s where zg_id= %s limit 1", columName, appId, zgId);
                        String ssdbKey1 = String.format("ad:user:col:%s:%s:%s", appId, zgId, columName);
                        String value1 = getUserFeildValue(ssdbKey1, sql1, columName);
                        if (StringUtils.isNotEmpty(value1)) {
                            System.out.println("用户属性查询sql： " + sql1 + "\n 查询结果：" + value1);
                            return compareValue(value1, matchJson);
                        }
                        break;
                    case "is_anonymous":
                        //实名匿名判断
                        String sql0 = String.format("select user_id from  b_user_%s where device_id=%s and zg_id=%s limit 1", appId, zgDid, zgId);
                        String ssdbKey0 = String.format("ad:user:col:%s:%s:%s", appId, zgId, "uid");
                        String value0 = getUserFeildValue(ssdbKey0, sql0, "user_id");
                        if (StringUtils.isEmpty(value0) || "NULL".equalsIgnoreCase(value0)) {
                            value0 = "匿名";
                        } else {
                            value0 = "实名";
                        }
                        if (StringUtils.isNotEmpty(value0)) {
                            System.out.println("用户属性查询sql： " + sql0 + "\n 查询结果：" + value0);
                            return compareValue(value0, matchJson);
                        }
                        break;
                    default:
                        String sql2 = String.format("select %s from f_user_detail_%s where zg_id= %s limit 1", columName, appId, zgId);
                        String ssdbKey2 = String.format("ad:user:col:%s:%s:%s", appId, zgId, columName);
                        String value2 = getUserFeildValue(ssdbKey2, sql2, columName);
                        if (StringUtils.isNotEmpty(value2)) {
                            System.out.println("用户属性查询sql： " + sql2 + "\n 查询结果：" + value2);
                            return compareValue(value2, matchJson);
                        }
                        break;
                }
            } else {
                //有用户属性id 从 用户属性表获取 b_user_property_177 属性值
                String sql3 = String.format("select property_value from b_user_property_%s where property_id = %s and zg_id = %s limit 1", appId, attrId, zgId);
                String ssdbKey3 = String.format("ad:user:id:%s:%s:%s", appId, zgId, attrId);
                String value3 = getUserFeildValue(ssdbKey3, sql3, "property_value");
                if (StringUtils.isNotEmpty(value3)) {
                    System.out.println("用户属性查询sql： " + sql3 + "\n 查询结果：" + value3);
                    return compareValue(value3, matchJson);
                }
            }
        }
        if ("eventProp".equals(propCategory)) {
            //事件属性条件限制
            //{"attrId":5560,"propCategory":"eventProp","values":["100"],"dimensionSub":"event_attr","label":"商品价格","type":2,"operator":"gt","attrName":"商品价格"}
            String label = matchJson.getString("label");  // 如 "_广告分析链接ID":183,
            String eventAttValue = "";
            if (props.containsKey("_" + label)) {
                // _ 开头的是自定义属性
                eventAttValue = String.valueOf(props.get("_" + label));
            }
            if (props.containsKey("$" + label)) {
                // $ 开头的是内置属性
                eventAttValue = String.valueOf(props.get("$" + label));
            }
            System.out.println(label + " 事件属性值： " + eventAttValue);
            return compareValue(eventAttValue, matchJson);
        }

        return false;
    }

    public static Boolean compareValue(String eventAttValue, JSONObject matchJson, String label) {
        JSONArray values = matchJson.getJSONArray("values");
        Integer type = matchJson.getInteger("type");
        String operator = matchJson.getString("operator");

        if (StringUtils.isNotEmpty(eventAttValue) && !values.isEmpty()) {
            String targetValue = values.getString(0);
            if (type == 1) {
                return compareStringValue(eventAttValue, targetValue, operator);
            }
            //type=2 为数值型
            if (type == 2) {
                return compareNumericValue(eventAttValue, targetValue, operator);
            }
            //type=3 为时间类型 (目前仅用户属性有)
            if (type == 3) {
                return compareDateValue(eventAttValue, values, operator);
            }
        } else {
            if ("业务".equals(label)) {
                switch (operator) {
                    case "equal":
                        return true;
                    case "not equal":
                        return false;
                }
            }
            //空值处理
            return handleNullValue(eventAttValue, operator);
        }
        return false;
    }

    public static Boolean compareValue(String eventAttValue, JSONObject matchJson) {
        JSONArray values = matchJson.getJSONArray("values");
        Integer type = matchJson.getInteger("type");
        String operator = matchJson.getString("operator");

        if (StringUtils.isNotEmpty(eventAttValue) && !values.isEmpty()) {
            String targetValue = values.getString(0);
            if (type == 1) {
                return compareStringValue(eventAttValue, targetValue, operator, true);
            }
            //type=2 为数值型
            if (type == 2) {
                return compareNumericValue(eventAttValue, targetValue, operator);
            }
            //type=3 为时间类型 (目前仅用户属性有)
            if (type == 3) {
                return compareDateValue(eventAttValue, values, operator);
            }
        } else {
            if ("".equals(eventAttValue)) {
                // 保留原有逻辑，但为空的处理
            }
            //空值处理
            return handleNullValue(eventAttValue, operator);
        }
        return false;
    }

    /**
     * 比较字符串值
     */
    private static Boolean compareStringValue(String eventAttValue, String targetValue, String operator) {
        return compareStringValue(eventAttValue, targetValue, operator, false);
    }

    /**
     * 比较字符串值 - 支持两种正则处理方式
     */
    private static Boolean compareStringValue(String eventAttValue, String targetValue, String operator, boolean usePatternMatch) {
        switch (operator) {
            case "equal":
                //是
                return eventAttValue.equals(targetValue);
            case "regexp":
                //正则匹配
                if (usePatternMatch) {
                    return isMatch(Pattern.compile(targetValue), eventAttValue);
                } else {
                    return eventAttValue.matches(targetValue);
                }
            case "contains":
                //包含
                return eventAttValue.contains(targetValue);
            case "not regexp":
                //正则不匹配
                if (usePatternMatch) {
                    return !isMatch(Pattern.compile(targetValue), eventAttValue);
                } else {
                    return !eventAttValue.matches(targetValue);
                }
            case "not equal":
                //不是
                return !eventAttValue.equals(targetValue);
            case "not contains":
                //不包含
                return !eventAttValue.contains(targetValue);
            case "begin with":
                //开头是
                return eventAttValue.startsWith(targetValue);
            case "end with":
                //结尾是
                return eventAttValue.endsWith(targetValue);
            case "not begin with":
                //开头不是
                return !eventAttValue.startsWith(targetValue);
            case "not end with":
                //结尾不是
                return !eventAttValue.endsWith(targetValue);
            case "is not null":
                //不是空值
                return true;
            default:
                return false;
        }
    }

    /**
     * 比较数值型值 - 增加了null值检查
     */
    private static Boolean compareNumericValue(String eventAttValue, String targetValue, String operator) {
        // 检查是否为null或"null"字符串
        if (isNullOrNullString(eventAttValue) || isNullOrNullString(targetValue)) {
            return false; // 如果任一值为null，则比较失败
        }

        try {
            Long eventValue = Long.parseLong(eventAttValue);
            Long target = Long.parseLong(targetValue);

            switch (operator) {
                case "gt":
                    //>
                    return eventValue > target;
                case "equal":
                    //=
                    return eventValue.equals(target);
                case "lt":
                    //<
                    return eventValue < target;
                case "ge":
                    //>=
                    return eventValue >= target;
                case "le":
                    //<=
                    return eventValue <= target;
                case "not equal":
                    //不等于
                    return !eventValue.equals(target);
                default:
                    return false;
            }
        } catch (NumberFormatException e) {
            // 如果解析失败，记录错误并返回false
            System.err.println("数值解析错误: eventAttValue=" + eventAttValue + ", targetValue=" + targetValue + ", operator=" + operator);
            return false;
        }
    }

    /**
     * 比较时间类型值
     */
    private static Boolean compareDateValue(String eventAttValue, JSONArray values, String operator) {
        if (isNullOrNullString(eventAttValue)) {
            return false;
        }

        try {
            long thisTimestamp = Long.parseLong(eventAttValue) * 1000;

            switch (operator) {
                case "relative":
                    //最近 * 天 ["55","1"] 1662256541 秒
                    int day = Integer.parseInt(values.getString(0));
                    return thisTimestamp >= System.currentTimeMillis() - day * 24 * 60 * 60 * 1000L;
                case "absolute":
                    //固定时段 ["2022-09-12","2022-09-19"] 1662256541 秒
                    long startTimestamp = LocalDate.parse(values.getString(0), DATE_FORMATTER)
                            .atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
                    long endTimestamp = LocalDate.parse(values.getString(1), DATE_FORMATTER)
                            .atStartOfDay(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
                    return thisTimestamp >= startTimestamp && thisTimestamp <= endTimestamp;
                default:
                    return false;
            }
        } catch (NumberFormatException e) {
            System.err.println("时间戳解析错误: eventAttValue=" + eventAttValue);
            return false;
        }
    }

    /**
     * 处理空值情况
     */
    private static Boolean handleNullValue(String eventAttValue, String operator) {
        switch (operator) {
            case "is null":
                //是空值
                return StringUtils.isEmpty(eventAttValue) || "null".equalsIgnoreCase(eventAttValue);
            case "is not null":
                //不是空值
                return !StringUtils.isEmpty(eventAttValue) && !"null".equalsIgnoreCase(eventAttValue);
            default:
                return false;
        }
    }

    /**
     * 检查是否为null或"null"字符串
     */
    private static boolean isNullOrNullString(String value) {
        return value == null || "null".equalsIgnoreCase(value) || StringUtils.isEmpty(value);
    }

    /**
     * 从impala查询用户属性字段：先从ssdb获取，没有再查impala
     */
    public static String getUserFeildValue(String ssdbKey, String sql, String columName) {
//        String value = AdRedisClient.getStr(ssdbKey);
//        if (StringUtils.isEmpty(value)) {
//            value = ImpalaDao.querySQLForOne(sql, columName);
//            if (!StringUtils.isEmpty(value)) {
//                //查询到结果则缓存于ssdb，缓存6小时
//                AdRedisClient.setStr(ssdbKey, value, 6 * 60 * 60);
//            }
//        }
//        return value;
        return null;
    }

    public static boolean isMatch(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        return matcher.find();
    }
}