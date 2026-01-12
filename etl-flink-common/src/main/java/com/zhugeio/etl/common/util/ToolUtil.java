package com.zhugeio.etl.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ningjh
 * @name ToolsUtil
 * @date 2025/12/4
 * @description
 */
public class ToolUtil {
    private static final Logger logger = LoggerFactory.getLogger(ToolUtil.class);

    // 定义日期格式常量
    private static final String DATE_TS_STR = "yyyy-MM-dd'T'HH:mm'Z'";
    private static final String DATE_TS_STR_SE = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    // 定义日期格式化器
    private static final DateTimeFormatter DATE_TS_FORMATTER = DateTimeFormatter.ofPattern(DATE_TS_STR);
    private static final DateTimeFormatter DATE_TS_FORMATTER_SE = DateTimeFormatter.ofPattern(DATE_TS_STR_SE);

    /**
     * 日期解析 按US 的时区
     * 2023-01-04T08:11Z  2023-01-04 16:12:36
     * 2023-01-04T09:59Z  2023-01-04 17:59:57 差异8小时
     *
     * 注意：原始输入是UTC时间（带Z表示零时区），
     * 如果要转换为本地时间，需要根据时区调整
     */
    public static long dateUsStrToTimestamp(String dateStr) {
        long time = 0L;

        if (dateStr == null || dateStr.trim().isEmpty()) {
            return time;
        }

        try {
            if (dateStr.length() > 17) {
                // 包含秒的格式：2023-01-04T08:11:23Z
                LocalDateTime dateTime = LocalDateTime.parse(dateStr, DATE_TS_FORMATTER_SE);
                // 使用UTC时区（ZoneOffset.UTC 或 ZoneOffset.of("+0")）
                time = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            } else {
                // 只到分钟的格式：2023-01-04T08:11Z
                LocalDateTime dateTime = LocalDateTime.parse(dateStr, DATE_TS_FORMATTER);
                time = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            }
        } catch (DateTimeParseException e) {
            logger.info("日期解析异常: {}, 原始字符串: {}", e.getMessage(), dateStr, e);
        } catch (Exception e) {
            logger.error("日期转换发生未知异常: {}, 原始字符串: {}", e.getMessage(), dateStr, e);
        }

        return time;
    }

    /**
     * 对字符串进行md5加密，返回固定长度的字符（32位），如：a167d52277c8cec9e5876c10dd43dfe0
     *
     * @param str 需要加密的字符串
     * @return MD5加密后的32位小写字符串
     */
    public static String getMD5Str(String str) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            md5.update(bytes, 0, bytes.length);
            String result = new BigInteger(1, md5.digest()).toString(16).toLowerCase();

            // 确保结果是32位，前面补0
            while (result.length() < 32) {
                result = "0" + result;
            }
            return result;

        } catch (NoSuchAlgorithmException e) {
            // 通常不会发生，因为MD5是标准算法
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    /**
     * url参数解析为json
     * http://localhost:63342/%E6%99%AE%E9%80%9AJS%E6%95%B0%E6%8D%AE%E4%B8%8A%E4%BC%A0%E9%A1%B5%E9%9D%A2/data-upload.html?_ijt=lshbjf76s9tdat3661du68a6dq&_ij_reload=RELOAD_ON_SAVE
     */
    public static Map<String, Object> urlParseToMap(String url) {
        try {
            // 解码URL
            String decodeUrl = URLDecoder.decode(url, "UTF-8");
            Map<String, Object> map = new HashMap<>();

            if (decodeUrl.contains("?")) {
                String[] fields = decodeUrl.split("\\?");
                if (fields.length > 1) {
                    String keyValues = fields[1];

                    if (keyValues.contains("&")) {
                        String[] keyValuesArr = keyValues.split("&");
                        for (String keyValue : keyValuesArr) {
                            if (keyValue.contains("=")) {
                                String[] keyValueArr = keyValue.split("=");
                                String key = keyValueArr[0];
                                String value = keyValueArr.length > 1 ? keyValueArr[1] : "";
                                map.put(key, value);
                            }
                        }
                    } else if (keyValues.contains("=")) {
                        String[] keyValueArr = keyValues.split("=");
                        String key = keyValueArr[0];
                        String value = keyValueArr.length > 1 ? keyValueArr[1] : "";
                        map.put(key, value);
                    }
                }
            }
            return map;
        } catch (Exception e) {
            // 处理异常，返回空Map或记录日志
            e.printStackTrace();
            return new HashMap<>();
        }
    }

    /**
     * UA解析
     * 从 User-Agent 字符串中提取操作系统名称和版本
     *
     * @param ua User-Agent 字符串
     * @return 格式为 "操作系统名称:版本号" 的字符串
     */
    public static String uaAnalysis(String ua) {
        // 空值检查
        if (ua == null) {
            return ":";
        }

        // 清理模式：匹配非字母、数字、空格的字符
        String cleanPattern = "[^a-zA-Z0-9 ]";

        // 默认使用 Linux/Android 正则表达式
        Pattern pattern;
        if (ua.contains("Android")) {
            // Linux/Android 正则
            pattern = Pattern.compile("linux;.*(android).([\\w.,/\\-]+)", Pattern.CASE_INSENSITIVE);
        } else {
            // iOS 正则
            pattern = Pattern.compile("(ip[honead]+)(?:.*os.([\\w.,/\\-]+).like|;\\sopera)", Pattern.CASE_INSENSITIVE);
        }

        Matcher matcher = pattern.matcher(ua);
        String name = "";
        String version = "";

        if (matcher.find()) {
            name = matcher.group(1);
            if (matcher.groupCount() > 1) {
                version = matcher.group(2);
                if (version != null) {
                    version = version.replaceAll(cleanPattern, ".");
                }
            }
        }

        return name + ":" + version;
    }

    /**
     * 使用更全面的正则表达式，提高匹配准确性
     */
    public static String uaAnalysisImproved(String ua) {
        if (ua == null || ua.trim().isEmpty()) {
            return ":";
        }

        // 多种操作系统正则表达式
        String[][] osPatterns = {
                // Android
                {"(?i).*android.*?([\\d.]+).*", "Android"},
                // iOS
                {"(?i).*(iphone|ipad|ipod).*?os\\s+([\\d_]+).*", "iOS"},
                {"(?i).*(iphone|ipad|ipod).*", "iOS"},
                // Windows
                {"(?i).*windows\\s+nt\\s+([\\d.]+).*", "Windows"},
                // Mac OS
                {"(?i).*mac\\s+os\\s+x\\s+([\\d._]+).*", "Mac OS"},
                // Linux
                {"(?i).*linux.*", "Linux"}
        };

        String osName = "";
        String osVersion = "";

        for (String[] pattern : osPatterns) {
            Pattern p = Pattern.compile(pattern[0]);
            Matcher m = p.matcher(ua);
            if (m.matches()) {
                osName = pattern[1];
                if (m.groupCount() >= 1 && m.group(1) != null) {
                    osVersion = m.group(1).replaceAll("[^\\d._]", ".");
                }
                break;
            }
        }

        return osName + ":" + osVersion;
    }

}
