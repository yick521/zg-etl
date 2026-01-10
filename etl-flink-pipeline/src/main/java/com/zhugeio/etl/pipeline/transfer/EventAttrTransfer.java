package com.zhugeio.etl.pipeline.transfer;

import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.model.EventAttrRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * 事件属性转换器
 *
 * 注意:
 * - 关键词富化已在上游 SearchKeywordEnrichOperator 中完成
 * - 本类直接从 pr 中读取 $utm_term、$search_engine 等已富化字段
 * - 不再需要 eqidKeywords 参数
 *
 * 优化点:
 * 1. 批量收集所有需要查询的属性
 * 2. 并行发起异步查询
 * 3. 统一等待结果，只阻塞一次
 */
public class EventAttrTransfer implements Serializable {

    private static final long serialVersionUID = 3L;
    private static final Logger LOG = LoggerFactory.getLogger(EventAttrTransfer.class);

    private static final String NULL_VALUE = "\\N";

    private static final String WWW_BAIDU_COM = ".baidu.com";
    private static final String WWW_SOGOU_COM = ".sogou.com";
    private static final String CN_BING_COM = ".bing.com";
    private static final String WWW_SO_COM = ".so.com";
    private static final String WWW_GOOGLE_COM = ".google.com";
    private static final String WWW_GOOGLE_CO = ".google.co";
    private static final String M_SM_CN = "m.sm.cn";

    private static final Set<String> CUSTOM_PROPERTY_DT = new HashSet<>(Arrays.asList("evt", "mkt", "abp"));

    private final int expireSubDays;
    private final int expireAddDays;
    private final ConfigCacheService configCacheService;
    private final int eventAttrLengthLimit;

    private Set<String> mktAttrs;
    private Set<String> abpAttrs;

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));
    private static final ThreadLocal<SimpleDateFormat> YEAR_WEEK_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("YYYYww"));

    public EventAttrTransfer(ConfigCacheService configCacheService, int expireSubDays, int expireAddDays, int eventAttrLengthLimit) {
        this.configCacheService = configCacheService;
        this.expireSubDays = expireSubDays;
        this.expireAddDays = expireAddDays;
        this.eventAttrLengthLimit = eventAttrLengthLimit;

        this.mktAttrs = new HashSet<>(Arrays.asList(
                "ct", "tz", "zg_zgid", "zg_did", "zg_eid", "zg_sid", "zg_uid", "eid", "uuid"
        ));
        this.abpAttrs = new HashSet<>(Arrays.asList(
                "ct", "tz", "zg_zgid", "zg_did", "zg_eid", "zg_sid", "zg_uid", "eid", "uuid"
        ));
    }

    public void setMktAttrs(Set<String> mktAttrs) { this.mktAttrs = mktAttrs; }
    public void setAbpAttrs(Set<String> abpAttrs) { this.abpAttrs = abpAttrs; }

    /**
     * 转换事件数据 (从 Map) - 精简版
     *
     * 注意: 关键词已由上游 SearchKeywordEnrichOperator 填充到 pr 中
     * - $utm_term: 搜索关键词
     * - $search_engine: 搜索引擎
     *
     * @param appId 应用ID
     * @param platform 平台
     * @param dt 数据类型 (evt/vtl/mkt/ss/se/abp)
     * @param pr 属性数据 (已包含富化后的关键词)
     * @param ip IP地址
     * @param ipResult IP解析结果 [国家, 省份, 城市]
     * @param ua User-Agent
     * @param uaResult UA解析结果
     * @param business 业务标识
     * @return EventAttrRow
     */
    public CompletableFuture<EventAttrRow> transferFromMapAsync(Integer appId, Integer platform, String dt, Map<String, Object> pr,
                                                                String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                                                String business) {

        String zgId = getStringValue(pr, "$zg_zgid");
        String zgEid = getStringValue(pr, "$zg_eid");
        String zgDid = getStringValue(pr, "$zg_did");

        if (isNullOrEmpty(zgId) || isNullOrEmpty(zgEid) || isNullOrEmpty(zgDid)) {
            return CompletableFuture.completedFuture(null);
        }

        Long ct = getLongValue(pr, "$ct");
        Integer tz = getIntValue(pr, "$tz");

        if (ct == null || tz == null) {
            return CompletableFuture.completedFuture(null);
        }

        String realtime = timestampToDateString(ct, tz);
        if (isNullOrEmpty(realtime) || isExpiredTime(realtime)) {
            return CompletableFuture.completedFuture(null);
        }

        Map<String, String> timeComponents = getTimeComponents(ct);
        if (timeComponents.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        EventAttrRow row = new EventAttrRow(appId);

        // 填充基础字段（同步，无网络调用）
        fillBasicFieldsFromMap(row, pr, platform, dt, ip, ipResult, ua, uaResult,
                business, realtime, timeComponents);

        row.setEid(zgEid);
        row.setYw(getYearWeek(realtime));

        // 填充自定义属性 - 批量优化
        if (CUSTOM_PROPERTY_DT.contains(dt)) {
            return fillCustomPropertiesBatchAsync(row, pr, dt, zgEid)
                    .thenApply(v -> row);
        } else {
            return CompletableFuture.completedFuture(row);
        }
    }

    /**
     * 自定义属性查询封装
     */
    private static class CustomPropQuery {
        final String key;      // 原始 key，如 "_商品名称"
        final String propId;   // 属性ID

        CustomPropQuery(String key, String propId) {
            this.key = key;
            this.propId = propId;
        }
    }

    /**
     * 批量优化版：填充自定义属性
     *
     * 优化流程：
     * 1. 收集所有需要查询的属性（propId 列表）
     * 2. 调用 batchGetEventAttrColumnIndex 批量查询
     * 3. 根据结果填充 row
     */
    private CompletableFuture<Void> fillCustomPropertiesBatchAsync(EventAttrRow row, Map<String, Object> pr,
                                                                   String dt, String zgEid) {
        Set<String> attrSet = getAttrSet(dt);
        int customColumns = EventAttrRow.getCustomColumns();

        // Step 1: 收集所有需要查询的属性
        List<CustomPropQuery> queries = new ArrayList<>();

        for (String key : pr.keySet()) {
            boolean isCustomProp = false;

            if ("evt".equals(dt) && key.startsWith("_")) {
                isCustomProp = true;
            } else if (("mkt".equals(dt) || "abp".equals(dt)) && !key.startsWith("$")
                    && !attrSet.contains(key.replace("$", ""))) {
                isCustomProp = true;
            }

            if (isCustomProp) {
                String propIdKey = "$zg_epid#" + key;
                String propId = getStringValue(pr, propIdKey);

                if (!isNullOrEmpty(propId)) {
                    queries.add(new CustomPropQuery(key, propId));
                }
            }
        }

        if (queries.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Step 2: 收集所有 propId
        List<String> propIds = new ArrayList<>(queries.size());
        Map<String, CustomPropQuery> propIdToQuery = new HashMap<>();

        for (CustomPropQuery query : queries) {
            propIds.add(query.propId);
            propIdToQuery.put(query.propId, query);
        }

        // Step 3: 批量查询
        return configCacheService.batchGetEventAttrColumnIndex(zgEid, propIds)
                .thenAccept(columnIndexMap -> {
                    // Step 4: 根据结果填充 row
                    for (Map.Entry<String, Integer> entry : columnIndexMap.entrySet()) {
                        String propId = entry.getKey();
                        Integer colIndex = entry.getValue();

                        if (colIndex != null && colIndex >= 1 && colIndex <= customColumns) {
                            CustomPropQuery query = propIdToQuery.get(propId);
                            if (query != null) {
                                String propValue = ensureLength(getStringValue(pr, query.key), eventAttrLengthLimit);
                                row.setCustomProperty(colIndex, propValue);

                                String propTypeKey = "$zg_eptp#" + query.key;
                                String propType = ensureLength(getStringValue(pr, propTypeKey), 256);
                                row.setPropertyType(colIndex, propType);
                            }
                        }
                    }
                })
                .exceptionally(ex -> {
                    LOG.warn("批量填充自定义属性失败: zgEid={}, count={}", zgEid, queries.size(), ex);
                    return null;
                });
    }

    /**
     * 填充基础字段
     */
    private void fillBasicFieldsFromMap(EventAttrRow row, Map<String, Object> pr, Integer platform, String dt,
                                        String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                        String business, String realtime, Map<String, String> timeComponents) {

        // 列0-3: 核心ID
        row.setZgId(getStringValue(pr, "$zg_zgid"));
        row.setSessionId(getStringValue(pr, "$zg_sid"));
        row.setUuid(ensureLength(getStringValue(pr, "$uuid"), 256));
        row.setEventIdColumn(getStringValue(pr, "$zg_eid"));

        // 列4-6: 时间字段
        row.setBeginDayId(timeComponents.get("day"));
        row.setBeginDate(String.valueOf(Timestamp.valueOf(realtime).getTime() / 1000));
        row.setBeginTimeId(timeComponents.get("hour") + "0" + timeComponents.get("mill"));

        // 列7-9: 用户/设备/事件
        row.setDeviceId(getStringValue(pr, "$zg_did"));
        row.setUserId(getStringValue(pr, "$zg_uid"));
        row.setEventName(ensureLength(getEventNameFromMap(pr), 256));

        // 列10-13: 平台/网络
        row.setPlatform(platform);
        row.setNetwork(ensureNetwork(getStringValue(pr, "$net")));
        row.setMccmnc(ensureIntLength(getStringValue(pr, "$cr"), 256));
        row.setUseragent(ensureLength(ua, 256));

        // 列14-16: URL
        String website = ensureLength(getStringValue(pr, "$referrer_domain"), 1088);
        String currentUrl = ensureLength(getStringValue(pr, "$url"), 1088);
        String referrerUrl = ensureLength(getStringValue(pr, "$ref"), 1088);
        LOG.debug("DEBUG UTM: website={}, isSearchEngine={}, referrerUrl={}",
                website, isSearchEngine(website), referrerUrl);

        row.setWebsite(website);
        row.setCurrentUrl(currentUrl);
        row.setReferrerUrl(referrerUrl);

        // 列17-18: 渠道/版本
        row.setChannel(ensureLength(getStringValue(pr, "$cn"), 256));
        row.setAppVersion(ensureLength(getStringValue(pr, "$vn"), 256));

        // 列19-20: IP
        Long ipLong = ipToLong(ip);
        row.setIp(ipLong);
        row.setIpStr(ensureLength(ip, 256));

        // 列21-23: 地理位置
        if (ipResult != null && ipResult.length >= 3) {
            row.setCountry(ensureLength(ipResult[0], 256));
            row.setArea(ensureLength(ipResult[1], 256));
            row.setCity(ensureLength(ipResult[2], 256));
        } else {
            row.setCountry(NULL_VALUE);
            row.setArea(NULL_VALUE);
            row.setCity(NULL_VALUE);
        }
        LOG.debug("DEBUG uaResult: {}", uaResult);
        // 列24-27: 操作系统/浏览器
        if (uaResult != null) {
            row.setOs(ensureLength(uaResult.get("os"), 256));
            row.setOv(ensureLength(uaResult.get("os_version"), 256));
            row.setBs(ensureLength(uaResult.get("browser"), 256));
            row.setBv(ensureLength(uaResult.get("browser_version"), 256));
        } else {
            row.setOs(NULL_VALUE);
            row.setOv(NULL_VALUE);
            row.setBs(NULL_VALUE);
            row.setBv(NULL_VALUE);
        }

        // 列28-32: UTM (关键词已由上游富化)
        String source = getStringValue(pr, "$utm_source");
        String medium = getStringValue(pr, "$utm_medium");
        String campaign = getStringValue(pr, "$utm_campaign");
        String utmContent = getStringValue(pr, "$utm_content");
        String utmTerm = getStringValue(pr, "$utm_term");

        // 🔍 关键调试日志
        LOG.debug("DEBUG UTM VALUES: source={}, medium={}, campaign={}, utmContent={}, utmTerm={}",
                source, medium, campaign, utmContent, utmTerm);
        LOG.debug("DEBUG UTM EMPTY CHECK: source={}, medium={}, campaign={}, utmContent={}, utmTerm={}",
                isNullOrEmpty(source), isNullOrEmpty(medium), isNullOrEmpty(campaign),
                isNullOrEmpty(utmContent), isNullOrEmpty(utmTerm));

        // 如果上游没有填充 UTM，则按原逻辑处理
        if ((isNullOrEmpty(source) && isNullOrEmpty(medium)
                && isNullOrEmpty(campaign) && isNullOrEmpty(utmContent))) {

            if (isSearchEngine(website)) {
                medium = "搜索自然流量";
                source = website;
                if (isNullOrEmpty(utmTerm)) {
                    utmTerm = getUtmTermFromRef(null, referrerUrl);
                }
            } else {
                if (isNullOrEmpty(referrerUrl)) {
                    medium = NULL_VALUE;
                    source = NULL_VALUE;
                } else {
                    medium = "引荐";
                    source = website;
                    if (isNullOrEmpty(utmTerm)) {
                        utmTerm = getUtmTermFromRef(null, referrerUrl);
                    }
                }
            }
        } else {
            source = ensureLength(isNullOrEmpty(source) ? website : source, 256);
        }

        row.setUtmSource(ensureLength(source, 256));
        row.setUtmMedium(ensureLength(medium, 256));
        row.setUtmCampaign(ensureLength(campaign, 256));
        row.setUtmContent(ensureLength(utmContent, 256));
        row.setUtmTerm(ensureLength(utmTerm, 256));

        // 列33-34: 持续时间/UTC
        row.setDuration(ensureIntRange(getStringValue(pr, "$dru"), 0, 86400000));
        row.setUtcDate(String.valueOf(System.currentTimeMillis()));

        // 列35-39: attr
        row.setAttr1(ensureLength(business, 256));
        row.setAttr2(ensureLength(getStringValue(pr, "$wxeid"), 256));
        row.setAttr3(ensureLength(getStringValue(pr, "$env_attr3"), 256));
        row.setAttr4(NULL_VALUE);
        row.setAttr5(getStringValue(pr, "$zg_zgid") + "_" + getStringValue(pr, "$zg_sid"));
    }

    // ============ 工具方法 ============

    private Set<String> getAttrSet(String dt) {
        if ("mkt".equals(dt)) { return mktAttrs; }
        if ("abp".equals(dt)) { return abpAttrs; }
        return Collections.emptySet();
    }

    private String getEventNameFromMap(Map<String, Object> pr) {
        String zgEid = getStringValue(pr, "$zg_eid");
        if ("-1".equals(zgEid)) { return "st"; }
        if ("-2".equals(zgEid)) { return "se"; }
        return getStringValue(pr, "$eid");
    }

    private String timestampToDateString(Long ct, Integer tz) {
        if (ct == null || tz == null) { return NULL_VALUE; }
        if (Math.abs(tz) > 48 * 3600 * 1000) { return NULL_VALUE; }
        try { return DATE_FORMAT.get().format(new Date(ct)); }
        catch (Exception e) { return NULL_VALUE; }
    }

    private Map<String, String> getTimeComponents(Long ct) {
        Map<String, String> result = new HashMap<>();
        try {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ct);
            result.put("day", DAY_FORMAT.get().format(cal.getTime()));
            result.put("hour", String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
            result.put("mill", String.format("%03d", cal.get(Calendar.MILLISECOND)));
        } catch (Exception e) {
            LOG.warn("Failed to get time components", e);
        }
        return result;
    }

    private String getYearWeek(String realtime) {
        try {
            Date date = DATE_FORMAT.get().parse(realtime);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_MONTH, -1);
            return YEAR_WEEK_FORMAT.get().format(cal.getTime());
        } catch (Exception e) { return NULL_VALUE; }
    }

    private boolean isExpiredTime(String realtime) {
        try {
            Date eventDate = DATE_FORMAT.get().parse(realtime);
            long eventTime = eventDate.getTime();

            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, expireAddDays);
            cal.set(Calendar.HOUR_OF_DAY, 23);
            cal.set(Calendar.MINUTE, 59);
            cal.set(Calendar.SECOND, 59);
            long maxTime = cal.getTimeInMillis();

            cal.setTime(new Date());
            cal.add(Calendar.DAY_OF_MONTH, -expireSubDays);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            long minTime = cal.getTimeInMillis();

            return eventTime < minTime || eventTime > maxTime;
        } catch (Exception e) { return true; }
    }

    private boolean isSearchEngine(String website) {
        if (isNullOrEmpty(website)) { return false; }
        return website.contains(WWW_BAIDU_COM) || website.contains(WWW_SOGOU_COM) ||
                website.contains(CN_BING_COM) || website.contains(WWW_SO_COM) ||
                website.contains(M_SM_CN) || website.contains(WWW_GOOGLE_COM) ||
                website.contains(WWW_GOOGLE_CO);
    }

    private String getUtmTermFromRef(String utmTerm, String referrerUrl) {
        if (!isNullOrEmpty(utmTerm)) { return utmTerm; }
        if (isNullOrEmpty(referrerUrl)) { return NULL_VALUE; }

        try {
            java.net.URI uri = new java.net.URI(referrerUrl);
            String host = uri.getHost();
            String query = uri.getQuery();

            if (query == null) { return NULL_VALUE; }

            Map<String, String> params = parseQueryString(query);

            if (host != null) {
                if (host.contains(WWW_SOGOU_COM)) {
                    String keyword = params.get("query");
                    if (keyword != null) { return java.net.URLDecoder.decode(keyword, "UTF-8"); }
                } else if (host.contains(CN_BING_COM) || host.contains(WWW_SO_COM)
                        || host.contains(M_SM_CN) || host.contains(WWW_GOOGLE_COM)
                        || host.contains(WWW_GOOGLE_CO)) {
                    String keyword = params.get("q");
                    if (keyword != null) { return java.net.URLDecoder.decode(keyword, "UTF-8"); }
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to parse referrer URL: {}", referrerUrl, e);
        }
        return NULL_VALUE;
    }

    private Map<String, String> parseQueryString(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null) { return params; }
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2) {
                params.put(pair[0], pair[1]);
            }
        }
        return params;
    }

    private String getStringValue(Map<String, Object> map, String key) {
        if (map == null || key == null) { return NULL_VALUE; }
        Object value = map.get(key);
        return value != null ? String.valueOf(value) : NULL_VALUE;
    }

    private Long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) { return null; }
        if (value instanceof Number) { return ((Number) value).longValue(); }
        try { return Long.parseLong(String.valueOf(value)); }
        catch (NumberFormatException e) { return null; }
    }

    private Integer getIntValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) { return null; }
        if (value instanceof Number) { return ((Number) value).intValue(); }
        try { return Integer.parseInt(String.valueOf(value)); }
        catch (NumberFormatException e) { return null; }
    }

    private boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty() || NULL_VALUE.equals(value);
    }

    private String ensureLength(String value, int maxLength) {
        if (isNullOrEmpty(value)) { return NULL_VALUE; }
        value = value.replaceAll("[\t\n\r\"\\\\\u0000]", " ").trim();
        return value.length() > maxLength ? value.substring(0, maxLength) : value;
    }

    private String ensureNetwork(String value) {
        if ("-1".equals(value)) { return NULL_VALUE; }
        return ensureIntLength(value, 256);
    }

    private String ensureIntLength(String value, int maxLength) {
        if (isNullOrEmpty(value) || "null".equals(value)) { return NULL_VALUE; }
        if (value.length() > 6 || !value.matches("[0-9]*")) { return NULL_VALUE; }
        return value;
    }

    private String ensureIntRange(String value, int min, int max) {
        if (isNullOrEmpty(value)) { return "0"; }
        try {
            int intValue = Integer.parseInt(value);
            if (intValue >= min && intValue <= max) { return value; }
        } catch (NumberFormatException e) { }
        return "0";
    }

    private Long ipToLong(String ip) {
        if (isNullOrEmpty(ip)) { return null; }
        try {
            String[] parts = ip.split("\\.");
            if (parts.length != 4) { return null; }
            long result = 0;
            for (int i = 0; i < 4; i++) {
                result = (result << 8) | Integer.parseInt(parts[i]);
            }
            return result;
        } catch (Exception e) { return null; }
    }
}