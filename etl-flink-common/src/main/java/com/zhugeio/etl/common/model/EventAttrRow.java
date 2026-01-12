package com.zhugeio.etl.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 事件属性表行数据 (b_user_event_attr_{appId})
 *
 * Doris 表结构 (可配置列数):
 * - 列 0-39: 基础字段 (40列，固定)
 * - 列 40-(40+N-1): 自定义属性值 cus1-cusN (N列，可配置)
 * - 列 (40+N)-(40+2N-1): 属性类型 type1-typeN (N列，可配置)
 * - 列 (40+2N)-(40+2N+1): 分区字段 eid, yw (2列，固定)
 * - 最后一列: __DORIS_DELETE_SIGN__ (隐藏列)
 *
 * 配置方式:
 * - 配置项: event.attr.custom.columns (默认 100)
 * - 例如配置 150，则总列数 = 40 + 150 + 150 + 2 + 1 = 343
 */
public class EventAttrRow implements Serializable {

    private static final long serialVersionUID = 1L;

    // ============ 可配置参数 ============

    /** 自定义属性列数（默认100，可通过 configure() 修改） */
    private static int CUSTOM_COLUMNS = 100;

    /** 类型列数（与自定义属性列数相同） */
    private static int TYPE_COLUMNS = 100;

    /** 总列数（动态计算） */
    private static int TOTAL_COLUMNS = calculateTotalColumns();

    // ============ 固定参数 ============

    /** 基础字段数（固定40列） */
    public static final int BASIC_COLUMNS = 40;

    /** 分区字段数（固定2列：eid, yw） */
    public static final int PARTITION_COLUMNS = 2;

    /** 自定义属性起始索引（固定从40开始） */
    public static final int CUS_START_INDEX = 40;

    /** NULL 值表示 */
    public static final String NULL_VALUE = "\\N";

    /** 删除标记值：0=不删除，1=删除 */
    public static final String DELETE_SIGN_FALSE = "0";
    public static final String DELETE_SIGN_TRUE = "1";

    // ============ 动态计算的索引 ============

    /** 类型列起始索引 */
    private static int TYPE_START_INDEX = CUS_START_INDEX + CUSTOM_COLUMNS;

    /** eid 索引 */
    private static int EID_INDEX = TYPE_START_INDEX + TYPE_COLUMNS;

    /** yw 索引 */
    private static int YW_INDEX = EID_INDEX + 1;

    /** __DORIS_DELETE_SIGN__ 索引 */
    private static int DELETE_SIGN_INDEX = YW_INDEX + 1;

    // ============ 实例字段 ============

    /** 应用ID */
    private Integer appId;

    /** 事件ID (用于分区) */
    private String eventId;

    /** 所有列数据 */
    private final List<String> columns;

    // ============ 配置方法 ============

    /**
     * 配置自定义属性列数（需要在创建对象前调用）
     *
     * @param customColumns 自定义属性列数（如 100, 150, 200 等）
     */
    public static synchronized void configure(int customColumns) {
        if (customColumns < 1) {
            throw new IllegalArgumentException("customColumns must be >= 1");
        }

        CUSTOM_COLUMNS = customColumns;
        TYPE_COLUMNS = customColumns;  // 类型列数与自定义属性列数相同

        // 重新计算索引
        TYPE_START_INDEX = CUS_START_INDEX + CUSTOM_COLUMNS;
        EID_INDEX = TYPE_START_INDEX + TYPE_COLUMNS;
        YW_INDEX = EID_INDEX + 1;
        DELETE_SIGN_INDEX = YW_INDEX + 1;
        TOTAL_COLUMNS = calculateTotalColumns();

        System.out.println("[EventAttrRow] 配置更新: customColumns=" + customColumns +
                ", totalColumns=" + TOTAL_COLUMNS);
    }

    /**
     * 计算总列数
     */
    private static int calculateTotalColumns() {
        // 基础字段 + 自定义属性 + 类型列 + 分区字段 + 删除标记
        return BASIC_COLUMNS + CUSTOM_COLUMNS + TYPE_COLUMNS + PARTITION_COLUMNS + 1;
    }

    /**
     * 获取当前配置的自定义属性列数
     */
    public static int getCustomColumns() {
        return CUSTOM_COLUMNS;
    }

    /**
     * 获取当前配置的总列数
     */
    public static int getTotalColumns() {
        return TOTAL_COLUMNS;
    }

    // ============ 构造函数 ============

    public EventAttrRow() {
        this.columns = new ArrayList<>(TOTAL_COLUMNS);
        // 初始化所有列为 NULL
        for (int i = 0; i < TOTAL_COLUMNS; i++) {
            columns.add(NULL_VALUE);
        }
        // 默认不删除
        columns.set(DELETE_SIGN_INDEX, DELETE_SIGN_FALSE);
    }

    public EventAttrRow(Integer appId) {
        this();
        this.appId = appId;
    }

    // ============ 基础字段设置方法 (Doris 列顺序) ============

    /** 列0: 诸葛ID */
    public void setZgId(String value) { setColumn(0, value); }

    /** 列1: 会话ID */
    public void setSessionId(String value) { setColumn(1, value); }

    /** 列2: UUID (区分同一时刻触发的同一事件) */
    public void setUuid(String value) { setColumn(2, value); }

    /** 列3: 事件ID (zg_eid) */
    public void setEventIdColumn(String value) {
        setColumn(3, value);
        this.eventId = value;
    }

    /** 列4: 日期ID (yyyyMMdd) - Doris 在 date 前面 */
    public void setBeginDayId(String value) { setColumn(4, value); }

    /** 列5: 开始时间戳 (秒) */
    public void setBeginDate(String value) { setColumn(5, value); }

    /** 列6: 时间ID (HH0SSS) */
    public void setBeginTimeId(String value) { setColumn(6, value); }

    /** 列7: 设备ID */
    public void setDeviceId(String value) { setColumn(7, value); }

    /** 列8: 用户ID */
    public void setUserId(String value) { setColumn(8, value); }

    /** 列9: 事件名称 ($eid) */
    public void setEventName(String value) { setColumn(9, value); }

    /** 列10: 平台 */
    public void setPlatform(Integer value) { setColumn(10, value != null ? String.valueOf(value) : NULL_VALUE); }

    /** 列11: 网络类型 */
    public void setNetwork(String value) { setColumn(11, value); }

    /** 列12: 运营商 (mccmnc) */
    public void setMccmnc(String value) { setColumn(12, value); }

    /** 列13: UserAgent */
    public void setUseragent(String value) { setColumn(13, value); }

    /** 列14: 来源网站 */
    public void setWebsite(String value) { setColumn(14, value); }

    /** 列15: 当前URL */
    public void setCurrentUrl(String value) { setColumn(15, value); }

    /** 列16: 来源URL */
    public void setReferrerUrl(String value) { setColumn(16, value); }

    /** 列17: 渠道 */
    public void setChannel(String value) { setColumn(17, value); }

    /** 列18: 应用版本 */
    public void setAppVersion(String value) { setColumn(18, value); }

    /** 列19: IP (数字格式) */
    public void setIp(Long value) { setColumn(19, value != null ? String.valueOf(value) : NULL_VALUE); }

    /** 列20: IP (字符串格式) */
    public void setIpStr(String value) { setColumn(20, value); }

    /** 列21: 国家 */
    public void setCountry(String value) { setColumn(21, value); }

    /** 列22: 地区/省份 */
    public void setArea(String value) { setColumn(22, value); }

    /** 列23: 城市 */
    public void setCity(String value) { setColumn(23, value); }

    /** 列24: 操作系统 */
    public void setOs(String value) { setColumn(24, value); }

    /** 列25: 操作系统版本 */
    public void setOv(String value) { setColumn(25, value); }

    /** 列26: 浏览器 */
    public void setBs(String value) { setColumn(26, value); }

    /** 列27: 浏览器版本 */
    public void setBv(String value) { setColumn(27, value); }

    /** 列28: 广告来源 */
    public void setUtmSource(String value) { setColumn(28, value); }

    /** 列29: 广告媒介 */
    public void setUtmMedium(String value) { setColumn(29, value); }

    /** 列30: 广告名称 */
    public void setUtmCampaign(String value) { setColumn(30, value); }

    /** 列31: 广告内容 */
    public void setUtmContent(String value) { setColumn(31, value); }

    /** 列32: 广告关键词 */
    public void setUtmTerm(String value) { setColumn(32, value); }

    /** 列33: 持续时间 (毫秒) */
    public void setDuration(String value) { setColumn(33, value); }

    /** 列34: UTC时间戳 */
    public void setUtcDate(String value) { setColumn(34, value); }

    /** 列35: attr1 (业务标识) */
    public void setAttr1(String value) { setColumn(35, value); }

    /** 列36: attr2 (微信小程序appId) */
    public void setAttr2(String value) { setColumn(36, value); }

    /** 列37: attr3 */
    public void setAttr3(String value) { setColumn(37, value); }

    /** 列38: attr4 */
    public void setAttr4(String value) { setColumn(38, value); }

    /** 列39: attr5 (zgid_sessionid) */
    public void setAttr5(String value) { setColumn(39, value); }

    // ============ 自定义属性设置方法 ============

    /**
     * 设置自定义属性值
     * @param colIndex 列索引 1-N（N 为配置的 CUSTOM_COLUMNS）
     * @param value 属性值
     */
    public void setCustomProperty(int colIndex, String value) {
        if (colIndex >= 1 && colIndex <= CUSTOM_COLUMNS) {
            setColumn(CUS_START_INDEX - 1 + colIndex, value);
        }
    }

    /**
     * 设置属性类型
     * @param colIndex 列索引 1-N（N 为配置的 TYPE_COLUMNS）
     * @param type 属性类型
     */
    public void setPropertyType(int colIndex, String type) {
        if (colIndex >= 1 && colIndex <= TYPE_COLUMNS) {
            setColumn(TYPE_START_INDEX - 1 + colIndex, type);
        }
    }

    // ============ 分区字段设置方法 ============

    /** 设置事件ID分区字段 */
    public void setEid(String value) { setColumn(EID_INDEX, value); }

    /** 设置年周 */
    public void setYw(String value) { setColumn(YW_INDEX, value); }

    // ============ 删除标记设置方法 ============

    /**
     * 设置删除标记
     * @param delete true=标记删除，false=不删除
     */
    public void setDeleteSign(boolean delete) {
        columns.set(DELETE_SIGN_INDEX, delete ? DELETE_SIGN_TRUE : DELETE_SIGN_FALSE);
    }

    /**
     * 标记为删除
     */
    public void markDeleted() {
        setDeleteSign(true);
    }

    // ============ 通用方法 ============

    private void setColumn(int index, String value) {
        if (index >= 0 && index < TOTAL_COLUMNS) {
            columns.set(index, value != null && !value.isEmpty() ? value : NULL_VALUE);
        }
    }

    public String getColumn(int index) {
        if (index >= 0 && index < TOTAL_COLUMNS) {
            return columns.get(index);
        }
        return NULL_VALUE;
    }

    public List<String> getColumns() {
        return columns;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getEventId() {
        return eventId;
    }

    /**
     * 获取表名
     */
    public String getTableName() {
        return "b_user_event_attr_" + appId;
    }

    /**
     * 获取去重键
     * 格式: zg_id + session_id + uuid + event_id + begin_day_id + begin_date
     */
    public String getDedupeKey() {
        return columns.get(0) + "_" + columns.get(1) + "_" + columns.get(2) + "_" +
                columns.get(3) + "_" + columns.get(4) + "_" + columns.get(5);
    }

    /**
     * 转换为 TSV 格式 (Tab分隔)
     * 包含所有列，包括 __DORIS_DELETE_SIGN__
     */
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        for (String col : columns) {
            joiner.add(col);
        }
        return joiner.toString();
    }

    // Doris 列名 (基础字段，按顺序)
    private static final String[] BASIC_COLUMN_NAMES = {
            "zg_id", "session_id", "uuid", "event_id",
            "begin_day_id", "begin_date", "begin_time_id",
            "device_id", "user_id", "event_name",
            "platform", "network", "mccmnc", "useragent",
            "website", "current_url", "referrer_url",
            "channel", "app_version", "ip", "ip_str",
            "country", "area", "city",
            "os", "ov", "bs", "bv",
            "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
            "duration", "utc_date",
            "attr1", "attr2", "attr3", "attr4", "attr5"
    };

    /**
     * 转换为 JSON 格式（用于 Doris Stream Load）
     * 只输出非空字段，减少数据量
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder(512);
        sb.append("{");
        boolean first = true;

        // 基础字段 (列 0-39)
        for (int i = 0; i < BASIC_COLUMN_NAMES.length && i < columns.size(); i++) {
            String value = columns.get(i);
            if (!isNullValue(value)) {
                if (!first) sb.append(",");
                sb.append("\"").append(BASIC_COLUMN_NAMES[i]).append("\":\"")
                        .append(escapeJson(value)).append("\"");
                first = false;
            }
        }

        // 自定义属性 cus1-cusN (列 40 开始)
        for (int i = 1; i <= CUSTOM_COLUMNS; i++) {
            int colIndex = CUS_START_INDEX - 1 + i;
            if (colIndex < columns.size()) {
                String value = columns.get(colIndex);
                if (!isNullValue(value)) {
                    if (!first) sb.append(",");
                    sb.append("\"cus").append(i).append("\":\"")
                            .append(escapeJson(value)).append("\"");
                    first = false;
                }
            }
        }

        // 类型字段 type1-typeN
        for (int i = 1; i <= TYPE_COLUMNS; i++) {
            int colIndex = TYPE_START_INDEX - 1 + i;
            if (colIndex < columns.size()) {
                String value = columns.get(colIndex);
                if (!isNullValue(value)) {
                    if (!first) sb.append(",");
                    sb.append("\"type").append(i).append("\":\"")
                            .append(escapeJson(value)).append("\"");
                    first = false;
                }
            }
        }

        // 分区字段 eid, yw
        String eid = columns.get(EID_INDEX);
        if (!isNullValue(eid)) {
            if (!first) sb.append(",");
            sb.append("\"eid\":\"").append(escapeJson(eid)).append("\"");
            first = false;
        }

        String yw = columns.get(YW_INDEX);
        if (!isNullValue(yw)) {
            if (!first) sb.append(",");
            sb.append("\"yw\":\"").append(escapeJson(yw)).append("\"");
            first = false;
        }

        // 删除标记（必须输出）
        if (!first) sb.append(",");
        sb.append("\"__DORIS_DELETE_SIGN__\":").append(columns.get(DELETE_SIGN_INDEX));

        sb.append("}");
        return sb.toString();
    }

    private boolean isNullValue(String value) {
        return value == null || value.isEmpty() || NULL_VALUE.equals(value);
    }

    private String escapeJson(String value) {
        if (value == null) return "";
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * 获取当前类型起始索引（供外部使用）
     */
    public static int getTypeStartIndex() {
        return TYPE_START_INDEX;
    }

    /**
     * 获取 EID 索引（供外部使用）
     */
    public static int getEidIndex() {
        return EID_INDEX;
    }

    /**
     * 获取 YW 索引（供外部使用）
     */
    public static int getYwIndex() {
        return YW_INDEX;
    }

    @Override
    public String toString() {
        return "EventAttrRow{" +
                "appId=" + appId +
                ", eventId='" + eventId + '\'' +
                ", zgId='" + columns.get(0) + '\'' +
                ", eventName='" + columns.get(9) + '\'' +
                ", totalColumns=" + TOTAL_COLUMNS +
                '}';
    }
}