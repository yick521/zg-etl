package com.zhugeio.etl.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 用户属性表行数据 (b_user_property_{appId})
 *
 * 表结构 (8列):
 * - zg_id BIGINT
 * - property_id INT
 * - user_id BIGINT
 * - property_name VARCHAR
 * - property_data_type VARCHAR
 * - property_value VARCHAR
 * - platform SMALLINT
 * - last_update_date BIGINT
 */
public class UserPropertyRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String NULL_VALUE = "\\N";

    @JsonIgnore
    private Integer appId;

    @JsonIgnore
    private boolean cdpMode;

    // 字段值 - 类型匹配 Doris
    @JsonProperty("zg_id")
    private Long zgId;  // BIGINT

    @JsonProperty("property_id")
    private Integer propertyId;  // INT

    @JsonProperty("user_id")
    private Long userId;  // BIGINT

    @JsonProperty("property_name")
    private String propertyName;  // VARCHAR

    @JsonProperty("property_data_type")
    private String propertyDataType;  // VARCHAR

    @JsonProperty("property_value")
    private String propertyValue;  // VARCHAR

    @JsonProperty("platform")
    private Integer platform;  // SMALLINT

    @JsonProperty("last_update_date")
    private Long lastUpdateDate;  // BIGINT

    public UserPropertyRow() {
        this.cdpMode = false;
    }

    public UserPropertyRow(Integer appId, boolean cdpMode) {
        this.appId = appId;
        this.cdpMode = cdpMode;
    }

    // ========== 工具方法：统一处理 null 值 ==========

    /**
     * 字符串标准化：把 \N 和空字符串转为 null
     */
    private static String normalizeNull(String value) {
        return (value == null || value.isEmpty() || NULL_VALUE.equals(value)) ? null : value;
    }

    /**
     * 解析 Long，处理 \N 和空字符串
     */
    private static Long parseLong(String value) {
        String normalized = normalizeNull(value);
        if (normalized == null) {
            return null;
        }
        try {
            return Long.parseLong(normalized);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 解析 Integer，处理 \N 和空字符串
     */
    private static Integer parseInt(String value) {
        String normalized = normalizeNull(value);
        if (normalized == null) {
            return null;
        }
        try {
            return Integer.parseInt(normalized);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ========== Getters and Setters ==========

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public boolean isCdpMode() {
        return cdpMode;
    }

    public void setCdpMode(boolean cdpMode) {
        this.cdpMode = cdpMode;
    }

    public Long getZgId() {
        return zgId;
    }

    public void setZgId(Long zgId) {
        this.zgId = zgId;
    }

    public void setZgId(String zgId) {
        this.zgId = parseLong(zgId);
    }

    public Integer getPropertyId() {
        return propertyId;
    }

    public void setPropertyId(Integer propertyId) {
        this.propertyId = propertyId;
    }

    public void setPropertyId(String propertyId) {
        this.propertyId = parseInt(propertyId);
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setUserId(String userId) {
        this.userId = parseLong(userId);
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(String propertyName) {
        this.propertyName = normalizeNull(propertyName);
    }

    public String getPropertyDataType() {
        return propertyDataType;
    }

    public void setPropertyDataType(String propertyDataType) {
        this.propertyDataType = normalizeNull(propertyDataType);
    }

    public String getPropertyValue() {
        return propertyValue;
    }

    public void setPropertyValue(String propertyValue) {
        this.propertyValue = normalizeNull(propertyValue);
    }

    public Integer getPlatform() {
        return platform;
    }

    public void setPlatform(Integer platform) {
        this.platform = platform;
    }

    public Long getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(Long lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }

    // ========== 业务方法 ==========

    /**
     * 获取表名
     */
    @JsonIgnore
    public String getTableName() {
        return "b_user_property_" + appId;
    }

    /**
     * 获取去重键
     */
    @JsonIgnore
    public String getDedupeKey() {
        return (zgId != null ? zgId.toString() : NULL_VALUE) + "_" +
                (propertyId != null ? propertyId.toString() : NULL_VALUE);
    }

    /**
     * 转换为 TSV 格式
     */
    @JsonIgnore
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");

        if (cdpMode) {
            // CDP 模式
            joiner.add(zgId != null ? zgId.toString() : NULL_VALUE);
            joiner.add(propertyId != null ? propertyId.toString() : NULL_VALUE);
            joiner.add(propertyValue != null ? propertyValue : NULL_VALUE);
            joiner.add(userId != null ? userId.toString() : NULL_VALUE);
            joiner.add(propertyName != null ? propertyName : NULL_VALUE);
            joiner.add(propertyDataType != null ? propertyDataType : NULL_VALUE);
            joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
            joiner.add(lastUpdateDate != null ? String.valueOf(lastUpdateDate) : NULL_VALUE);
        } else {
            // 普通模式
            joiner.add(zgId != null ? zgId.toString() : NULL_VALUE);
            joiner.add(propertyId != null ? propertyId.toString() : NULL_VALUE);
            joiner.add(userId != null ? userId.toString() : NULL_VALUE);
            joiner.add(propertyName != null ? propertyName : NULL_VALUE);
            joiner.add(propertyDataType != null ? propertyDataType : NULL_VALUE);
            joiner.add(propertyValue != null ? propertyValue : NULL_VALUE);
            joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
            joiner.add(lastUpdateDate != null ? String.valueOf(lastUpdateDate) : NULL_VALUE);
        }

        return joiner.toString();
    }

    @Override
    public String toString() {
        return "UserPropertyRow{" +
                "appId=" + appId +
                ", cdpMode=" + cdpMode +
                ", zgId=" + zgId +
                ", propertyId=" + propertyId +
                ", propertyName='" + propertyName + '\'' +
                ", propertyValue='" + propertyValue + '\'' +
                '}';
    }
}
