package com.zhugeio.etl.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 用户映射表行数据 (b_user_{appId})
 *
 * 表结构 (5列):
 * - device_id: BIGINT 设备ID
 * - zg_id: BIGINT 诸葛ID
 * - user_id: BIGINT 注册用户ID
 * - begin_date: BIGINT 开始时间戳 (秒)
 * - platform: SMALLINT 平台
 */
public class UserRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String NULL_VALUE = "\\N";

    @JsonIgnore
    private Integer appId;

    @JsonProperty("device_id")
    private Long deviceId;  // 改为 Long 匹配 Doris BIGINT

    @JsonProperty("zg_id")
    private Long zgId;  // 改为 Long 匹配 Doris BIGINT

    @JsonProperty("user_id")
    private Long userId;  // 改为 Long 匹配 Doris BIGINT

    @JsonProperty("begin_date")
    private Long beginDate;

    @JsonProperty("platform")
    private Integer platform;

    public UserRow() {
    }

    public UserRow(Integer appId) {
        this.appId = appId;
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

    // ========== Getters and Setters ==========

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Long deviceId) {
        this.deviceId = deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = parseLong(deviceId);
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

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setUserId(String userId) {
        this.userId = parseLong(userId);
    }

    public Long getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Long beginDate) {
        this.beginDate = beginDate;
    }

    public Integer getPlatform() {
        return platform;
    }

    public void setPlatform(Integer platform) {
        this.platform = platform;
    }

    // ========== 业务方法 ==========

    /**
     * 获取表名
     */
    @JsonIgnore
    public String getTableName() {
        return "b_user_" + appId;
    }

    /**
     * 获取去重键
     * 格式: device_id + zg_id
     */
    @JsonIgnore
    public String getDedupeKey() {
        return (deviceId != null ? deviceId.toString() : NULL_VALUE) + "_" +
                (zgId != null ? zgId.toString() : NULL_VALUE);
    }

    /**
     * 转换为 TSV 格式
     */
    @JsonIgnore
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        joiner.add(deviceId != null ? deviceId.toString() : NULL_VALUE);
        joiner.add(zgId != null ? zgId.toString() : NULL_VALUE);
        joiner.add(userId != null ? userId.toString() : NULL_VALUE);
        joiner.add(beginDate != null ? String.valueOf(beginDate) : NULL_VALUE);
        joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
        return joiner.toString();
    }

    @Override
    public String toString() {
        return "UserRow{" +
                "appId=" + appId +
                ", deviceId=" + deviceId +
                ", zgId=" + zgId +
                ", userId=" + userId +
                '}';
    }
}
