package com.zhugeio.etl.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * 设备表行数据 (b_device_{appId})
 *
 * 表结构 (22列):
 * 0: device_id BIGINT - 设备ID
 * 1: device_md5 VARCHAR - 设备MD5
 * 2: platform SMALLINT - 平台
 * 3: device_type VARCHAR - 设备类型
 * 4: l INT - 水平像素
 * 5: h INT - 垂直像素
 * 6: device_brand VARCHAR - 设备品牌
 * 7: device_model VARCHAR - 设备型号
 * 8: resolution VARCHAR - 分辨率
 * 9: phone VARCHAR - 手机号
 * 10: imei VARCHAR - IMEI
 * 11: mac VARCHAR - MAC地址
 * 12: is_prison_break SMALLINT - 是否越狱
 * 13: is_crack SMALLINT - 是否破解
 * 14: language VARCHAR - 语言
 * 15: timezone VARCHAR - 时区
 * 16: attr1 VARCHAR - 扩展属性1
 * 17: attr2 VARCHAR - 扩展属性2
 * 18: attr3 VARCHAR - 扩展属性3
 * 19: attr4 VARCHAR - 扩展属性4
 * 20: attr5 VARCHAR - 扩展属性5
 * 21: last_update_date BIGINT - 最后更新时间 (秒)
 */
public class DeviceRow implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String NULL_VALUE = "\\N";

    @JsonIgnore
    private Integer appId;

    // 基础字段 - 类型匹配 Doris
    @JsonProperty("device_id")
    private Long deviceId;  // BIGINT

    @JsonProperty("device_md5")
    private String deviceMd5;  // VARCHAR

    @JsonProperty("platform")
    private Integer platform;  // SMALLINT

    @JsonProperty("device_type")
    private String deviceType;  // VARCHAR

    @JsonProperty("l")
    private Integer horizontalPixel;  // INT

    @JsonProperty("h")
    private Integer verticalPixel;  // INT

    @JsonProperty("device_brand")
    private String deviceBrand;  // VARCHAR

    @JsonProperty("device_model")
    private String deviceModel;  // VARCHAR

    @JsonProperty("resolution")
    private String resolution;  // VARCHAR

    @JsonProperty("phone")
    private String phone;  // VARCHAR

    @JsonProperty("imei")
    private String imei;  // VARCHAR

    @JsonProperty("mac")
    private String mac;  // VARCHAR

    @JsonProperty("is_prison_break")
    private Integer isPrisonBreak;  // SMALLINT

    @JsonProperty("is_crack")
    private Integer isCrack;  // SMALLINT

    @JsonProperty("language")
    private String language;  // VARCHAR

    @JsonProperty("timezone")
    private String timezone;  // VARCHAR

    // 扩展字段
    @JsonProperty("attr1")
    private String attr1;  // VARCHAR

    @JsonProperty("attr2")
    private String attr2;  // VARCHAR

    @JsonProperty("attr3")
    private String attr3;  // VARCHAR

    @JsonProperty("attr4")
    private String attr4;  // VARCHAR

    @JsonProperty("attr5")
    private String attr5;  // VARCHAR

    // 时间字段
    @JsonProperty("last_update_date")
    private Long lastUpdateDate;  // BIGINT

    public DeviceRow() {
    }

    public DeviceRow(Integer appId) {
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

    public Long getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Long deviceId) {
        this.deviceId = deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = parseLong(deviceId);
    }

    public String getDeviceMd5() {
        return deviceMd5;
    }

    public void setDeviceMd5(String deviceMd5) {
        this.deviceMd5 = normalizeNull(deviceMd5);
    }

    public Integer getPlatform() {
        return platform;
    }

    public void setPlatform(Integer platform) {
        this.platform = platform;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = normalizeNull(deviceType);
    }

    public Integer getHorizontalPixel() {
        return horizontalPixel;
    }

    public void setHorizontalPixel(Integer horizontalPixel) {
        this.horizontalPixel = horizontalPixel;
    }

    public void setHorizontalPixel(String horizontalPixel) {
        this.horizontalPixel = parseInt(horizontalPixel);
    }

    public Integer getVerticalPixel() {
        return verticalPixel;
    }

    public void setVerticalPixel(Integer verticalPixel) {
        this.verticalPixel = verticalPixel;
    }

    public void setVerticalPixel(String verticalPixel) {
        this.verticalPixel = parseInt(verticalPixel);
    }

    public String getDeviceBrand() {
        return deviceBrand;
    }

    public void setDeviceBrand(String deviceBrand) {
        this.deviceBrand = normalizeNull(deviceBrand);
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = normalizeNull(deviceModel);
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = normalizeNull(resolution);
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = normalizeNull(phone);
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = normalizeNull(imei);
    }

    public String getMac() {
        return mac;
    }

    public void setMac(String mac) {
        this.mac = normalizeNull(mac);
    }

    public Integer getIsPrisonBreak() {
        return isPrisonBreak;
    }

    public void setIsPrisonBreak(Integer isPrisonBreak) {
        this.isPrisonBreak = isPrisonBreak;
    }

    public void setIsPrisonBreak(String isPrisonBreak) {
        this.isPrisonBreak = parseInt(isPrisonBreak);
    }

    public Integer getIsCrack() {
        return isCrack;
    }

    public void setIsCrack(Integer isCrack) {
        this.isCrack = isCrack;
    }

    public void setIsCrack(String isCrack) {
        this.isCrack = parseInt(isCrack);
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = normalizeNull(language);
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = normalizeNull(timezone);
    }

    public String getAttr1() {
        return attr1;
    }

    public void setAttr1(String attr1) {
        this.attr1 = normalizeNull(attr1);
    }

    public String getAttr2() {
        return attr2;
    }

    public void setAttr2(String attr2) {
        this.attr2 = normalizeNull(attr2);
    }

    public String getAttr3() {
        return attr3;
    }

    public void setAttr3(String attr3) {
        this.attr3 = normalizeNull(attr3);
    }

    public String getAttr4() {
        return attr4;
    }

    public void setAttr4(String attr4) {
        this.attr4 = normalizeNull(attr4);
    }

    public String getAttr5() {
        return attr5;
    }

    public void setAttr5(String attr5) {
        this.attr5 = normalizeNull(attr5);
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
        return "b_device_" + appId;
    }

    /**
     * 获取去重键
     */
    @JsonIgnore
    public String getDedupeKey() {
        return deviceId != null ? deviceId.toString() : NULL_VALUE;
    }

    /**
     * 判断 device_model 是否为空
     */
    @JsonIgnore
    public boolean isDeviceModelEmpty() {
        return "zhuge.io".equals(deviceModel) || deviceModel == null;
    }

    /**
     * 转换为 TSV 格式
     */
    @JsonIgnore
    public String toTsv() {
        StringJoiner joiner = new StringJoiner("\t");
        joiner.add(deviceId != null ? deviceId.toString() : NULL_VALUE);
        joiner.add(deviceMd5 != null ? deviceMd5 : NULL_VALUE);
        joiner.add(platform != null ? String.valueOf(platform) : NULL_VALUE);
        joiner.add(deviceType != null ? deviceType : NULL_VALUE);
        joiner.add(horizontalPixel != null ? horizontalPixel.toString() : NULL_VALUE);
        joiner.add(verticalPixel != null ? verticalPixel.toString() : NULL_VALUE);
        joiner.add(deviceBrand != null ? deviceBrand : NULL_VALUE);
        joiner.add(deviceModel != null ? deviceModel : NULL_VALUE);
        joiner.add(resolution != null ? resolution : NULL_VALUE);
        joiner.add(phone != null ? phone : NULL_VALUE);
        joiner.add(imei != null ? imei : NULL_VALUE);
        joiner.add(mac != null ? mac : NULL_VALUE);
        joiner.add(isPrisonBreak != null ? isPrisonBreak.toString() : NULL_VALUE);
        joiner.add(isCrack != null ? isCrack.toString() : NULL_VALUE);
        joiner.add(language != null ? language : NULL_VALUE);
        joiner.add(timezone != null ? timezone : NULL_VALUE);
        joiner.add(attr1 != null ? attr1 : NULL_VALUE);
        joiner.add(attr2 != null ? attr2 : NULL_VALUE);
        joiner.add(attr3 != null ? attr3 : NULL_VALUE);
        joiner.add(attr4 != null ? attr4 : NULL_VALUE);
        joiner.add(attr5 != null ? attr5 : NULL_VALUE);
        joiner.add(lastUpdateDate != null ? String.valueOf(lastUpdateDate) : NULL_VALUE);
        return joiner.toString();
    }

    @Override
    public String toString() {
        return "DeviceRow{" +
                "appId=" + appId +
                ", deviceId=" + deviceId +
                ", deviceBrand='" + deviceBrand + '\'' +
                ", deviceModel='" + deviceModel + '\'' +
                '}';
    }
}
