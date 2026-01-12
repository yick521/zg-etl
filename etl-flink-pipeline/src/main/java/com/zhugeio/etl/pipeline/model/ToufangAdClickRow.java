package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 投放广告点击表 Row
 * 表: toufang_ad_click_a
 * 对应原 Kudu 表字段
 */
public class ToufangAdClickRow implements Serializable {
    private static final long serialVersionUID = 1L;

    // 主键字段
    @JsonProperty("zg_appid")
    private String zgAppid;

    @JsonProperty("click_time")
    private String clickTime;

    @JsonProperty("key_ip_data")
    private String keyIpData;

    @JsonProperty("key_muid_data")
    private String keyMuidData;

    @JsonProperty("zg_id")
    private String zgId;

    @JsonProperty("key_ad_data")
    private String keyAdData;

    // 普通字段
    @JsonProperty("other_key")
    private String otherKey;

    @JsonProperty("other_value")
    private String otherValue;

    @JsonProperty("key_ip_time")
    private String keyIpTime;

    @JsonProperty("key_muid_time")
    private String keyMuidTime;

    @JsonProperty("ad_data")
    private String adData;

    @JsonProperty("is_delete")
    private String isDelete;

    @JsonProperty("key_ad_time")
    private String keyAdTime;

    @JsonProperty("value_ad_time")
    private String valueAdTime;

    @JsonProperty("value_ad_data")
    private String valueAdData;

    @JsonProperty("key_ad_lid")
    private String keyAdLid;

    @JsonProperty("value_ad_lid")
    private String valueAdLid;

    @JsonProperty("key_type")
    private String keyType;

    @JsonProperty("lid")
    private String lid;

    @JsonProperty("update_time")
    private Long updateTime;

    // 无参构造函数
    public ToufangAdClickRow() {}

    // 全参构造函数
    public ToufangAdClickRow(String zgAppid, String clickTime, String keyIpData, String keyMuidData,
                             String zgId, String keyAdData, String otherKey, String otherValue,
                             String keyIpTime, String keyMuidTime, String adData, String isDelete,
                             String keyAdTime, String valueAdTime, String valueAdData, String keyAdLid,
                             String valueAdLid, String keyType, String lid, Long updateTime) {
        this.zgAppid = zgAppid;
        this.clickTime = clickTime;
        this.keyIpData = keyIpData;
        this.keyMuidData = keyMuidData;
        this.zgId = zgId;
        this.keyAdData = keyAdData;
        this.otherKey = otherKey;
        this.otherValue = otherValue;
        this.keyIpTime = keyIpTime;
        this.keyMuidTime = keyMuidTime;
        this.adData = adData;
        this.isDelete = isDelete;
        this.keyAdTime = keyAdTime;
        this.valueAdTime = valueAdTime;
        this.valueAdData = valueAdData;
        this.keyAdLid = keyAdLid;
        this.valueAdLid = valueAdLid;
        this.keyType = keyType;
        this.lid = lid;
        this.updateTime = updateTime;
    }

    // Getter 和 Setter 方法
    public String getZgAppid() { return zgAppid; }
    public void setZgAppid(String zgAppid) { this.zgAppid = zgAppid; }

    public String getClickTime() { return clickTime; }
    public void setClickTime(String clickTime) { this.clickTime = clickTime; }

    public String getKeyIpData() { return keyIpData; }
    public void setKeyIpData(String keyIpData) { this.keyIpData = keyIpData; }

    public String getKeyMuidData() { return keyMuidData; }
    public void setKeyMuidData(String keyMuidData) { this.keyMuidData = keyMuidData; }

    public String getZgId() { return zgId; }
    public void setZgId(String zgId) { this.zgId = zgId; }

    public String getKeyAdData() { return keyAdData; }
    public void setKeyAdData(String keyAdData) { this.keyAdData = keyAdData; }

    public String getOtherKey() { return otherKey; }
    public void setOtherKey(String otherKey) { this.otherKey = otherKey; }

    public String getOtherValue() { return otherValue; }
    public void setOtherValue(String otherValue) { this.otherValue = otherValue; }

    public String getKeyIpTime() { return keyIpTime; }
    public void setKeyIpTime(String keyIpTime) { this.keyIpTime = keyIpTime; }

    public String getKeyMuidTime() { return keyMuidTime; }
    public void setKeyMuidTime(String keyMuidTime) { this.keyMuidTime = keyMuidTime; }

    public String getAdData() { return adData; }
    public void setAdData(String adData) { this.adData = adData; }

    public String getIsDelete() { return isDelete; }
    public void setIsDelete(String isDelete) { this.isDelete = isDelete; }

    public String getKeyAdTime() { return keyAdTime; }
    public void setKeyAdTime(String keyAdTime) { this.keyAdTime = keyAdTime; }

    public String getValueAdTime() { return valueAdTime; }
    public void setValueAdTime(String valueAdTime) { this.valueAdTime = valueAdTime; }

    public String getValueAdData() { return valueAdData; }
    public void setValueAdData(String valueAdData) { this.valueAdData = valueAdData; }

    public String getKeyAdLid() { return keyAdLid; }
    public void setKeyAdLid(String keyAdLid) { this.keyAdLid = keyAdLid; }

    public String getValueAdLid() { return valueAdLid; }
    public void setValueAdLid(String valueAdLid) { this.valueAdLid = valueAdLid; }

    public String getKeyType() { return keyType; }
    public void setKeyType(String keyType) { this.keyType = keyType; }

    public String getLid() { return lid; }
    public void setLid(String lid) { this.lid = lid; }

    public Long getUpdateTime() { return updateTime; }
    public void setUpdateTime(Long updateTime) { this.updateTime = updateTime; }
}