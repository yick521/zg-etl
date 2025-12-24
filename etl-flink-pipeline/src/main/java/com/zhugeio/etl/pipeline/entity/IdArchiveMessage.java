package com.zhugeio.etl.pipeline.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * ID 映射 Kafka 消息
 * 
 * 格式:
 * {
 *   "type": "DEVICE",
 *   "appId": 1001,
 *   "key": "abc123md5...",
 *   "value": "7890123456789",
 *   "ts": 1702900000000
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IdArchiveMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 映射类型: DEVICE, USER, DEVICE_ZGID, USER_ZGID, ZGID_USER */
    @JsonProperty("type")
    private String type;

    /** 应用ID */
    @JsonProperty("appId")
    private Integer appId;

    /** 映射源 Key */
    @JsonProperty("key")
    private String key;

    /** 映射目标 Value */
    @JsonProperty("value")
    private String value;

    /** 时间戳 (毫秒) */
    @JsonProperty("ts")
    private Long ts;

    // ========== 构造函数 ==========

    public IdArchiveMessage() {}

    public IdArchiveMessage(String type, Integer appId, String key, String value, Long ts) {
        this.type = type;
        this.appId = appId;
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    // ========== Getters & Setters ==========

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    // ========== 辅助方法 ==========

    public boolean isValid() {
        return type != null && appId != null && key != null && value != null;
    }

    public ArchiveType getArchiveType() {
        if (type == null) {
            return null;
        }
        try {
            return ArchiveType.valueOf(type);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Override
    public String toString() {
        return "IdMappingMessage{" +
                "type='" + type + '\'' +
                ", appId=" + appId +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", ts=" + ts +
                '}';
    }

    /**
     * 映射类型枚举
     */
    public enum ArchiveType {
        /** 设备MD5 → zgDeviceId */
        DEVICE,
        
        /** 用户标识 → zgUserId */
        USER,
        
        /** zgDeviceId → zgId */
        DEVICE_ZGID,
        
        /** zgUserId → zgId */
        USER_ZGID,
        
        /** zgId → zgUserId (反向映射) */
        ZGID_USER
    }
}
