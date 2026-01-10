package com.zhugeio.etl.common.model;

import java.util.Map;

import java.io.Serializable;

/**
 * 属性信息类
 * 用于在批量处理过程中传递属性相关信息
 */
public class AttrInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer appId;          // appID
    private Integer eventId;        // eventID
    private String owner;           // 属性所有者
    private String attrName;        // 属性名称（不含下划线）
    private String realAttrName;    // 实际属性名称
    private String attrType;        // 属性类型（int/float/string）
    private transient Map<String, Object> pr; // 属性所在的pr对象引用（不需要序列化）
    private String originalKey;     // 原始key（含下划线）

    public AttrInfo(Integer appId, Integer eventId, String owner, String attrName,
                    String realAttrName, String attrType, Map<String, Object> pr, String originalKey) {
        this.appId = appId;
        this.eventId = eventId;
        this.owner = owner;
        this.attrName = attrName;
        this.realAttrName = realAttrName;
        this.attrType = attrType;
        this.pr = pr; // transient字段，用于在处理过程中引用原始数据
        this.originalKey = originalKey;
    }

    /**
     * 生成唯一key用于结果映射
     * 格式: eventId:owner:attrName
     */
    public String getKey() {
        return eventId + ":" + owner + ":" + attrName;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public Integer getEventId() {
        return eventId;
    }

    public void setEventId(Integer eventId) {
        this.eventId = eventId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getRealAttrName() {
        return realAttrName;
    }

    public void setRealAttrName(String realAttrName) {
        this.realAttrName = realAttrName;
    }

    public String getAttrType() {
        return attrType;
    }

    public void setAttrType(String attrType) {
        this.attrType = attrType;
    }

    /**
     * 获取pr对象引用
     * 注意：此字段为transient，不会被序列化
     * 仅用于本地处理过程中更新数据
     */
    public Map<String, Object> getPr() {
        return pr;
    }

    public void setPr(Map<String, Object> pr) {
        this.pr = pr;
    }

    public String getOriginalKey() {
        return originalKey;
    }

    public void setOriginalKey(String originalKey) {
        this.originalKey = originalKey;
    }
}
