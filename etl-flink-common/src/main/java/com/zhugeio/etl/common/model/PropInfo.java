package com.zhugeio.etl.common.model;

import java.io.Serializable;
import java.util.Map;

/**
 * 用户属性信息
 */
public class PropInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Integer appId;
    private final String owner;
    private final String zgZgid;        //用户 ID
    private final String propName;
    private final int propType;
    private final Map<String, Object> pr;
    private final String originalKey;

    public PropInfo(Integer appId, String owner, String zgZgid, String propName, int propType,
                    Map<String, Object> pr, String originalKey) {
        this.appId = appId;
        this.owner = owner;
        this.zgZgid = zgZgid;
        this.propName = propName;
        this.propType = propType;
        this.pr = pr;
        this.originalKey = originalKey;
    }

    public Integer getAppId() {
        return appId;
    }

    public String getOwner() {
        return owner;
    }

    public String getZgZgid() {
        return zgZgid;
    }

    public String getPropName() {
        return propName;
    }

    public int getPropType() {
        return propType;
    }

    public Map<String, Object> getPr() {
        return pr;
    }

    public String getOriginalKey() {
        return originalKey;
    }

    /**
     * 生成唯一标识 key
     * 格式: appId_owner_propName
     */
    public String getKey() {
        return appId + "_" + owner + "_" + propName;
    }

    @Override
    public String toString() {
        return "PropInfo{" +
                "appId=" + appId +
                ", owner='" + owner + '\'' +
                ", zgZgid='" + zgZgid + '\'' +
                ", propName='" + propName + '\'' +
                ", propType=" + propType +
                '}';
    }
}