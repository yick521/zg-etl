package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 用户映射表 Row
 * 表: dwd_id_user
 * 映射: cuid → zgUserId
 */
public class UserIdRow implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("app_id")
    private Integer appId;

    @JsonProperty("cuid")
    private String cuid;

    @JsonProperty("zg_user_id")
    private Long zgUserId;

    public UserIdRow() {}

    public UserIdRow(Integer appId, String cuid, Long zgUserId) {
        this.appId = appId;
        this.cuid = cuid;
        this.zgUserId = zgUserId;
    }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public String getCuid() { return cuid; }
    public void setCuid(String cuid) { this.cuid = cuid; }

    public Long getZgUserId() { return zgUserId; }
    public void setZgUserId(Long zgUserId) { this.zgUserId = zgUserId; }
}
