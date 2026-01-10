package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 用户ID→诸葛ID映射表 Row
 * 表: dwd_id_user_zgid
 * 映射: zgUserId → zgId
 */
public class UserZgidRow implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("app_id")
    private Integer appId;

    @JsonProperty("zg_user_id")
    private Long zgUserId;

    @JsonProperty("zg_id")
    private Long zgId;

    public UserZgidRow() {}

    public UserZgidRow(Integer appId, Long zgUserId, Long zgId) {
        this.appId = appId;
        this.zgUserId = zgUserId;
        this.zgId = zgId;
    }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public Long getZgUserId() { return zgUserId; }
    public void setZgUserId(Long zgUserId) { this.zgUserId = zgUserId; }

    public Long getZgId() { return zgId; }
    public void setZgId(Long zgId) { this.zgId = zgId; }
}
