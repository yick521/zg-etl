package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 诸葛ID→用户ID映射表 Row
 * 表: dwd_id_zgid_user
 * 映射: zgId → zgUserId (反向映射)
 */
public class ZgidUserRow implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("app_id")
    private Integer appId;

    @JsonProperty("zg_id")
    private Long zgId;

    @JsonProperty("zg_user_id")
    private Long zgUserId;

    public ZgidUserRow() {}

    public ZgidUserRow(Integer appId, Long zgId, Long zgUserId) {
        this.appId = appId;
        this.zgId = zgId;
        this.zgUserId = zgUserId;
    }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public Long getZgId() { return zgId; }
    public void setZgId(Long zgId) { this.zgId = zgId; }

    public Long getZgUserId() { return zgUserId; }
    public void setZgUserId(Long zgUserId) { this.zgUserId = zgUserId; }
}
