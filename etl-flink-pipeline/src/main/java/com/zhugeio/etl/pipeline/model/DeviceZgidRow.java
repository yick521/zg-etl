package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 设备ID→诸葛ID映射表 Row
 * 表: dwd_id_device_zgid
 * 映射: zgDeviceId → zgId
 */
public class DeviceZgidRow implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("app_id")
    private Integer appId;

    @JsonProperty("zg_device_id")
    private Long zgDeviceId;

    @JsonProperty("zg_id")
    private Long zgId;

    public DeviceZgidRow() {}

    public DeviceZgidRow(Integer appId, Long zgDeviceId, Long zgId) {
        this.appId = appId;
        this.zgDeviceId = zgDeviceId;
        this.zgId = zgId;
    }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public Long getZgDeviceId() { return zgDeviceId; }
    public void setZgDeviceId(Long zgDeviceId) { this.zgDeviceId = zgDeviceId; }

    public Long getZgId() { return zgId; }
    public void setZgId(Long zgId) { this.zgId = zgId; }
}
