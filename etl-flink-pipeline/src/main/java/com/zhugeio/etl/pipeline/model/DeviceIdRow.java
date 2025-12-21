package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 设备映射表 Row
 * 表: dwd_id_device
 * 映射: deviceMd5 → zgDeviceId
 */
public class DeviceIdRow implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("app_id")
    private Integer appId;

    @JsonProperty("device_md5")
    private String deviceMd5;

    @JsonProperty("zg_device_id")
    private Long zgDeviceId;

    public DeviceIdRow() {}

    public DeviceIdRow(Integer appId, String deviceMd5, Long zgDeviceId) {
        this.appId = appId;
        this.deviceMd5 = deviceMd5;
        this.zgDeviceId = zgDeviceId;
    }

    public Integer getAppId() { return appId; }
    public void setAppId(Integer appId) { this.appId = appId; }

    public String getDeviceMd5() { return deviceMd5; }
    public void setDeviceMd5(String deviceMd5) { this.deviceMd5 = deviceMd5; }

    public Long getZgDeviceId() { return zgDeviceId; }
    public void setZgDeviceId(Long zgDeviceId) { this.zgDeviceId = zgDeviceId; }
}
