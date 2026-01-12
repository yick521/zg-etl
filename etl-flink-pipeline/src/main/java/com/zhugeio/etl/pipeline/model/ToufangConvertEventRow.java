package com.zhugeio.etl.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 投放转化事件表 Row
 * 表: toufang_convert_event_a
 * 对应原 Kudu 表字段
 */
public class ToufangConvertEventRow implements Serializable {
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("zg_appid")
    private Integer zgAppid;
    
    @JsonProperty("zg_id")
    private Integer zgId;
    
    @JsonProperty("lid")
    private Integer lid;
    
    @JsonProperty("channel_id")
    private Integer channelId;
    
    @JsonProperty("zg_eid")
    private Integer zgEid;
    
    @JsonProperty("event_time")
    private Long eventTime;
    
    @JsonProperty("channel_adgroup_id")
    private String channelAdgroupId;
    
    @JsonProperty("channel_adgroup_name")
    private String channelAdgroupName;
    
    @JsonProperty("click_time")
    private Long clickTime;
    
    @JsonProperty("event_name")
    private String eventName;
    
    @JsonProperty("channel_event")
    private String channelEvent;
    
    @JsonProperty("match_json")
    private String matchJson;
    
    @JsonProperty("frequency")
    private Integer frequency;
    
    @JsonProperty("utm_campaign")
    private String utmCampaign;
    
    @JsonProperty("utm_source")
    private String utmSource;
    
    @JsonProperty("utm_medium")
    private String utmMedium;
    
    @JsonProperty("utm_term")
    private String utmTerm;
    
    @JsonProperty("utm_content")
    private String utmContent;

    // 无参构造函数（JSON反序列化需要）
    public ToufangConvertEventRow() {}

    // 全参构造函数
    public ToufangConvertEventRow(Integer zgAppid, Integer zgId, Integer lid, 
                                  Integer channelId, Integer zgEid, Long eventTime,
                                  String channelAdgroupId, String channelAdgroupName,
                                  Long clickTime, String eventName, String channelEvent,
                                  String matchJson, Integer frequency, String utmCampaign,
                                  String utmSource, String utmMedium, String utmTerm,
                                  String utmContent) {
        this.zgAppid = zgAppid;
        this.zgId = zgId;
        this.lid = lid;
        this.channelId = channelId;
        this.zgEid = zgEid;
        this.eventTime = eventTime;
        this.channelAdgroupId = channelAdgroupId;
        this.channelAdgroupName = channelAdgroupName;
        this.clickTime = clickTime;
        this.eventName = eventName;
        this.channelEvent = channelEvent;
        this.matchJson = matchJson;
        this.frequency = frequency;
        this.utmCampaign = utmCampaign;
        this.utmSource = utmSource;
        this.utmMedium = utmMedium;
        this.utmTerm = utmTerm;
        this.utmContent = utmContent;
    }

    // Getter 和 Setter 方法
    public Integer getZgAppid() { return zgAppid; }
    public void setZgAppid(Integer zgAppid) { this.zgAppid = zgAppid; }
    
    public Integer getZgId() { return zgId; }
    public void setZgId(Integer zgId) { this.zgId = zgId; }
    
    public Integer getLid() { return lid; }
    public void setLid(Integer lid) { this.lid = lid; }
    
    public Integer getChannelId() { return channelId; }
    public void setChannelId(Integer channelId) { this.channelId = channelId; }
    
    public Integer getZgEid() { return zgEid; }
    public void setZgEid(Integer zgEid) { this.zgEid = zgEid; }
    
    public Long getEventTime() { return eventTime; }
    public void setEventTime(Long eventTime) { this.eventTime = eventTime; }
    
    public String getChannelAdgroupId() { return channelAdgroupId; }
    public void setChannelAdgroupId(String channelAdgroupId) { this.channelAdgroupId = channelAdgroupId; }
    
    public String getChannelAdgroupName() { return channelAdgroupName; }
    public void setChannelAdgroupName(String channelAdgroupName) { this.channelAdgroupName = channelAdgroupName; }
    
    public Long getClickTime() { return clickTime; }
    public void setClickTime(Long clickTime) { this.clickTime = clickTime; }
    
    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }
    
    public String getChannelEvent() { return channelEvent; }
    public void setChannelEvent(String channelEvent) { this.channelEvent = channelEvent; }
    
    public String getMatchJson() { return matchJson; }
    public void setMatchJson(String matchJson) { this.matchJson = matchJson; }
    
    public Integer getFrequency() { return frequency; }
    public void setFrequency(Integer frequency) { this.frequency = frequency; }
    
    public String getUtmCampaign() { return utmCampaign; }
    public void setUtmCampaign(String utmCampaign) { this.utmCampaign = utmCampaign; }
    
    public String getUtmSource() { return utmSource; }
    public void setUtmSource(String utmSource) { this.utmSource = utmSource; }
    
    public String getUtmMedium() { return utmMedium; }
    public void setUtmMedium(String utmMedium) { this.utmMedium = utmMedium; }
    
    public String getUtmTerm() { return utmTerm; }
    public void setUtmTerm(String utmTerm) { this.utmTerm = utmTerm; }
    
    public String getUtmContent() { return utmContent; }
    public void setUtmContent(String utmContent) { this.utmContent = utmContent; }
    
    @Override
    public String toString() {
        return "ToufangConvertEventRow{" +
                "zgAppid=" + zgAppid +
                ", zgId=" + zgId +
                ", lid=" + lid +
                ", channelId=" + channelId +
                ", zgEid=" + zgEid +
                ", eventTime=" + eventTime +
                ", channelAdgroupId='" + channelAdgroupId + '\'' +
                ", channelAdgroupName='" + channelAdgroupName + '\'' +
                ", clickTime=" + clickTime +
                ", eventName='" + eventName + '\'' +
                ", channelEvent='" + channelEvent + '\'' +
                ", matchJson='" + matchJson + '\'' +
                ", frequency=" + frequency +
                ", utmCampaign='" + utmCampaign + '\'' +
                ", utmSource='" + utmSource + '\'' +
                ", utmMedium='" + utmMedium + '\'' +
                ", utmTerm='" + utmTerm + '\'' +
                ", utmContent='" + utmContent + '\'' +
                '}';
    }
}