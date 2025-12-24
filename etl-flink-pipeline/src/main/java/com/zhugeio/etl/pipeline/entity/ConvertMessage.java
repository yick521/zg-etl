package com.zhugeio.etl.pipeline.entity;

import java.io.Serializable;

/**
 * 回传事件记录信息 写入表 toufang_convert_event
 */
public class ConvertMessage implements Serializable {

    private long zgId = 0; // 用户id
    private long zgEid = 0; // 事件id
    private String eventName = ""; // 事件
    private long lid = 0; // 链接id
    private long channelId = 0; // 渠道id
    private Integer zgAppid = 0; // 应用id
    private long channelAdgroupId = 0; // 广告计划id
    private String channelAdgroupName = ""; // 广告计划名称
    private long clickTime = 0; // 广告点击事件的时间
    private long eventTime = 0; // 事件时间
    private String channelEvent = ""; // 渠道事件类型
    private String matchJson = ""; // 匹配条件
    private Integer frequency = 0; // 频次
    private String utmCampaign = ""; // 活动名称
    private String utmSource = ""; // 广告来源
    private String utmMedium = ""; // 广告媒介
    private String utmTerm = ""; // 关键词
    private String utmContent = ""; // 广告内容
    private String uuid = "";

    public void compileUtm(Utm utm) {
        this.utmCampaign = utm.getUtmCampaign();
        this.utmSource = utm.getUtmSource();
        this.utmMedium = utm.getUtmMedium();
        this.utmTerm = utm.getUtmTerm();
        this.utmContent = utm.getUtmContent();
    }

    public String toConvertJson() {
        return "{\"zg_id\":\"" + zgId +
                "\",\"zg_eid\":\"" + zgEid +
                "\",\"event_name\":\"" + eventName +
                "\",\"lid\":\"" + lid +
                "\",\"channel_id\":\"" + channelId +
                "\",\"zg_appid\":\"" + zgAppid +
                "\",\"channel_adgroup_id\":\"" + channelAdgroupId +
                "\",\"channel_adgroup_name\":\"" + channelAdgroupName +
                "\",\"click_time\":\"" + clickTime +
                "\",\"event_time\":\"" + eventTime +
                "\",\"channel_event\":\"" + channelEvent +
                "\",\"match_json\":\"" + matchJson +
                "\",\"frequency\":\"" + frequency +
                "\",\"utm_campaign\":\"" + utmCampaign +
                "\",\"utm_source\":\"" + utmSource +
                "\",\"utm_medium\":\"" + utmMedium +
                "\",\"utm_term\":\"" + utmTerm +
                "\",\"utm_content\":\"" + utmContent +
                "\",\"uuid\":\"" + uuid + "\"}";
    }

    // Getter 和 Setter 方法
    public long getZgId() {
        return zgId;
    }

    public void setZgId(long zgId) {
        this.zgId = zgId;
    }

    public long getZgEid() {
        return zgEid;
    }

    public void setZgEid(long zgEid) {
        this.zgEid = zgEid;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public long getLid() {
        return lid;
    }

    public void setLid(long lid) {
        this.lid = lid;
    }

    public long getChannelId() {
        return channelId;
    }

    public void setChannelId(long channelId) {
        this.channelId = channelId;
    }

    public Integer getZgAppid() {
        return zgAppid;
    }

    public void setZgAppid(Integer zgAppid) {
        this.zgAppid = zgAppid;
    }

    public long getChannelAdgroupId() {
        return channelAdgroupId;
    }

    public void setChannelAdgroupId(long channelAdgroupId) {
        this.channelAdgroupId = channelAdgroupId;
    }

    public String getChannelAdgroupName() {
        return channelAdgroupName;
    }

    public void setChannelAdgroupName(String channelAdgroupName) {
        this.channelAdgroupName = channelAdgroupName;
    }

    public long getClickTime() {
        return clickTime;
    }

    public void setClickTime(long clickTime) {
        this.clickTime = clickTime;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getChannelEvent() {
        return channelEvent;
    }

    public void setChannelEvent(String channelEvent) {
        this.channelEvent = channelEvent;
    }

    public String getMatchJson() {
        return matchJson;
    }

    public void setMatchJson(String matchJson) {
        this.matchJson = matchJson;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }

    public String getUtmCampaign() {
        return utmCampaign;
    }

    public void setUtmCampaign(String utmCampaign) {
        this.utmCampaign = utmCampaign;
    }

    public String getUtmSource() {
        return utmSource;
    }

    public void setUtmSource(String utmSource) {
        this.utmSource = utmSource;
    }

    public String getUtmMedium() {
        return utmMedium;
    }

    public void setUtmMedium(String utmMedium) {
        this.utmMedium = utmMedium;
    }

    public String getUtmTerm() {
        return utmTerm;
    }

    public void setUtmTerm(String utmTerm) {
        this.utmTerm = utmTerm;
    }

    public String getUtmContent() {
        return utmContent;
    }

    public void setUtmContent(String utmContent) {
        this.utmContent = utmContent;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}