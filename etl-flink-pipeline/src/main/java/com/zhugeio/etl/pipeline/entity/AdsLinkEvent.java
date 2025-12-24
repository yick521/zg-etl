package com.zhugeio.etl.pipeline.entity;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import java.io.Serializable;

/**
 * 广告链接事件实体类（使用Lombok）
 */
public class AdsLinkEvent implements Serializable {
    
    /** 链接ID */
    private Integer linkId = 0;
    
    /** 事件ID */
    private Integer eventId = 0;
    
    /** 渠道事件 */
    private String channelEvent = "";
    
    /** 匹配JSON */
    private String matchJson = "";
    
    /** 频次（0:首次，1:每次） */
    private Integer frequency = 0;
    
    /** 事件ID列表（逗号分隔） */
    private String eventIds = "";
    
    /** 窗口时间（秒） */
    private Long windowTime = 0L;

    public AdsLinkEvent() {
    }

    public AdsLinkEvent(Integer linkId, Integer eventId, String channelEvent, String matchJson, Integer frequency, String eventIds, Long windowTime) {
        this.linkId = linkId;
        this.eventId = eventId;
        this.channelEvent = channelEvent;
        this.matchJson = matchJson;
        this.frequency = frequency;
        this.eventIds = eventIds;
        this.windowTime = windowTime;
    }

    public Integer getLinkId() {
        return linkId;
    }

    public void setLinkId(Integer linkId) {
        this.linkId = linkId;
    }

    public Integer getEventId() {
        return eventId;
    }

    public void setEventId(Integer eventId) {
        this.eventId = eventId;
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

    public String getEventIds() {
        return eventIds;
    }

    public void setEventIds(String eventIds) {
        this.eventIds = eventIds;
    }

    public Long getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(Long windowTime) {
        this.windowTime = windowTime;
    }

    public String toJsonString() {
        return "{\"link_id\":\"" + linkId +
                "\",\"event_id\":" + eventId +
                ",\"channel_event\":\"" + channelEvent +
                "\",\"match_json\":" + matchJson +
                ",\"frequency\":\"" + frequency +
                "\"}";
    }

    /**
     * 从JSON字符串创建对象
     * @param jsonStr JSON字符串
     * @return AdsLinkEventLombok对象
     */
    public static AdsLinkEvent fromJson(String jsonStr) {
        try {
            JSONObject json = JSON.parseObject(jsonStr);
            AdsLinkEvent event = new AdsLinkEvent();
            event.setLinkId(json.getInteger("link_id"));
            event.setEventId(json.getInteger("event_id"));
            event.setChannelEvent(json.getString("channel_event"));
            event.setMatchJson(json.getString("match_json"));
            event.setFrequency(json.getInteger("frequency"));
            event.setEventIds(json.getString("event_ids"));
            event.setWindowTime(json.getLong("window_time"));
            return event;
        } catch (Exception e) {
            return null;
        }
    }
}