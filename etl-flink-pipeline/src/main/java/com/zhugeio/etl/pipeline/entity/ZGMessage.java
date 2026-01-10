package com.zhugeio.etl.pipeline.entity;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author ningjh
 * @name com.zhugeio.etl.pipeline.example.ZGMessage
 * @date 2025/12/1
 * @description 消息体，从kafka中获取
 */
@Data
public class ZGMessage implements Serializable {

    // 必需字段
    private String topic;
    private Integer partition;
    private Long offset;
    private String key;
    private String rawData;
    
    private Integer result = 0;
    private Integer appId = 0;
    private String appKey = "";
    private Integer sdk = 0;
    private Map<String, Object> data;
    private Map<String, Object> errData;
    private String json = "";
    private String error = "";
    private Integer errorCode = 0;
    private String errorDescribe = "";
    private String business = "";
    private Integer zgEid = 0;

    /**
     * 广告输出kafka信息
     */
    private List<String> advKafkaMsgList;

    /**
     * 广告输出kafka信息 - ad_user
     */
    private List<String> advUserKafkaMsgList;

    public ZGMessage() {
    }

    public ZGMessage(String topic, Integer partition, Long offset, String key, String rawData, Integer result, Integer appId, String appKey, Integer sdk, Map<String, Object> data, Map<String, Object> errData, String json, String error, Integer errorCode, String errorDescribe, String business) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.rawData = rawData;
        this.result = result;
        this.appId = appId;
        this.appKey = appKey;
        this.sdk = sdk;
        this.data = data;
        this.errData = errData;
        this.json = json;
        this.error = error;
        this.errorCode = errorCode;
        this.errorDescribe = errorDescribe;
        this.business = business;
    }
}