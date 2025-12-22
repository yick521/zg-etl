package com.zhugeio.etl.pipeline.entity;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

/**
 * 回传行为数据，发至kafka，由后端回传给广告平台
 */
public class ConvertMessageV2 implements Serializable {
    
    @JSONField
    private long zgId = 0; // 用户id
    
    @JSONField
    private String callback = "";
    
    @JSONField(name = "os")
    private Integer ados = 0; // 操作系统平台 os
    
    @JSONField
    private String muid = "";
    
    @JSONField
    private long lid = 0; // 链接id
    
    @JSONField
    private String callbackUrl = ""; // 回调链接
    
    @JSONField
    private String imei = ""; // 安卓设备ID md5

    // 从缓存获取 select link_id,channel_event from ads_link_event where is_delete = 0
    @JSONField
    private String eventType = ""; // 渠道事件
    
    @JSONField
    private String idfa = ""; // iOS 6+的设备ID
    
    // TODO 注意：该字段并未赋值
    @JSONField
    private String plat = ""; // 设备类型 ANDROID("1"), IOS("2")

    // 投放五期新增字段
    @JSONField
    private String channelType = "";
    
    @JSONField
    private String pushType = "";
    
    @JSONField
    private long clickTime = 0L;
    
    @JSONField
    private String akey = "";
    
    @JSONField
    private String token = "";
    
    @JSONField
    private String company = "";
    
    @JSONField
    private String authAccount = "";
    
    @JSONField
    private long zgAppid = 0L;
    
    @JSONField
    private String hwAppId = ""; // 华为商城应用id
    
    @JSONField
    private String actionTime = ""; // 华为广告深度回传用 （取回传事件的事件触发时间）
    
    @JSONField
    private String oaid = ""; // 安卓Q及更高版本的设备号
    
    @JSONField
    private String creativeId = ""; // vivo监测回传必备字段-创意id
    
    @JSONField
    private String channelClickId = ""; // 回传必备字段-clickId
    
    @JSONField
    private String channelAccountId = ""; // 回传必备字段-账号id

    public String toJsonString() {
        return "{\"callback\":\"" + callback +
                "\",\"os\":" + ados +
                ",\"muid\":\"" + muid +
                "\",\"lid\":" + lid +
                ",\"callback_url\":\"" + callbackUrl +
                "\",\"imei\":\"" + imei +
                "\",\"event_type\":\"" + eventType +
                "\",\"idfa\":\"" + idfa +
                "\",\"plat\":\"" + plat +
                "\",\"channel_type\":\"" + channelType +
                "\",\"push_type\":\"" + pushType +
                "\",\"click_time\":" + clickTime +
                ",\"akey\":\"" + akey + "\"" +
                ",\"token\":\"" + token + "\"" +
                ",\"company\":\"" + company + "\"" +
                ",\"hw_app_id\":\"" + hwAppId + "\"" +
                ",\"oaid\":\"" + oaid + "\"" +
                ",\"action_time\":\"" + actionTime + "\"" +
                ",\"auth_account\":\"" + authAccount + "\"" +
                ",\"creative_id\":\"" + creativeId + "\"" +
                ",\"channel_click_id\":\"" + channelClickId + "\"" +
                ",\"channel_account_id\":\"" + channelAccountId + "\"" +
                ",\"zg_appid\":" + zgAppid +
                ",\"zg_id\":" + zgId +
                "}";
    }

    public void setFields(JSONObject adMessageJson) {
        this.callback = String.valueOf(adMessageJson.get("callback") == null ? "" : adMessageJson.get("callback"));
        this.ados = Integer.valueOf(String.valueOf(adMessageJson.get("ados")));
        this.muid = String.valueOf(adMessageJson.get("muid") == null ? "" : adMessageJson.get("muid"));
        this.imei = String.valueOf(adMessageJson.get("imei") == null ? "" : adMessageJson.get("imei"));
        this.idfa = String.valueOf(adMessageJson.get("idfa") == null ? "" : adMessageJson.get("idfa"));
        this.channelType = String.valueOf(adMessageJson.get("channel_type") == null ? "" : adMessageJson.get("channel_type"));
        this.pushType = String.valueOf(adMessageJson.get("push_type") == null ? "" : adMessageJson.get("push_type"));
        this.clickTime = Long.parseLong(String.valueOf(adMessageJson.get("click_time") == null ? 0 : adMessageJson.get("click_time")));
        this.akey = String.valueOf(adMessageJson.get("akey") == null ? "" : adMessageJson.get("akey"));
        this.token = String.valueOf(adMessageJson.get("token") == null ? "" : adMessageJson.get("token"));
        this.company = String.valueOf(adMessageJson.get("company") == null ? "" : adMessageJson.get("company"));
        this.authAccount = String.valueOf(adMessageJson.get("auth_account") == null ? "" : adMessageJson.get("auth_account"));
        this.creativeId = String.valueOf(adMessageJson.get("creative_id") == null ? "" : adMessageJson.get("creative_id"));
        this.channelClickId = String.valueOf(adMessageJson.get("channel_click_id") == null ? "" : adMessageJson.get("channel_click_id"));
        this.channelAccountId = String.valueOf(adMessageJson.get("channel_account_id") == null ? "" : adMessageJson.get("channel_account_id"));
        this.zgAppid = Long.parseLong(String.valueOf(adMessageJson.get("zg_appid") == null ? 0 : adMessageJson.get("zg_appid")));
        this.hwAppId = String.valueOf(adMessageJson.get("hw_app_id") == null ? "" : adMessageJson.get("hw_app_id"));
        this.oaid = String.valueOf(adMessageJson.get("oaid") == null ? "" : adMessageJson.get("oaid"));
    }

    // Getter 和 Setter 方法
    public long getZgId() {
        return zgId;
    }

    public void setZgId(long zgId) {
        this.zgId = zgId;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public Integer getAdos() {
        return ados;
    }

    public void setAdos(Integer ados) {
        this.ados = ados;
    }

    public String getMuid() {
        return muid;
    }

    public void setMuid(String muid) {
        this.muid = muid;
    }

    public long getLid() {
        return lid;
    }

    public void setLid(long lid) {
        this.lid = lid;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public void setCallbackUrl(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getIdfa() {
        return idfa;
    }

    public void setIdfa(String idfa) {
        this.idfa = idfa;
    }

    public String getPlat() {
        return plat;
    }

    public void setPlat(String plat) {
        this.plat = plat;
    }

    public String getChannelType() {
        return channelType;
    }

    public void setChannelType(String channelType) {
        this.channelType = channelType;
    }

    public String getPushType() {
        return pushType;
    }

    public void setPushType(String pushType) {
        this.pushType = pushType;
    }

    public long getClickTime() {
        return clickTime;
    }

    public void setClickTime(long clickTime) {
        this.clickTime = clickTime;
    }

    public String getAkey() {
        return akey;
    }

    public void setAkey(String akey) {
        this.akey = akey;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getAuthAccount() {
        return authAccount;
    }

    public void setAuthAccount(String authAccount) {
        this.authAccount = authAccount;
    }

    public long getZgAppid() {
        return zgAppid;
    }

    public void setZgAppid(long zgAppid) {
        this.zgAppid = zgAppid;
    }

    public String getHwAppId() {
        return hwAppId;
    }

    public void setHwAppId(String hwAppId) {
        this.hwAppId = hwAppId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getOaid() {
        return oaid;
    }

    public void setOaid(String oaid) {
        this.oaid = oaid;
    }

    public String getCreativeId() {
        return creativeId;
    }

    public void setCreativeId(String creativeId) {
        this.creativeId = creativeId;
    }

    public String getChannelClickId() {
        return channelClickId;
    }

    public void setChannelClickId(String channelClickId) {
        this.channelClickId = channelClickId;
    }

    public String getChannelAccountId() {
        return channelAccountId;
    }

    public void setChannelAccountId(String channelAccountId) {
        this.channelAccountId = channelAccountId;
    }
}