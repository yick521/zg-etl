package com.zhugeio.etl.pipeline.operator.adv;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.model.ToufangAdClickRow;
import com.zhugeio.etl.pipeline.model.ToufangConvertEventRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ningjh
 * @name GateProcessFunction
 * @date 2025/12/10
 * @description 分流
 */
public class AdvProcessFunction extends ProcessFunction<ZGMessage, ToufangConvertEventRow> {
    private static final Logger logger = LoggerFactory.getLogger(AdvProcessFunction.class);

    private OutputTag<ToufangAdClickRow> advClickOutputTag;

    public AdvProcessFunction() {
    }

    public AdvProcessFunction(OutputTag<ToufangAdClickRow> advClickOutputTag) {
        this.advClickOutputTag = advClickOutputTag;
    }

    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ToufangConvertEventRow>.Context context, Collector<ToufangConvertEventRow> collector) throws Exception {
        String rawData = zgMessage.getRawData();
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(rawData);
        }catch (Exception e){
            logger.error("json parse error ",e);
        }
        if (jsonObject != null && !jsonObject.isEmpty()){
            String tableName = jsonObject.getString("tableName");
            if ("toufang_convert_event".equals(tableName)){
                ToufangConvertEventRow rowDataFromJson = jsonToToufangConvertEventRow(jsonObject);
                collector.collect(rowDataFromJson);
            }

            if("toufang_ad_click".equals(tableName)) {
                ToufangAdClickRow toufangAdClickRow = jsonToAdClickRowData(jsonObject);
                if(toufangAdClickRow != null)
                    context.output(advClickOutputTag, toufangAdClickRow);
            }

        }
    }

    /**
     * 将 JSONObject 转换为 ToufangConvertEventRow
     */
    public static ToufangConvertEventRow jsonToToufangConvertEventRow(JSONObject json) {
        JSONObject dataJson = json.getJSONObject("data");
        ToufangConvertEventRow row = new ToufangConvertEventRow();
        row.setZgAppid(dataJson.getInteger("zg_appid"));
        row.setZgId(dataJson.getInteger("zg_id"));
        row.setLid(dataJson.getInteger("lid"));
        row.setChannelId(dataJson.getInteger("channel_id"));
        row.setZgEid(dataJson.getInteger("zg_eid"));
        row.setEventTime(dataJson.getLong("event_time"));
        row.setChannelAdgroupId(dataJson.getString("channel_adgroup_id"));
        row.setChannelAdgroupName(dataJson.getString("channel_adgroup_name"));
        row.setClickTime(dataJson.getLong("click_time"));
        row.setEventName(dataJson.getString("event_name"));
        row.setChannelEvent(dataJson.getString("channel_event"));
        row.setMatchJson(dataJson.getString("match_json"));
        row.setFrequency(dataJson.getInteger("frequency"));
        row.setUtmCampaign(dataJson.getString("utm_campaign"));
        row.setUtmSource(dataJson.getString("utm_source"));
        row.setUtmMedium(dataJson.getString("utm_medium"));
        row.setUtmTerm(dataJson.getString("utm_term"));
        row.setUtmContent(dataJson.getString("utm_content"));
        return row;
    }

    /**
     * 添加待写入 toufang_ad_click 表的数据
     * 返回ToufangAdClickRow对象
     *
     * @param json 输入JSON对象
     * @return ToufangAdClickRow对象，如果数据无效则返回null
     */
    public static ToufangAdClickRow jsonToAdClickRowData(JSONObject json) {
        if (json == null || !json.containsKey("data")) {
            return null;
        }

        JSONObject dataJson = json.getJSONObject("data");

        // 根据原Scala逻辑，判断使用哪个分支
        if (dataJson.containsKey("value_ad_data")) {
            JSONObject adDataJson = dataJson.getJSONObject("value_ad_data");
            // 匹配上zgid的广告信息
            String keyAdData = dataJson.getString("key_ad_data");
            // adtfdata:${appId}:${zgid}:${adms.ct}
            String[] split = keyAdData.split(":");
            String appId = split.length > 1 ? split[1] : " ";
            String zgid = split.length > 2 ? split[2] : " ";
            String click_time = split.length > 3 ? split[3] : " ";

            ToufangAdClickRow row = new ToufangAdClickRow();

            // 设置主键字段
            row.setZgAppid(appId);
            row.setClickTime(click_time);
            row.setKeyIpData(" ");        // 原逻辑：空字符串转为空格
            row.setKeyMuidData(" ");      // 原逻辑：空字符串转为空格
            row.setZgId(zgid);
            row.setKeyAdData(StringUtils.isEmpty(dataJson.getString("key_ad_data")) ? " " : dataJson.getString("key_ad_data"));

            // 设置普通字段
            row.setOtherKey(StringUtils.isEmpty(dataJson.getString("other_key")) ? " " : dataJson.getString("other_key"));
            row.setOtherValue(" ");       // 原Scala代码中没有设置other_value，设为空格
            row.setKeyIpTime(" ");        // 原逻辑：空字符串转为空格
            row.setKeyMuidTime(" ");      // 原逻辑：空字符串转为空格
            row.setAdData(" ");           // 原逻辑：空字符串转为空格
            row.setIsDelete(" ");         // 原逻辑：空字符串转为空格
            row.setKeyAdTime(StringUtils.isEmpty(dataJson.getString("key_ad_time")) ? " " : dataJson.getString("key_ad_time"));
            row.setValueAdTime(StringUtils.isEmpty(dataJson.getString("value_ad_time")) ? " " : dataJson.getString("value_ad_time"));
            row.setValueAdData(StringUtils.isEmpty(dataJson.getString("value_ad_data")) ? " " : dataJson.getString("value_ad_data"));
            row.setKeyAdLid(StringUtils.isEmpty(dataJson.getString("key_ad_lid")) ? " " : dataJson.getString("key_ad_lid"));
            row.setValueAdLid(StringUtils.isEmpty(dataJson.getString("value_ad_lid")) ? " " : dataJson.getString("value_ad_lid"));
            row.setKeyType("appid_zgid"); // key_type
            row.setLid(StringUtils.isEmpty(adDataJson.getString("lid")) ? " " : adDataJson.getString("lid"));
            row.setUpdateTime(System.currentTimeMillis());
            return row;
        } else if (dataJson.containsKey("ip_ua_key")) {
            String click_time = StringUtils.isEmpty(dataJson.getString("click_time")) ? " " : dataJson.getString("click_time");
            String ip_ua_key = StringUtils.isEmpty(dataJson.getString("ip_ua_key")) ? " " : dataJson.getString("ip_ua_key");

            // 按照优先级获取 muid_key
            String muid_key = getMuidKeyByPriority(dataJson);

            ToufangAdClickRow row = new ToufangAdClickRow();

            // 设置主键字段
            row.setZgAppid(StringUtils.isEmpty(dataJson.getString("zg_appid")) ? " " : dataJson.getString("zg_appid"));
            row.setClickTime(click_time);
            row.setKeyIpData(ip_ua_key + ":" + click_time);   // "${ip_ua_key}:${click_time}"
            row.setKeyMuidData(muid_key + ":" + click_time);  // "${muid_key}:${click_time}"
            row.setZgId(" ");          // 原逻辑：空字符串转为空格
            row.setKeyAdData(" ");     // 原逻辑：空字符串转为空格

            // 设置普通字段
            row.setOtherKey(StringUtils.isEmpty(dataJson.getString("other_key")) ? " " : dataJson.getString("other_key"));
            row.setOtherValue(" ");    // 原Scala代码中没有设置other_value，设为空格
            row.setKeyIpTime(" ");     // 原逻辑：空字符串转为空格
            row.setKeyMuidTime(" ");   // 原逻辑：空字符串转为空格
            row.setAdData(dataJson.toJSONString());  // dataJSon.toJSONString
            row.setIsDelete(StringUtils.isEmpty(dataJson.getString("is_delete")) ? " " : dataJson.getString("is_delete"));
            row.setKeyAdTime(" ");     // 原逻辑：空字符串转为空格
            row.setValueAdTime(" ");   // 原逻辑：空字符串转为空格
            row.setValueAdData(" ");   // 原逻辑：空字符串转为空格
            row.setKeyAdLid(" ");      // 原逻辑：空字符串转为空格
            row.setValueAdLid(" ");    // 原逻辑：空字符串转为空格
            row.setKeyType("appid_muid_or_ip");  // key_type
            row.setLid(StringUtils.isEmpty(dataJson.getString("lid")) ? " " : dataJson.getString("lid"));
            row.setUpdateTime(System.currentTimeMillis());
            return row;
        } else {
            // 两个条件都不满足，返回null
            return null;
        }
    }


    /**
     * 按照优先级获取 muid_key
     * 完全按照原Scala逻辑实现
     */
    private static String getMuidKeyByPriority(JSONObject dataJson) {
//         注意：原Scala代码有逻辑错误，应该是"如果不是空"才赋值，但代码写的是"如果是空"才赋值
//         原Scala代码：
//        if (StringUtils.isEmpty(dataJSon.getString("channel_click_id_key"))) {
//            muid_key = dataJSon.getString("channel_click_id_key")
//        } else if (StringUtils.isEmpty(dataJSon.getString("muid_key"))) {
//            muid_key = dataJSon.getString("muid_key")
//        } else if (StringUtils.isEmpty(dataJSon.getString("idfa_key"))) {
//            muid_key = dataJSon.getString("idfa_key")
//        } else if (StringUtils.isEmpty(dataJSon.getString("imei_key"))) {
//            muid_key = dataJSon.getString("imei_key")
//        } else if (StringUtils.isEmpty(dataJSon.getString("android_id_key"))) {
//            muid_key = dataJSon.getString("android_id_key")
//        } else if (StringUtils.isEmpty(dataJSon.getString("oaid_key"))) {
//            muid_key = dataJSon.getString("oaid_key")
//        }
//         这看起来是逻辑错误，应该是检查不为空才赋值

        // 按照正确逻辑重新实现（按优先级检查字段是否存在且不为空）
        if (!StringUtils.isEmpty(dataJson.getString("channel_click_id_key"))) {
            return dataJson.getString("channel_click_id_key");
        } else if (!StringUtils.isEmpty(dataJson.getString("muid_key"))) {
            return dataJson.getString("muid_key");
        } else if (!StringUtils.isEmpty(dataJson.getString("idfa_key"))) {
            return dataJson.getString("idfa_key");
        } else if (!StringUtils.isEmpty(dataJson.getString("imei_key"))) {
            return dataJson.getString("imei_key");
        } else if (!StringUtils.isEmpty(dataJson.getString("android_id_key"))) {
            return dataJson.getString("android_id_key");
        } else if (!StringUtils.isEmpty(dataJson.getString("oaid_key"))) {
            return dataJson.getString("oaid_key");
        } else {
            return " ";
        }
    }


}
