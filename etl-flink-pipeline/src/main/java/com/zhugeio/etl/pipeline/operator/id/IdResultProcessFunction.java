package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.util.DimUtils;
import com.zhugeio.etl.common.util.ToolUtil;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.etl.pipeline.operator.gate.GateFlatMapFunction;
import com.zhugeio.tool.commons.JsonUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author ningjh
 * @name GateProcessFunction
 * @date 2025/12/10
 * @description 分流
 */
public class IdResultProcessFunction extends ProcessFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(IdResultProcessFunction.class);
    private OutputTag<String> dataQualityTag;

    private final String error_count_key = "error_count";
    private final String error_pro_count_key = "error_pro_count";
    private final String error_Log_Type = "error-log";
    private final String data_count_type = "data-count";

    public IdResultProcessFunction() {
    }

    public IdResultProcessFunction(OutputTag<String> dataQualityTag) {
        this.dataQualityTag = dataQualityTag;
    }

    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ZGMessage>.Context context, Collector<ZGMessage> collector) throws Exception {
        if (zgMessage.getResult() != -1) {
            // 主数据流
            collector.collect(zgMessage);
        }else {
            // 数据质量
            sendErrorMsgs(zgMessage,context);
        }
    }
    public void sendErrorMsgs(ZGMessage msg ,ProcessFunction<ZGMessage, ZGMessage>.Context context) {
        try {
            // 有异常
            Map<String, Object> errorMsgMap = new HashMap<>();
            int errorCode = msg.getErrorCode();
            String errorDescribe = msg.getErrorDescribe();

            if (errorCode == ErrorMessageEnum.JSON_FORMAT_ERROR.getErrorCode()) {
                return;
            }
            if (errorCode == ErrorMessageEnum.BASIC_SCHEMA_FORMAT_NOT_MATCH.getErrorCode()) {
                return;
            }
            if (errorCode == ErrorMessageEnum.AK_NONE.getErrorCode()) {
                return;
            }
            Integer appId = msg.getAppId();
            if (appId == null) {
                return;
            }
            int day = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new Date()));
            String pl = String.valueOf(msg.getData().get("pl"));
            String sdk = String.valueOf(msg.getData().get("sdk"));
            int plat = DimUtils.sdk(pl);

            // 异常数量自增写入ssdb
            String errorRedisKey = error_count_key + "#" + appId + ":" + day + ":" + plat + ":";
            // 属性异常数量自增写入ssdb
            String errorProRedisKey = error_pro_count_key + "#" + appId + ":" + day + ":" + plat + ":";

            JSONObject jsonCount = new JSONObject();
            List<?> listData = (List<?>)msg.getData().get("data");
            for (Object listDatum : listData) {
                Map<String, Object> dataMap = new HashMap<>();
                Map<?, ?> dataItem = (Map<?, ?>) listDatum;
                String dt = (String) dataItem.get("dt");
                // 处理带有上报的事件 无事件无统计意义，数据质量是按照事件展示的
                if ("evt".equals(dt) || "vtl".equals(dt) || "abp".equals(dt)) {
                    Map<?, ?> props = (Map<?, ?>) dataItem.get("pr");
                    // 事件名称
                    String eventName = props.get("$eid") == null ? "" : String.valueOf(props.get("$eid"));
                    // 事件时间
                    Object ctObj = props.get("$ct") == null ? 0 : props.get("$ct");
                    long ct = Long.parseLong(String.valueOf(ctObj));

                    int currentErrorCode = errorCode;
                    String currentErrorDescribe = errorDescribe;

                    if (dataItem.containsKey("errorCode")) {
                        currentErrorCode = Integer.parseInt(dataItem.get("errorCode").toString());
                        dataItem.remove("errorCode");
                    }
                    if (dataItem.containsKey("errorDescribe")) {
                        currentErrorDescribe = dataItem.get("errorDescribe").toString();
                        dataItem.remove("errorDescribe");
                    }

                    int proFlag = 0;
                    if (currentErrorCode == ErrorMessageEnum.EVENT_ATTR_ID_ERROR.getErrorCode()) {
                        proFlag = 1;
                    }

                    String redisKey;
                    if (proFlag == 1) {
                        redisKey = errorProRedisKey + eventName;
                    } else {
                        redisKey = errorRedisKey + eventName;
                    }

                    if (jsonCount.containsKey(redisKey)) {
                        jsonCount.put(redisKey, jsonCount.getIntValue(redisKey) + 1);
                    } else {
                        jsonCount.put(redisKey, 1);
                    }

                    Map<String, Object> modifiedErrData = new HashMap<>(msg.getData());
                    modifiedErrData.put("data", dataItem);
                    dataMap.put("app_id", appId);
                    dataMap.put("error_code", currentErrorCode);
                    String errDataJson = JsonUtil.toJson(modifiedErrData);
                    dataMap.put("data_json", errDataJson);
                    dataMap.put("data_md5", ToolUtil.getMD5Str(errDataJson));
                    dataMap.put("error_md5", ToolUtil.getMD5Str(currentErrorDescribe));
                    dataMap.put("log_utc_date", System.currentTimeMillis());
                    dataMap.put("log_utc_day_id", day);
                    dataMap.put("event_begin_date", ct);
                    dataMap.put("pl", pl);
                    dataMap.put("sdk", sdk);
                    dataMap.put("platform", plat);
                    // 1表示事件属性相关错误日志标识,0非事件属性的错误标识
                    dataMap.put("pro_flag", proFlag);
                    dataMap.put("event_name", eventName);
                    dataMap.put("error_msg", currentErrorDescribe);

                    errorMsgMap.put("data", dataMap);
                    errorMsgMap.put("type", error_Log_Type);
                    // 数据质量 错误数据明细
                    context.output(dataQualityTag, JsonUtil.toJson(errorMsgMap));
                }
            }

            // 数据质量 错误数据统计
            if (!jsonCount.isEmpty()) {
                JSONObject allCountJson = new JSONObject();
                allCountJson.put("data", jsonCount);
                allCountJson.put("type", data_count_type);
                context.output(dataQualityTag, allCountJson.toJSONString());
            }
        } catch (Exception e) {
            logger.error("IdResultProcessFunction error , msg is {}",msg,e);
        }
    }
}
