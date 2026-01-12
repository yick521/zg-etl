package com.zhugeio.etl.pipeline.operator.gate;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.util.DimUtils;
import com.zhugeio.etl.common.util.ToolUtil;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import org.apache.flink.configuration.Configuration;
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
public class GateProcessFunction extends ProcessFunction<ZGMessage, ZGMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(GateProcessFunction.class);

    private OutputTag<String> dataQualityTag;
    private OutputTag<String> debugTag;
    private SimpleDateFormat sdf ;

    public GateProcessFunction() {
    }

    public GateProcessFunction(OutputTag<String> dataQualityTag, OutputTag<String> debugTag) {
        this.dataQualityTag = dataQualityTag;
        this.debugTag = debugTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sdf = new SimpleDateFormat("yyyyMMdd");
    }

    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ZGMessage>.Context context, Collector<ZGMessage> collector) throws Exception {
        LOG.debug("GateProcessFunction processElement zgMessage is {} ",zgMessage);
        if(zgMessage.getResult() != -1){
            // 主数据流
            collector.collect(zgMessage);
            // debug功能数据
            Map<String, Object> data = zgMessage.getData();
            if(data.containsKey("debug") && String.valueOf(data.get("debug")).equals("1")){
                context.output(debugTag, zgMessage.getRawData());
            }
            // 数据质量 接入(正确)数据统计
            int day = Integer.parseInt(sdf.format(new Date()));
            JSONObject successCountJson = new JSONObject();
            String pl = zgMessage.getData().get("pl").toString();
            int plat = DimUtils.sdk(pl);
            allCount(zgMessage, plat, day, successCountJson);
            LOG.debug("GateProcessFunction successCountJson is {} ",successCountJson);
            if(!successCountJson.isEmpty()){
                JSONObject countJson = new JSONObject();
                countJson.put("data", successCountJson);
                countJson.put("type", "data-count");
                context.output(dataQualityTag, countJson.toString());
            }
        }else { // 错误数据
            // 只有部分是要统计的错误  有些错误连 ak 或 appId 都获取不到，统计了也没意义
            if(
                    zgMessage.getErrorCode()== ErrorMessageEnum.IP_BLOCK.getErrorCode() ||
                    zgMessage.getErrorCode()== ErrorMessageEnum.UA_BLOCK.getErrorCode()
            ){
                int day = Integer.parseInt(sdf.format(new Date()));
                JSONObject errorCountJson = new JSONObject();
                List<String> errorLogList = new ArrayList<>();
                String pl = zgMessage.getData().get("pl").toString();
                int plat = DimUtils.sdk(pl);
                String sdk = zgMessage.getData().get("sdk").toString();
                errorCount(zgMessage, pl, plat, sdk, day, errorCountJson, errorLogList);
                LOG.debug("GateProcessFunction errorLogList is {} ",errorLogList);
                // 数据质量 错误统计
                if(!errorCountJson.isEmpty()){
                    JSONObject countJson = new JSONObject();
                    countJson.put("data", errorCountJson);
                    countJson.put("type", "data-count");
                    context.output(dataQualityTag, countJson.toString());
                }
                // 数据质量 错误明细
                for (String errorLog : errorLogList){
                    LOG.debug("GateProcessFunction errorLog is {} ",errorLog);
                    context.output(dataQualityTag, errorLog);
                }
            }
        }
    }

    /**
     *
     * @param zgMessage
     * @param plat
     * @param day
     * @param allCountJson 统计数量
     */
    private void allCount(ZGMessage zgMessage,int plat,int day,JSONObject allCountJson){
        String key = "all_count" + "#" + zgMessage.getAppId() + ":" + day + ":" + plat + ":";
        List<?> listData = (List<?>)zgMessage.getData().get("data");
        for (Object listDatum : listData) {
            Map<?, ?> map = (Map<?, ?>) listDatum;
            String dt = String.valueOf(map.get("dt"));
            //处理带有上报的事件
            if ("evt".equals(dt) || "vtl".equals(dt) || "abp".equals(dt)) {
                Map<?, ?> pr = (Map<?, ?>) map.get("pr");
                //事件名称
                String eventName = String.valueOf(pr.get("$eid"));
                if (allCountJson.containsKey(key + eventName)) {
                    allCountJson.put(key + eventName, allCountJson.getIntValue(key + eventName) + 1);
                } else {
                    allCountJson.put(key + eventName, 1);
                }
            }
        }
    }

    /**
     *
     * @param zgMessage
     * @param pl
     * @param plat
     * @param sdk
     * @param day
     * @param errorCountJson 统计错误数量
     * @param errorLogList 记录错误明细
     */
    private void errorCount(ZGMessage zgMessage, String pl, int plat, String sdk, int day, JSONObject errorCountJson, List<String> errorLogList){
        String key = "error_count" + "#" + zgMessage.getAppId() + ":" + day + ":" + plat + ":";
        List<?> listData = (List<?>)zgMessage.getData().get("data");
        for (Object listDatum : listData) {
            Map<?, ?> map = (Map<?, ?>) listDatum;
            String dt = String.valueOf(map.get("dt"));
            //处理带有上报的事件
            if ("evt".equals(dt) || "vtl".equals(dt) || "abp".equals(dt)) {
                Map<?, ?> pr = (Map<?, ?>) map.get("pr");
                //事件名称
                String eventName = String.valueOf(pr.get("$eid"));

                //事件时间
                Object ctObj = pr.get("$ct");
                long ct = Long.parseLong(String.valueOf(ctObj == null ? 0 : ctObj));

                if (errorCountJson.containsKey(key + eventName)) {
                    errorCountJson.put(key + eventName, errorCountJson.getIntValue(key + eventName) + 1);
                } else {
                    errorCountJson.put(key + eventName, 1);
                }
                HashMap<String, Object> dataMap = new HashMap<>();
                //错误日志详情数据组装
                dataMap.put("app_id", zgMessage.getAppId());
                dataMap.put("error_code", zgMessage.getErrorCode());
                dataMap.put("data_json", zgMessage.getRawData());
                dataMap.put("data_md5", ToolUtil.getMD5Str(zgMessage.getRawData()));
                dataMap.put("error_md5", ToolUtil.getMD5Str(zgMessage.getErrorDescribe()));
                dataMap.put("log_utc_date", System.currentTimeMillis());
                dataMap.put("log_utc_day_id", day);
                dataMap.put("event_begin_date", ct);
                dataMap.put("pl", pl);
                dataMap.put("sdk", sdk);
                dataMap.put("platform", plat);
                dataMap.put("pro_flag", 0); // 1表示事件属性相关错误日志标识
                dataMap.put("event_name", eventName);
                dataMap.put("error_msg", zgMessage.getErrorDescribe());
                // 错误明细收集
                JSONObject errorLogMap = new JSONObject();
                errorLogMap.put("data", dataMap);
                errorLogMap.put("type", "error-event-log");
                errorLogList.add(errorLogMap.toString());
            }
        }
    }
}
