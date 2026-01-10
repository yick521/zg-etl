package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.util.DimUtils;
import com.zhugeio.etl.common.util.ToolUtil;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.enums.ErrorMessageEnum;
import com.zhugeio.tool.commons.JsonUtil;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;


public class IdResultProcessFunction extends ProcessFunction<ZGMessage, ZGMessage> {
    private static final Logger logger = LoggerFactory.getLogger(IdResultProcessFunction.class);
    private final OutputTag<String> dataQualityTag;

    public IdResultProcessFunction(OutputTag<String> dataQualityTag) {
        this.dataQualityTag = dataQualityTag;
    }


    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ZGMessage>.Context context, Collector<ZGMessage> collector) {
        if (zgMessage.getResult() != -1) {
            Object dataObj = zgMessage.getData().get("data");
            if (dataObj != null && !"[]".equals(dataObj.toString())) {
                // 从数据中提取ID信息
                putIdInfos(zgMessage.getData());
            }
            collector.collect(zgMessage);
        } else {
            sendErrorMsgs(zgMessage, context);
        }
    }

    /**
     * 添加ID信息到消息数据中
     */
    private void putIdInfos(Map<String, Object> msgData) {
        try {
            List<Map<String, Object>> dataList = (List<Map<String, Object>>) msgData.get("data");
            if (dataList == null) {
                return;
            }

            // 添加zgid信息
            List<Map<String, Object>> zgidItems = getIdInfosFromMsg(msgData);
            dataList.addAll(zgidItems);

            // 添加did信息
            List<Map<String, Object>> didItems = getDidInfosFromMsg(msgData);
            dataList.addAll(didItems);

            // 添加uid信息
            List<Map<String, Object>> uidItems = getUidInfosFromMsg(msgData);
            dataList.addAll(uidItems);
        } catch (Exception e) {
            logger.error("Put ID infos error, msgData is {}", msgData, e);
        }
    }

    /**
     * 获取DID信息
     */
    private List<Map<String, Object>> getDidInfosFromMsg(Map<String, Object> msgData) {
        Map<String, Object> resultItem = new HashMap<>();
        resultItem.put("dt", "pl");

        Map<String, Object> pr = new HashMap<>();
        resultItem.put("pr", pr);
        pr.put("$dv", "zhuge.io");

        Map<String, Object> usr = (Map<String, Object>) msgData.get("usr");
        if (usr != null) {
            pr.put("$zg_did", usr.get("$zg_did"));
        }

        List<Map<String, Object>> items = new ArrayList<>();
        items.add(resultItem);
        return items;
    }

    /**
     * 获取ZGID信息
     */
    private List<Map<String, Object>> getIdInfosFromMsg(Map<String, Object> msgData) {
        List<Map<String, Object>> lists = new ArrayList<>();
        Set<IdComposite> composites = new HashSet<>();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) msgData.get("data");
        if (dataList == null) return lists;

        for (Map<String, Object> map : dataList) {
            Map<String, Object> pr = (Map<String, Object>) map.get("pr");
            if (pr != null) {
                IdComposite idComposite = getIdCompositeFromProp(pr);
                if (!composites.contains(idComposite)) {
                    composites.add(idComposite);
                    Set<String> keys = new HashSet<>(Arrays.asList("$ct", "$tz", "$zg_did", "$zg_uid", "$zg_zgid"));
                    lists.add(getMapFromMap("zgid", keys, pr));
                }
            }
        }
        return lists;
    }

    /**
     * 获取UID信息
     */
    private List<Map<String, Object>> getUidInfosFromMsg(Map<String, Object> msgData) {
        List<Map<String, Object>> lists = new ArrayList<>();
        Set<String> cuids = new HashSet<>();

        List<Map<String, Object>> dataList = (List<Map<String, Object>>) msgData.get("data");
        if (dataList == null) return lists;

        for (Map<String, Object> map : dataList) {
            Map<String, Object> pr = (Map<String, Object>) map.get("pr");
            if (pr != null && pr.containsKey("$cuid")) {
                String cuid = String.valueOf(pr.get("$cuid"));
                if (!cuids.contains(cuid)) {
                    Set<String> keys = new HashSet<>(Arrays.asList("$ct", "$tz", "$zg_did", "$zg_uid", "$zg_zgid", "$cuid"));
                    lists.add(getMapFromMap("usr", keys, pr));
                    cuids.add(cuid);
                }
            }
        }
        return lists;
    }

    /**
     * 从属性中提取ID组合
     */
    private IdComposite getIdCompositeFromProp(Map<String, Object> prop) {
        Long zgDid = prop.get("$zg_did") == null ? null : Long.valueOf(String.valueOf(prop.get("$zg_did")));
        Long zgUid = prop.get("$zg_uid") == null ? null : Long.valueOf(String.valueOf(prop.get("$zg_uid")));
        Long zgId = prop.get("$zg_zgid") == null ? null : Long.valueOf(String.valueOf(prop.get("$zg_zgid")));
        return new IdComposite(zgDid, zgUid, zgId);
    }

    /**
     * 从原始属性中提取指定keys的数据
     */
    private Map<String, Object> getMapFromMap(String dt, Set<String> keys, Map<String, Object> prop) {
        Map<String, Object> result = new HashMap<>();
        result.put("dt", dt);

        Map<String, Object> resultProp = new HashMap<>();
        for (Map.Entry<String, Object> entry : prop.entrySet()) {
            if (keys.contains(entry.getKey()) && entry.getValue() != null) {
                resultProp.put(entry.getKey(), entry.getValue());
            }
        }
        result.put("pr", resultProp);
        return result;
    }

    /**
     * ID组合类，用于去重
     */
    private static class IdComposite {
        private final Long zgDid;
        private final Long zgUid;
        private final Long zgId;

        public IdComposite(Long zgDid, Long zgUid, Long zgId) {
            this.zgDid = zgDid;
            this.zgUid = zgUid;
            this.zgId = zgId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IdComposite that = (IdComposite) o;
            return Objects.equals(zgDid, that.zgDid) &&
                    Objects.equals(zgUid, that.zgUid) &&
                    Objects.equals(zgId, that.zgId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(zgDid, zgUid, zgId);
        }
    }

    public void sendErrorMsgs(ZGMessage msg, ProcessFunction<ZGMessage, ZGMessage>.Context context) {
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
            String error_count_key = "error_count";
            String errorRedisKey = error_count_key + "#" + appId + ":" + day + ":" + plat + ":";
            // 属性异常数量自增写入ssdb
            String error_pro_count_key = "error_pro_count";
            String errorProRedisKey = error_pro_count_key + "#" + appId + ":" + day + ":" + plat + ":";

            JSONObject jsonCount = new JSONObject();
            List<?> listData = (List<?>) msg.getData().get("data");
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
                    String error_Log_Type = "error-log";
                    errorMsgMap.put("type", error_Log_Type);
                    // 数据质量 错误数据明细
                    context.output(dataQualityTag, JsonUtil.toJson(errorMsgMap));
                }
            }

            // 数据质量 错误数据统计
            if (!jsonCount.isEmpty()) {
                JSONObject allCountJson = new JSONObject();
                allCountJson.put("data", jsonCount);
                String data_count_type = "data-count";
                allCountJson.put("type", data_count_type);
                context.output(dataQualityTag, allCountJson.toJSONString());
            }
        } catch (Exception e) {
            logger.error("IdResultProcessFunction error , msg is {}", msg, e);
        }
    }
}