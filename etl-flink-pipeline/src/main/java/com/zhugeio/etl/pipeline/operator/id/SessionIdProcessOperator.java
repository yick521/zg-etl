package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class SessionIdProcessOperator extends ProcessFunction<ZGMessage, ZGMessage> {

    @Override
    public void processElement(ZGMessage input, Context ctx, Collector<ZGMessage> out) throws Exception {
        if (input.getResult() != -1) {
            ZGMessage output = createOutput(input);
            out.collect(output);
        } else {
            out.collect(input);
        }
    }

    private ZGMessage createOutput(ZGMessage input) {
        // input.getData() 返回的是整个消息体 Map/JSONObject，其中包含 "data" 数组
        Object dataObj = input.getData();

        if (dataObj == null) {
            return input;
        }

        JSONArray dataArray = null;

        // 处理不同的数据类型
        if (dataObj instanceof JSONObject) {
            JSONObject dataJson = (JSONObject) dataObj;
            dataArray = dataJson.getJSONArray("data");
        } else if (dataObj instanceof java.util.Map) {
            // 如果是 Map 类型，先转换为 JSONObject
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> dataMap = (java.util.Map<String, Object>) dataObj;
            Object innerData = dataMap.get("data");
            if (innerData instanceof JSONArray) {
                dataArray = (JSONArray) innerData;
            } else if (innerData instanceof java.util.List) {
                // 如果是 List，转换为 JSONArray
                dataArray = new JSONArray((java.util.List<?>) innerData);
                dataMap.put("data", dataArray);  // 更新回去
            }
        }

        if (dataArray == null) {
            return input;
        }

        // 遍历 data 数组中的每个元素
        for (int i = 0; i < dataArray.size(); i++) {
            JSONObject item = dataArray.getJSONObject(i);
            if (item == null) {
                continue;
            }

            // 获取 pr 对象
            Object prObj = item.get("pr");
            if (prObj instanceof JSONObject) {
                JSONObject pr = (JSONObject) prObj;

                // 处理 $sid -> $zg_sid
                if (pr.containsKey("$sid")) {
                    pr.put("$zg_sid", pr.getLongValue("$sid"));
                } else {
                    pr.put("$zg_sid", -1L);
                }
            }

            // 为特定事件类型添加 UUID
            String dt = item.getString("dt");
            if ("evt".equals(dt) || "ss".equals(dt) || "se".equals(dt) ||
                    "mkt".equals(dt) || "abp".equals(dt)) {
                item.put("$uuid", generateUUID());
            }
        }

        return input;
    }

    private String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}