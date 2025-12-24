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
        if(input.getResult() != -1){
            ZGMessage output = createOutput(input);
            out.collect(output);
        }else {
            out.collect(input);
        }
    }

    private ZGMessage createOutput(ZGMessage input) {
        JSONArray data = (JSONArray)input.getData();
        for (int i = 0; i < data.size(); i++){
            JSONObject item = data.getJSONObject(i);
            if (item != null) {
                Object pr = item.get("pr");
                if (pr instanceof JSONObject) {
                    JSONObject prObject = (JSONObject) pr;
                    if(prObject.containsKey("$sid")){
                        prObject.put("$zg_sid", prObject.getLongValue("$sid"));
                    }else {
                        prObject.put("$zg_sid", -1L);
                    }
                }
                String dt = item.getString("dt");
                if("evt".equals(dt) || "ss".equals(dt) || "se".equals(dt) || "mkt".equals(dt) || "abp".equals(dt)){
                    item.put("$uuid", generateUUID());
                }
            }
        }
        return input;
    }

    private String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}