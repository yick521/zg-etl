package com.zhugeio.etl.pipeline.operator.id;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 会话ID处理算子 - 修复版
 *
 * 修复点:
 * 1. 使用 Map<String, Object> 访问数据，而非强转 JSONObject
 * 2. 与 UserPropAsyncOperator 保持一致的数据访问方式
 */
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

    @SuppressWarnings("unchecked")
    private ZGMessage createOutput(ZGMessage input) {
        Object dataObj = input.getData();

        if (dataObj == null) {
            return input;
        }

        // 修复: 使用 Map 方式访问数据
        if (!(dataObj instanceof Map)) {
            return input;
        }

        Map<String, Object> dataMap = (Map<String, Object>) dataObj;
        Object innerData = dataMap.get("data");

        if (!(innerData instanceof List)) {
            return input;
        }

        List<Map<String, Object>> dataArray = (List<Map<String, Object>>) innerData;

        // 遍历 data 数组中的每个元素
        for (Map<String, Object> item : dataArray) {
            if (item == null) {
                continue;
            }

            // 获取 pr 对象
            Object prObj = item.get("pr");
            if (prObj instanceof Map) {
                Map<String, Object> pr = (Map<String, Object>) prObj;

                // 处理 $sid -> $zg_sid
                if (pr.containsKey("$sid")) {
                    pr.put("$zg_sid", parseSidToLong(pr.get("$sid")));
                } else {
                    pr.put("$zg_sid", -1L);
                }

                // 为特定事件类型添加 UUID
                String dt = (String) item.get("dt");
                if ("evt".equals(dt) || "ss".equals(dt) || "se".equals(dt) ||
                        "mkt".equals(dt) || "abp".equals(dt)) {
                    // 将 UUID 添加到 pr 中
                    pr.put("$uuid", generateUUID());
                }
            }
        }

        return input;
    }

    private Long parseSidToLong(Object sidValue) {
        if (sidValue == null) {
            return -1L;
        }
        try {
            return new BigDecimal(sidValue.toString()).longValue();
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    private String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}