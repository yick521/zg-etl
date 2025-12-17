package com.zhugeio.etl.pipeline.operator.gate;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @author ningjh
 * @name GateProcessFunction
 * @date 2025/12/10
 * @description 分流
 */
public class GateProcessFunction extends ProcessFunction<ZGMessage, ZGMessage> {
    private OutputTag<String> dataQualityTag;
    private OutputTag<String> debugTag;

    public GateProcessFunction() {
    }

    public GateProcessFunction(OutputTag<String> dataQualityTag, OutputTag<String> debugTag) {
        this.dataQualityTag = dataQualityTag;
        this.debugTag = debugTag;
    }

    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ZGMessage>.Context context, Collector<ZGMessage> collector) throws Exception {
        if (zgMessage.getDataQualityError() == null) {
            // 主数据流
            collector.collect(zgMessage);
            // 数据质量数据统计-含接入数据、错误数据 注意：解析失败的数据不在统计范围内 因为统计是按照appid和事件来的
            context.output(dataQualityTag, zgMessage.getDataQuality().toString());
            // debug功能数据
            Map<String, Object> data = zgMessage.getData();
            if(data.containsKey("debug") && String.valueOf(data.get("debug")).equals("1")){
                context.output(debugTag, zgMessage.getRawData());
            }
        }else {
            // 数据质量错误明细
            context.output(dataQualityTag, zgMessage.getDataQualityError().toString());
        }
    }
}
