package com.zhugeio.etl.pipeline.operator.gate;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * @author ningjh
 * @name GateProcessFunction
 * @date 2025/12/10
 * @description 分流
 */
public class AdvConvertEventProcessFunction extends ProcessFunction<ZGMessage, ZGMessage> {
    private OutputTag<String> advConvertEventTag;
    private OutputTag<String> advConvertEventUserTag;

    public AdvConvertEventProcessFunction() {
    }

    public AdvConvertEventProcessFunction(OutputTag<String> advConvertEventTag, OutputTag<String> advConvertEventUserTag) {
        this.advConvertEventTag = advConvertEventTag;
        this.advConvertEventUserTag = advConvertEventUserTag;
    }

    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ZGMessage>.Context context, Collector<ZGMessage> collector) throws Exception {
        List<String> advKafkaMsgList = zgMessage.getAdvKafkaMsgList();
        if(advKafkaMsgList != null && !advKafkaMsgList.isEmpty()){
            for (String msg : advKafkaMsgList){
                context.output(advConvertEventTag, msg);
            }
        }
        zgMessage.setAdvKafkaMsgList(null);
        List<String> advUserKafkaMsgList = zgMessage.getAdvUserKafkaMsgList();
        if(advUserKafkaMsgList != null && !advUserKafkaMsgList.isEmpty()){
            for (String msg : advUserKafkaMsgList){
                context.output(advConvertEventUserTag, msg);
            }
        }
        zgMessage.setAdvUserKafkaMsgList(null);
        collector.collect(zgMessage);
    }
}
