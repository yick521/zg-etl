package com.zhugeio.etl.pipeline.operator.gate;

import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author ningjh
 * @name GateProcessFunction
 * @date 2025/12/10
 * @description 分流
 */
public class AdvSaveAppAdDataProcessFunction extends ProcessFunction<ZGMessage, ZGMessage> {
    private OutputTag<String> advSaveAppAdDataTag;

    public AdvSaveAppAdDataProcessFunction() {
    }

    public AdvSaveAppAdDataProcessFunction(OutputTag<String> advSaveAppAdDataTag) {
        this.advSaveAppAdDataTag = advSaveAppAdDataTag;
    }

    @Override
    public void processElement(ZGMessage zgMessage, ProcessFunction<ZGMessage, ZGMessage>.Context context, Collector<ZGMessage> collector) throws Exception {
        List<String> advKafkaMsgList = zgMessage.getAdvKafkaMsgList();
        if(advKafkaMsgList != null && !advKafkaMsgList.isEmpty()){
            for (String avgKafkaMsg : advKafkaMsgList){
                context.output(advSaveAppAdDataTag, avgKafkaMsg);
            }
        }
        zgMessage.setAdvKafkaMsgList(null);
        collector.collect(zgMessage);
    }
}
