package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * RouterOutput 解包算子
 * 
 * 将 RouterOutput 中的数据解包并路由到对应的侧输出
 * 
 * 主输出: EventAttrRow
 * 侧输出: UserRow, DeviceRow, UserPropertyRow
 */
public class RouterOutputUnpacker extends ProcessFunction<RouterOutput, EventAttrRow> {
    
    private static final long serialVersionUID = 1L;
    
    // 侧输出标签 (与原 DataRouterOperator 保持一致)
    public static final OutputTag<UserRow> USER_OUTPUT =
            new OutputTag<UserRow>("user-output") {};
    public static final OutputTag<DeviceRow> DEVICE_OUTPUT =
            new OutputTag<DeviceRow>("device-output") {};
    public static final OutputTag<UserPropertyRow> USER_PROPERTY_OUTPUT =
            new OutputTag<UserPropertyRow>("user-property-output") {};
    
    @Override
    public void processElement(RouterOutput output, Context ctx, Collector<EventAttrRow> out) throws Exception {
        if (output == null || output.isEmpty()) {
            return;
        }
        
        // 输出 UserRow 到侧输出
        for (UserRow row : output.getUserRows()) {
            ctx.output(USER_OUTPUT, row);
        }
        
        // 输出 DeviceRow 到侧输出
        for (DeviceRow row : output.getDeviceRows()) {
            ctx.output(DEVICE_OUTPUT, row);
        }
        
        // 输出 UserPropertyRow 到侧输出
        for (UserPropertyRow row : output.getUserPropertyRows()) {
            ctx.output(USER_PROPERTY_OUTPUT, row);
        }
        
        // 输出 EventAttrRow 到主输出
        for (EventAttrRow row : output.getEventAttrRows()) {
            out.collect(row);
        }
    }
}
