package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.pipeline.entity.IdArchiveMessage;
import com.zhugeio.etl.pipeline.entity.IdArchiveMessage.ArchiveType;
import com.zhugeio.etl.pipeline.model.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * ID 映射路由算子
 * 
 * 输入: IdArchiveMessage (Kafka 消息)
 * 输出: 根据 type 路由到 5 个不同的输出
 * 
 * 主输出: DeviceIdRow (DEVICE 类型最多，作为主输出)
 * 侧输出:
 *   - USER_OUTPUT: UserIdRow
 *   - DEVICE_ZGID_OUTPUT: DeviceZgidRow
 *   - USER_ZGID_OUTPUT: UserZgidRow
 *   - ZGID_USER_OUTPUT: ZgidUserRow
 */
public class IdArchiveRouterOperator extends ProcessFunction<IdArchiveMessage, DeviceIdRow> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IdArchiveRouterOperator.class);

    // 侧输出标签
    public static final OutputTag<UserIdRow> USER_OUTPUT =
            new OutputTag<UserIdRow>("user-output") {};
    public static final OutputTag<DeviceZgidRow> DEVICE_ZGID_OUTPUT =
            new OutputTag<DeviceZgidRow>("device-zgid-output") {};
    public static final OutputTag<UserZgidRow> USER_ZGID_OUTPUT =
            new OutputTag<UserZgidRow>("user-zgid-output") {};
    public static final OutputTag<ZgidUserRow> ZGID_USER_OUTPUT =
            new OutputTag<ZgidUserRow>("zgid-user-output") {};

    // 统计计数器
    private transient AtomicLong deviceCount;
    private transient AtomicLong userCount;
    private transient AtomicLong deviceZgidCount;
    private transient AtomicLong userZgidCount;
    private transient AtomicLong zgidUserCount;
    private transient AtomicLong errorCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        deviceCount = new AtomicLong(0);
        userCount = new AtomicLong(0);
        deviceZgidCount = new AtomicLong(0);
        userZgidCount = new AtomicLong(0);
        zgidUserCount = new AtomicLong(0);
        errorCount = new AtomicLong(0);

        LOG.info("[IdArchiveRouter-{}] 初始化完成", 
                getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(IdArchiveMessage msg, Context ctx,
                               Collector<DeviceIdRow> out) throws Exception {
        if (!msg.isValid()) {
            errorCount.incrementAndGet();
            return;
        }

        ArchiveType type = msg.getArchiveType();
        if (type == null) {
            errorCount.incrementAndGet();
            LOG.warn("未知的映射类型: {}", msg.getType());
            return;
        }

        Integer appId = msg.getAppId();
        String key = msg.getKey();
        String value = msg.getValue();

        try {
            switch (type) {
                case DEVICE:
                    // deviceMd5 → zgDeviceId
                    DeviceIdRow deviceRow = new DeviceIdRow(
                            appId, key, Long.parseLong(value));
                    out.collect(deviceRow);
                    deviceCount.incrementAndGet();
                    break;

                case USER:
                    // cuid → zgUserId
                    UserIdRow userRow = new UserIdRow(
                            appId, key, Long.parseLong(value));
                    ctx.output(USER_OUTPUT, userRow);
                    userCount.incrementAndGet();
                    break;

                case DEVICE_ZGID:
                    // zgDeviceId → zgId
                    DeviceZgidRow deviceZgidRow = new DeviceZgidRow(
                            appId, Long.parseLong(key), Long.parseLong(value));
                    ctx.output(DEVICE_ZGID_OUTPUT, deviceZgidRow);
                    deviceZgidCount.incrementAndGet();
                    break;

                case USER_ZGID:
                    // zgUserId → zgId
                    UserZgidRow userZgidRow = new UserZgidRow(
                            appId, Long.parseLong(key), Long.parseLong(value));
                    ctx.output(USER_ZGID_OUTPUT, userZgidRow);
                    userZgidCount.incrementAndGet();
                    break;

                case ZGID_USER:
                    // zgId → zgUserId
                    ZgidUserRow zgidUserRow = new ZgidUserRow(
                            appId, Long.parseLong(key), Long.parseLong(value));
                    ctx.output(ZGID_USER_OUTPUT, zgidUserRow);
                    zgidUserCount.incrementAndGet();
                    break;

                default:
                    errorCount.incrementAndGet();
                    break;
            }
        } catch (NumberFormatException e) {
            errorCount.incrementAndGet();
            LOG.warn("ID 解析失败: type={}, key={}, value={}", type, key, value, e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("[IdArchiveRouter] 统计: DEVICE={}, USER={}, DEVICE_ZGID={}, " +
                        "USER_ZGID={}, ZGID_USER={}, ERROR={}",
                deviceCount.get(), userCount.get(), deviceZgidCount.get(),
                userZgidCount.get(), zgidUserCount.get(), errorCount.get());
        super.close();
    }
}
