package com.zhugeio.etl.pipeline.kafka;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.common.util.Dims;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka 消息反序列化
 */
public class ZGMsgSchema implements KafkaRecordDeserializationSchema<ZGMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ZGMsgSchema.class);

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<ZGMessage> out) {

        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();
        String rawData = new String(record.value(), StandardCharsets.UTF_8);
//        LOG.debug("deserialize : offset={}, topic={}, partition={}, rawData={}", record.offset(), record.topic(), record.partition(),rawData);

        ZGMessage message = new ZGMessage();
        message.setTopic(topic);
        message.setPartition(partition);
        message.setOffset(offset);
        message.setRawData(rawData);

        // 解析 JSON 并填充 data 字段
        try {
            JSONObject json = JSON.parseObject(rawData);
            if (json != null) {
                // 转换为 Map<String, Object>
                Map<String, Object> dataMap = new HashMap<>(json);
                message.setData(dataMap);

                // 提取常用字段
                message.setAppId(json.getInteger("app_id"));
                message.setAppKey(json.getString("ak"));
                message.setBusiness(json.getString("business"));

                // 提取 SDK 类型
                String sdk = json.getString("sdk");
                if (sdk != null) {
                    message.setSdk(getSdkType(sdk));
                }
            }
        } catch (Exception e) {
            LOG.warn("JSON 解析失败: offset={}, error={}", offset, e.getMessage());
            message.setError(e.getMessage());
            message.setErrorCode(1);
        }

        out.collect(message);
    }

    /**
     * 获取 SDK 类型编号
     */
    private Integer getSdkType(String sdk) {
        if (sdk == null) {
            return 0;
        }
        return Dims.sdk(sdk);
    }

    @Override
    public TypeInformation<ZGMessage> getProducedType() {
        return TypeInformation.of(ZGMessage.class);
    }
}