package com.zhugeio.etl.pipeline.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhugeio.etl.pipeline.entity.IdMappingMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * ID 映射消息反序列化器
 */
public class IdMappingMessageSchema implements DeserializationSchema<IdMappingMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IdMappingMessageSchema.class);

    private transient ObjectMapper objectMapper;

    @Override
    public IdMappingMessage deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }

        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }

        try {
            return objectMapper.readValue(message, IdMappingMessage.class);
        } catch (Exception e) {
            LOG.warn("反序列化 IdMappingMessage 失败: {}", 
                    new String(message, StandardCharsets.UTF_8), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(IdMappingMessage nextElement) {
        return false;
    }

    @Override
    public TypeInformation<IdMappingMessage> getProducedType() {
        return TypeInformation.of(IdMappingMessage.class);
    }
}
