package com.zhugeio.etl.common.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Source 构建器
 *
 * 无业务依赖，可跨项目复用
 */
public class KafkaSourceBuilder<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBuilder.class);

    private String brokers;
    private String topic;
    private String groupId;
    private Properties consumerProps;
    private DeserializationSchema<T> valueDeserializer;
    private KafkaRecordDeserializationSchema<T> recordDeserializer;
    private OffsetsInitializer offsetsInitializer;

    private KafkaSourceBuilder() {
        this.consumerProps = defaultConsumerProps();
        this.offsetsInitializer = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
    }

    public static <T> KafkaSourceBuilder<T> builder() {
        return new KafkaSourceBuilder<>();
    }

    // ========== 必填参数 ==========

    public KafkaSourceBuilder<T> brokers(String brokers) {
        this.brokers = brokers;
        return this;
    }

    public KafkaSourceBuilder<T> topic(String topic) {
        this.topic = topic;
        return this;
    }

    public KafkaSourceBuilder<T> groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    /**
     * 简单反序列化器 (只需要 value)
     */
    public KafkaSourceBuilder<T> deserializer(DeserializationSchema<T> deserializer) {
        this.valueDeserializer = deserializer;
        this.recordDeserializer = null;
        return this;
    }

    /**
     * 完整反序列化器 (需要访问 topic/partition/offset 等元数据)
     */
    public KafkaSourceBuilder<T> deserializer(KafkaRecordDeserializationSchema<T> deserializer) {
        this.recordDeserializer = deserializer;
        this.valueDeserializer = null;
        return this;
    }

    // ========== 可选参数 ==========

    public KafkaSourceBuilder<T> consumerProps(Properties props) {
        this.consumerProps = props;
        return this;
    }

    public KafkaSourceBuilder<T> addConsumerProp(String key, String value) {
        this.consumerProps.setProperty(key, value);
        return this;
    }

    public KafkaSourceBuilder<T> startFromEarliest() {
        this.offsetsInitializer = OffsetsInitializer.earliest();
        return this;
    }

    public KafkaSourceBuilder<T> startFromLatest() {
        this.offsetsInitializer = OffsetsInitializer.latest();
        return this;
    }

    public KafkaSourceBuilder<T> startFromCommitted() {
        this.offsetsInitializer = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
        return this;
    }

    public KafkaSourceBuilder<T> startFromCommitted(OffsetResetStrategy resetStrategy) {
        this.offsetsInitializer = OffsetsInitializer.committedOffsets(resetStrategy);
        return this;
    }

    public KafkaSourceBuilder<T> startFromTimestamp(long timestamp) {
        this.offsetsInitializer = OffsetsInitializer.timestamp(timestamp);
        return this;
    }

    // ========== 构建 ==========

    public KafkaSource<T> build() {
        validate();

        LOG.info("构建 Kafka Source: brokers={}, topic={}, groupId={}", brokers, topic, groupId);

        org.apache.flink.connector.kafka.source.KafkaSourceBuilder<T> builder =
                KafkaSource.<T>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(topic)
                        .setGroupId(groupId)
                        .setStartingOffsets(offsetsInitializer)
                        .setProperties(consumerProps);

        // 根据类型选择反序列化方式
        if (recordDeserializer != null) {
            builder.setDeserializer(recordDeserializer);
        } else if (valueDeserializer != null) {
            builder.setValueOnlyDeserializer(valueDeserializer);
        }

        return builder.build();
    }

    private void validate() {
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("brokers is required");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("groupId is required");
        }
        if (valueDeserializer == null && recordDeserializer == null) {
            throw new IllegalArgumentException("deserializer is required");
        }
    }

    // ========== 静态工厂方法 ==========

    /**
     * 快速构建 String 类型 Source
     */
    public static KafkaSource<String> buildStringSource(String brokers, String topic, String groupId) {
        return KafkaSourceBuilder.<String>builder()
                .brokers(brokers)
                .topic(topic)
                .groupId(groupId)
                .deserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * 快速构建 String 类型 Source (带自定义配置)
     */
    public static KafkaSource<String> buildStringSource(
            String brokers, String topic, String groupId, Properties consumerProps) {
        return KafkaSourceBuilder.<String>builder()
                .brokers(brokers)
                .topic(topic)
                .groupId(groupId)
                .consumerProps(consumerProps)
                .deserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * 默认 Consumer 配置
     */
    public static Properties defaultConsumerProps() {
        Properties props = new Properties();
        props.setProperty("max.partition.fetch.bytes", "52428800");   // 50MB
        props.setProperty("max.poll.records", "1000000");
        props.setProperty("fetch.max.wait.ms", "1000");
        return props;
    }

    /**
     * 高吞吐 Consumer 配置
     */
    public static Properties highThroughputConsumerProps() {
        Properties props = new Properties();
        props.setProperty("max.partition.fetch.bytes", "104857600");  // 100MB
        props.setProperty("max.poll.records", "2000000");
        props.setProperty("fetch.max.wait.ms", "500");
        props.setProperty("fetch.min.bytes", "1048576");              // 1MB
        return props;
    }

    /**
     * 低延迟 Consumer 配置
     */
    public static Properties lowLatencyConsumerProps() {
        Properties props = new Properties();
        props.setProperty("max.partition.fetch.bytes", "10485760");   // 10MB
        props.setProperty("max.poll.records", "100000");
        props.setProperty("fetch.max.wait.ms", "100");
        props.setProperty("fetch.min.bytes", "1");
        return props;
    }
}