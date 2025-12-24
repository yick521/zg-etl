package com.zhugeio.etl.pipeline.archive;

import com.alibaba.fastjson.JSONObject;
import com.zhugeio.etl.common.config.Config;
import com.zhugeio.etl.pipeline.enums.ArchiveType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * kvrocks 映射数据输出 Kafka 服务 - 优化版
 *
 * id模块工作过程会产生如下映射数据，需输出到 Kafka
 * dwd_id_device: deviceMd5 → zgDeviceId
 * dwd_id_user: cuid → zgUserId
 * dwd_id_device_zgid: zgDeviceId → zgId
 * dwd_id_user_zgid: zgUserId → zgId
 * dwd_id_zgid_user: zgId → zgUserId
 *
 *  设计思路:
 * 1. 静态单例 - 一个 TaskManager (JVM) 只有一个实例
 * 2. 所有 Task/线程共享，计数真正聚合到一起
 * 3. 错误日志直接发送，依赖 Kafka Producer 自身批量机制
 * 4. 计数使用 AtomicLong 线程安全累加，定期批量发送
 */
public class ArchiveKafkaService implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveKafkaService.class);

    //  静态单例
    private static volatile ArchiveKafkaService INSTANCE;
    private static final Object LOCK = new Object();

    // 配置
    private String kafkaBrokers;
    private String kafkaTopic;
    private boolean enabled;
    private volatile boolean initialized = false;

    private transient KafkaProducer<String, String> producer;

    private transient volatile boolean closed = false;

    private ArchiveKafkaService() {}

    /**
     *  获取单例
     */
    public static ArchiveKafkaService getInstance() {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = new ArchiveKafkaService();
                    INSTANCE.init(
                            Config.getString(Config.ID_ARCHIVE_ENABLED),
                            Config.getString(Config.KAFKA_ID_ARCHIVE_TOPIC),
                            Config.getBoolean(Config.ID_ARCHIVE_BROKERS, true)
                    );
                }
            }
        }
        return INSTANCE;
    }

    /**
     *  获取单例 (自定义配置)
     */
    public static ArchiveKafkaService getInstance(String kafkaBrokers, String kafkaTopic, boolean enabled) {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = new ArchiveKafkaService();
                    INSTANCE.init(kafkaBrokers, kafkaTopic, enabled);
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 初始化
     */
    public void init(String kafkaBrokers, String kafkaTopic, boolean enabled) {
        if (initialized) {
            return;
        }
        
        synchronized (LOCK) {
            if (initialized) {
                return;
            }

            this.kafkaBrokers = kafkaBrokers;
            this.kafkaTopic = kafkaTopic;
            this.enabled = enabled;

            if (!enabled) {
                LOG.info("[ArchiveKafkaService] 已禁用");
                initialized = true;
                return;
            }

            try {
                closed = false;

                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ACKS_CONFIG, "0");
                props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
                props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
                props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
                props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
                props.put(ProducerConfig.CLIENT_ID_CONFIG, "Archive-producer-" + System.currentTimeMillis());

                producer = new KafkaProducer<>(props);
                initialized = true;
                
                LOG.info("[ArchiveKafkaService] 初始化成功 (TaskManager级单例), brokers={}, topic={}",
                        kafkaBrokers, kafkaTopic);
                        
            } catch (Exception e) {
                LOG.error("[ArchiveKafkaService] 初始化失败", e);
                throw new RuntimeException("ArchiveKafkaService 初始化失败", e);
            }
        }
    }



    // ==================== 工具方法 ====================

    /**
     * 带类型发送
     * @param type
     * @param appId
     * @param key
     * @param value
     */
    public void sendToKafka(ArchiveType type, Integer appId,String key,Long value) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", type.name());
        jsonObject.put("appId", appId);
        jsonObject.put("key", key);
        jsonObject.put("value", value);
        sendToKafka(jsonObject.toJSONString());
    }

    public void sendToKafka(String message) {
        if (producer != null && !closed) {
            producer.send(new ProducerRecord<>(kafkaTopic,  message), (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn("[Archive] Kafka 发送失败: {}", exception.getMessage());
                }
            });
        }
    }


    /**
     *  关闭
     */
    public void close() {
        if (!enabled || closed) {
            return;
        }

        closed = true;

        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                LOG.info("[DataQualityKafkaService] 已关闭");
            } catch (Exception e) {
                LOG.warn("[DataQualityKafkaService] 关闭失败: {}", e.getMessage());
            }
        }
        
        initialized = false;
        INSTANCE = null;
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (INSTANCE != null) {
                LOG.info("JVM 关闭，清理 ArchiveKafkaService...");
                INSTANCE.close();
            }
        }, "dq-shutdown-hook"));
    }
}
