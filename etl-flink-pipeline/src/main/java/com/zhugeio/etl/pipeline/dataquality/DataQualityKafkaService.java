package com.zhugeio.etl.pipeline.dataquality;

import com.alibaba.fastjson.JSONObject;
import com.zhugeio.etl.common.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据质量 Kafka 服务 - 优化版
 * 
 *  设计思路:
 * 1. 静态单例 - 一个 TaskManager (JVM) 只有一个实例
 * 2. 所有 Task/线程共享，计数真正聚合到一起
 * 3. 错误日志直接发送，依赖 Kafka Producer 自身批量机制
 * 4. 计数使用 AtomicLong 线程安全累加，定期批量发送
 */
public class DataQualityKafkaService implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataQualityKafkaService.class);

    // 消息类型常量
    public static final String DATA_COUNT_TYPE = "data-count";
    public static final String ERROR_LOG_TYPE = "error-log";
    public static final String SUCCESS_COUNT_KEY = "success_count";
    public static final String ERROR_COUNT_KEY = "error_count";

    // 计数刷新间隔
    private static final long COUNT_FLUSH_INTERVAL_MS = 5000;

    //  静态单例
    private static volatile DataQualityKafkaService INSTANCE;
    private static final Object LOCK = new Object();
    
    // 配置
    private String kafkaBrokers;
    private String kafkaTopic;
    private boolean enabled;
    private volatile boolean initialized = false;

    private transient KafkaProducer<String, String> producer;
    
    //  计数聚合缓存 (线程安全)
    private transient ConcurrentHashMap<String, AtomicLong> countCache;
    
    private transient ScheduledExecutorService flushScheduler;
    private transient volatile boolean closed = false;

    private static final ThreadLocal<SimpleDateFormat> DAY_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyyMMdd"));

    private DataQualityKafkaService() {}

    /**
     *  获取单例
     */
    public static DataQualityKafkaService getInstance() {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = new DataQualityKafkaService();
                    INSTANCE.init(
                            Config.getString(Config.DQ_KAFKA_BROKERS),
                            Config.getString(Config.DQ_KAFKA_TOPIC),
                            Config.getBoolean(Config.DQ_ENABLED, true)
                    );
                }
            }
        }
        return INSTANCE;
    }

    /**
     *  获取单例 (自定义配置)
     */
    public static DataQualityKafkaService getInstance(String kafkaBrokers, String kafkaTopic, boolean enabled) {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = new DataQualityKafkaService();
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
                LOG.info("[DataQualityKafkaService] 已禁用");
                initialized = true;
                return;
            }

            try {
                closed = false;
                countCache = new ConcurrentHashMap<>();

                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ACKS_CONFIG, "0");
                props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
                props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
                props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
                props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
                props.put(ProducerConfig.CLIENT_ID_CONFIG, "dq-producer-" + System.currentTimeMillis());

                producer = new KafkaProducer<>(props);
                startCountFlushScheduler();
                initialized = true;
                
                LOG.info("[DataQualityKafkaService] 初始化成功 (TaskManager级单例), brokers={}, topic={}", 
                        kafkaBrokers, kafkaTopic);
                        
            } catch (Exception e) {
                LOG.error("[DataQualityKafkaService] 初始化失败", e);
                throw new RuntimeException("DataQualityKafkaService 初始化失败", e);
            }
        }
    }

    private void startCountFlushScheduler() {
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dq-count-flush");
            t.setDaemon(true);
            return t;
        });
        
        flushScheduler.scheduleAtFixedRate(() -> {
            try {
                flushCounts();
            } catch (Exception e) {
                LOG.warn("[DataQuality] 定时刷新计数异常", e);
            }
        }, COUNT_FLUSH_INTERVAL_MS, COUNT_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // ==================== 错误日志 ====================

    public void sendErrorLog(ErrorMessageEnum errorEnum,
                             Integer appId,
                             Integer platform,
                             String eventName,
                             String dataJson,
                             Long ct,
                             String pl,
                             String sdk) {
        if (!enabled || closed || producer == null) {
            return;
        }

        try {
            String currentDay = DAY_FORMAT.get().format(new Date());

            JSONObject errorMsgJson = new JSONObject();
            JSONObject dataJsonObj = new JSONObject();

            dataJsonObj.put("app_id", appId);
            dataJsonObj.put("error_code", errorEnum.getErrorCode());
            dataJsonObj.put("data_json", dataJson);
            dataJsonObj.put("data_md5", getMD5(dataJson));
            dataJsonObj.put("error_md5", getMD5(errorEnum.getErrorMessage()));
            dataJsonObj.put("log_utc_date", System.currentTimeMillis());
            dataJsonObj.put("log_utc_day_id", currentDay);
            dataJsonObj.put("event_begin_date", ct != null ? String.valueOf(ct) : "0");
            dataJsonObj.put("pl", pl != null ? pl : "");
            dataJsonObj.put("sdk", sdk != null ? sdk : "");
            dataJsonObj.put("platform", platform);
            dataJsonObj.put("pro_flag", 0);
            dataJsonObj.put("event_name", eventName != null ? eventName : "\\N");
            dataJsonObj.put("error_msg", errorEnum.getErrorMessage());

            errorMsgJson.put("data", dataJsonObj);
            errorMsgJson.put("type", ERROR_LOG_TYPE);

            sendToKafka(String.valueOf(appId), errorMsgJson.toJSONString());
            recordErrorCount(appId, platform, eventName);

        } catch (Exception e) {
            LOG.warn("[DataQuality] 发送错误日志失败: {}", e.getMessage());
        }
    }

    // ==================== 计数聚合 ====================

    public void recordErrorCount(Integer appId, Integer platform, String eventName) {
        if (!enabled || countCache == null) {
            return;
        }

        String currentDay = DAY_FORMAT.get().format(new Date());
        String key = ERROR_COUNT_KEY + "#" + appId + ":" + currentDay + ":" + platform + ":" +
                (eventName != null ? eventName : "\\N");
        
        countCache.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    public void recordSuccessCount(String tableName, long count) {
        if (!enabled || countCache == null || count <= 0) {
            return;
        }

        String currentDay = DAY_FORMAT.get().format(new Date());
        String key = SUCCESS_COUNT_KEY + "#" + tableName + ":" + currentDay;
        
        countCache.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(count);
    }

    public void recordSuccessCount(Integer appId, Integer platform, String eventName, long count) {
        if (!enabled || countCache == null || count <= 0) {
            return;
        }
        if (eventName == null || "\\N".equals(eventName)) {
            return;
        }

        String currentDay = DAY_FORMAT.get().format(new Date());
        String key = SUCCESS_COUNT_KEY + "#" + appId + ":" + currentDay + ":" + platform + ":" + eventName;
        
        countCache.computeIfAbsent(key, k -> new AtomicLong(0)).addAndGet(count);
    }

    // ==================== 刷新计数 ====================

    public void flushCounts() {
        if (!enabled || producer == null || closed || countCache == null || countCache.isEmpty()) {
            return;
        }

        try {
            Map<String, Long> snapshot = new HashMap<>();
            Iterator<Map.Entry<String, AtomicLong>> iterator = countCache.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, AtomicLong> entry = iterator.next();
                long value = entry.getValue().getAndSet(0);
                if (value > 0) {
                    snapshot.put(entry.getKey(), value);
                }
                if (entry.getValue().get() == 0) {
                    iterator.remove();
                }
            }

            if (snapshot.isEmpty()) {
                return;
            }

            JSONObject countJson = new JSONObject();
            for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
                countJson.put(entry.getKey(), entry.getValue());
            }

            JSONObject allCountJson = new JSONObject();
            allCountJson.put("data", countJson);
            allCountJson.put("type", DATA_COUNT_TYPE);

            sendToKafka(null, allCountJson.toJSONString());
            LOG.info("[DataQuality] 刷新聚合计数: {} 个维度", snapshot.size());

        } catch (Exception e) {
            LOG.warn("[DataQuality] 刷新计数失败: {}", e.getMessage());
        }
    }

    public void flushSuccessCounts() {
        if (!enabled || producer == null || closed || countCache == null) {
            return;
        }

        try {
            Map<String, Long> snapshot = new HashMap<>();
            
            for (Map.Entry<String, AtomicLong> entry : countCache.entrySet()) {
                if (entry.getKey().startsWith(SUCCESS_COUNT_KEY)) {
                    long value = entry.getValue().getAndSet(0);
                    if (value > 0) {
                        snapshot.put(entry.getKey(), value);
                    }
                }
            }

            if (snapshot.isEmpty()) {
                return;
            }

            JSONObject countJson = new JSONObject();
            for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
                countJson.put(entry.getKey(), entry.getValue());
            }

            JSONObject allCountJson = new JSONObject();
            allCountJson.put("data", countJson);
            allCountJson.put("type", DATA_COUNT_TYPE);

            sendToKafka(null, allCountJson.toJSONString());
            
        } catch (Exception e) {
            LOG.warn("[DataQuality] 刷新成功计数失败: {}", e.getMessage());
        }
    }

    public void flushAll() {
        if (!enabled || producer == null || closed) {
            return;
        }

        try {
            flushCounts();
            producer.flush();
        } catch (Exception e) {
            LOG.warn("[DataQuality] flushAll 异常", e);
        }
    }

    // ==================== 工具方法 ====================

    private void sendToKafka(String key, String message) {
        if (producer != null && !closed) {
            producer.send(new ProducerRecord<>(kafkaTopic, key, message), (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn("[DataQuality] Kafka 发送失败: {}", exception.getMessage());
                }
            });
        }
    }

    private String getMD5(String str) {
        if (str == null) {
            return "";
        }
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            md5.update(bytes, 0, bytes.length);
            return new BigInteger(1, md5.digest()).toString(16).toLowerCase();
        } catch (Exception e) {
            return "";
        }
    }

    public void close() {
        if (!enabled || closed) {
            return;
        }

        closed = true;

        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                flushScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                flushScheduler.shutdownNow();
            }
        }

        if (producer != null) {
            try {
                flushAll();
                producer.close();
                LOG.info("[DataQualityKafkaService] 已关闭");
            } catch (Exception e) {
                LOG.warn("[DataQualityKafkaService] 关闭失败: {}", e.getMessage());
            }
        }
        
        initialized = false;
        INSTANCE = null;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public int getCountCacheSize() {
        return countCache != null ? countCache.size() : 0;
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (INSTANCE != null) {
                LOG.info("JVM 关闭，清理 DataQualityKafkaService...");
                INSTANCE.close();
            }
        }, "dq-shutdown-hook"));
    }
}
