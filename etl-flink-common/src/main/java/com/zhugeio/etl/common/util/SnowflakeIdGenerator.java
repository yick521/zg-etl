package com.zhugeio.etl.common.util;

/**
 * 雪花算法ID生成器
 *
 * 64位结构:
 * 1位符号位(0) + 41位时间戳 + 10位workerId + 12位序列号
 *
 * - 时间戳: 精确到毫秒,可用69年
 * - workerId: 支持1024个节点
 * - 序列号: 每毫秒可生成4096个ID
 */
public class SnowflakeIdGenerator {

    // 起始时间戳 (2024-01-01 00:00:00)
    private static final long EPOCH = 1704067200000L;

    // 位数分配
    private static final long WORKER_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    // 最大值
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS); // 1023
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);   // 4095

    // 位移
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = WORKER_ID_BITS + SEQUENCE_BITS;

    private final long workerId;
    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public SnowflakeIdGenerator(long workerId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException(
                    "Worker ID must be between 0 and " + MAX_WORKER_ID);
        }
        this.workerId = workerId;
    }

    /**
     * 生成唯一ID (线程安全)
     */
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();

        // 时钟回拨检测
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(
                    "Clock moved backwards. Refusing to generate id for " +
                            (lastTimestamp - timestamp) + " milliseconds");
        }

        // 同一毫秒内
        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                // 序列号用完,等待下一毫秒
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // 新的毫秒,序列号重置
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        // 组装64位ID
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    /**
     * 解析ID (用于调试)
     */
    public static IdInfo parseId(long id) {
        long timestamp = (id >> TIMESTAMP_SHIFT) + EPOCH;
        long workerId = (id >> WORKER_ID_SHIFT) & MAX_WORKER_ID;
        long sequence = id & MAX_SEQUENCE;
        return new IdInfo(timestamp, workerId, sequence);
    }

    public static class IdInfo {
        public final long timestamp;
        public final long workerId;
        public final long sequence;

        public IdInfo(long timestamp, long workerId, long sequence) {
            this.timestamp = timestamp;
            this.workerId = workerId;
            this.sequence = sequence;
        }

        @Override
        public String toString() {
            return String.format("timestamp=%d, workerId=%d, sequence=%d",
                    timestamp, workerId, sequence);
        }
    }
}