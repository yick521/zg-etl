package com.zhugeio.etl.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 雪花算法ID生成器
 *
 * 64位结构:
 * 1位符号位(0) + 41位时间戳 + 10位workerId + 12位序列号
 *
 * - 时间戳: 精确到毫秒,可用69年
 * - workerId: 支持1024个节点
 * - 序列号: 每毫秒可生成4096个ID
 * 
 * 时钟回拨处理策略:
 * - 小于5ms的回拨: 等待恢复
 * - 大于5ms的回拨: 记录警告并等待更长时间
 * - 超过最大容忍时间(100ms): 抛出异常
 */
public class SnowflakeIdGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SnowflakeIdGenerator.class);

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

    // 时钟回拨配置
    private static final long SMALL_CLOCK_BACKWARDS_MS = 5L;     // 小回拨阈值
    private static final long MAX_CLOCK_BACKWARDS_MS = 100L;     // 最大容忍回拨时间

    private final long workerId;
    private long lastTimestamp = -1L;
    private long sequence = 0L;
    
    // 统计
    private long clockBackwardsCount = 0L;
    private long clockBackwardsTotalMs = 0L;

    public SnowflakeIdGenerator(long workerId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException(
                    "Worker ID must be between 0 and " + MAX_WORKER_ID);
        }
        this.workerId = workerId;
        LOG.info("SnowflakeIdGenerator 初始化, workerId={}", workerId);
    }

    /**
     * 生成唯一ID (线程安全)
     * 
     * 时钟回拨处理:
     * 1. 检测到回拨时，先尝试等待恢复
     * 2. 小于5ms的回拨直接等待
     * 3. 大于5ms小于100ms的回拨，记录警告并等待
     * 4. 超过100ms的回拨，抛出异常
     */
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();

        // 时钟回拨检测和处理
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            
            clockBackwardsCount++;
            clockBackwardsTotalMs += offset;
            
            if (offset <= SMALL_CLOCK_BACKWARDS_MS) {
                // 小回拨: 静默等待
                LOG.debug("检测到小时钟回拨 {}ms，等待恢复", offset);
                timestamp = waitForClockRecovery(lastTimestamp, offset);
            } else if (offset <= MAX_CLOCK_BACKWARDS_MS) {
                // 中等回拨: 记录警告并等待
                LOG.warn("检测到时钟回拨 {}ms，等待恢复 (累计回拨次数: {})", 
                        offset, clockBackwardsCount);
                timestamp = waitForClockRecovery(lastTimestamp, offset);
            } else {
                // 大回拨: 抛出异常
                String msg = String.format(
                        "时钟回拨过大，拒绝生成ID。回拨时间: %dms, 最大容忍: %dms, 累计回拨次数: %d",
                        offset, MAX_CLOCK_BACKWARDS_MS, clockBackwardsCount);
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
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

    /**
     * 等待时钟恢复
     * 
     * @param lastTimestamp 上次时间戳
     * @param offset        回拨偏移量
     * @return 恢复后的时间戳
     */
    private long waitForClockRecovery(long lastTimestamp, long offset) {
        long startWait = System.currentTimeMillis();
        long maxWaitTime = offset + 10; // 额外等待10ms缓冲
        
        try {
            // 先 sleep 大部分时间
            Thread.sleep(offset);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("时钟回拨等待被中断");
        }
        
        // 自旋等待确保恢复
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            if (System.currentTimeMillis() - startWait > maxWaitTime) {
                throw new RuntimeException(
                        "等待时钟恢复超时，可能存在严重的时钟问题");
            }
            
            // 短暂让出CPU
            Thread.yield();
            timestamp = System.currentTimeMillis();
        }
        
        long waitedTime = System.currentTimeMillis() - startWait;
        LOG.debug("时钟回拨恢复，实际等待 {}ms", waitedTime);
        
        return timestamp;
    }

    /**
     * 等待下一毫秒
     */
    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    /**
     * 获取统计信息
     */
    public String getStats() {
        return String.format("workerId=%d, clockBackwardsCount=%d, clockBackwardsTotalMs=%d",
                workerId, clockBackwardsCount, clockBackwardsTotalMs);
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

    public long getWorkerId() {
        return workerId;
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
            return String.format("timestamp=%d (%tF %<tT), workerId=%d, sequence=%d",
                    timestamp, timestamp, workerId, sequence);
        }
    }
}
