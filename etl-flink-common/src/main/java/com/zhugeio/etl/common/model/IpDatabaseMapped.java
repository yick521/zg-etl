package com.zhugeio.etl.common.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * IPv4 数据库 (内存映射版本)
 * 使用简单二进制格式 (256×256 索引)
 * 
 * 【核心优化】
 * - 数据使用 MappedByteBuffer，不占用 JVM 堆内存
 * - 只有 index[] 数组在堆内存（256KB）
 * - 100MB IP库：堆内存从 512MB 降到 ~256KB
 * 
 * 完全兼容原 IpDatabase 的文件格式和接口
 */
public class IpDatabaseMapped implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseMapped.class);
    private static final long serialVersionUID = 1L;

    // 内存映射缓冲区（不占堆内存）
    private final MappedByteBuffer dataBuffer;
    
    // 索引数据（堆内存，256KB）
    private final int[] index;
    
    // 数据偏移量
    private final int offset;
    
    // 文件大小
    private final int fileSize;

    /**
     * 构造函数
     * 
     * @param mappedBuffer 内存映射的 ByteBuffer
     */
    public IpDatabaseMapped(MappedByteBuffer mappedBuffer) {
        this.dataBuffer = mappedBuffer;
        this.fileSize = mappedBuffer.capacity();
        
        // 设置字节序为小端（与原格式一致）
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN);
        dataBuffer.position(0);

        // 读取 offset（索引长度）
        this.offset = dataBuffer.getInt();
        
        // 校验 offset 合法性
        if (offset <= 4 || offset > fileSize) {
            throw new IllegalArgumentException(
                "Invalid IP database format: offset=" + offset + 
                ", fileSize=" + fileSize);
        }

        // 读取索引到堆内存（256KB，这是唯一占用堆内存的部分）
        this.index = new int[65536];
        for (int i = 0; i < 65536; i++) {
            index[i] = dataBuffer.getInt();
        }

        LOG.info("IpDatabaseMapped 初始化完成: offset={}, fileSize={} MB, 堆内存占用≈256KB",
                 offset, fileSize / 1024 / 1024);
    }

    /**
     * 查找IP地址
     * 
     * @return String[] = [country, province, city] 或 [country, province, city, isp]
     */
    public String[] find(String ip) {
        if (ip == null || ip.isEmpty()) {
            return new String[]{"", "", ""};
        }

        String[] parts = ip.split("\\.");
        if (parts.length != 4) {
            return new String[]{"", "", ""};
        }

        try {
            int prefix = (Integer.parseInt(parts[0]) * 256 + Integer.parseInt(parts[1]));
            long ip2long = ip2long(ip);
            int start = index[prefix];
            int maxCompLen = offset - 262144 - 4;

            long tmpInt;
            long indexOffset = -1;
            int indexLength = -1;
            byte b = 0;

            // 在索引区搜索（使用同步访问 dataBuffer）
            synchronized (dataBuffer) {
                for (start = start * 9 + 262144; start < maxCompLen; start += 9) {
                    dataBuffer.position(start);
                    tmpInt = int2long(dataBuffer.getInt());
                    
                    if (tmpInt >= ip2long) {
                        dataBuffer.position(start + 4);
                        byte b4 = dataBuffer.get();
                        byte b5 = dataBuffer.get();
                        byte b6 = dataBuffer.get();
                        byte b7 = dataBuffer.get();
                        byte b8 = dataBuffer.get();
                        
                        indexOffset = bytesToLong(b, b6, b5, b4);
                        indexLength = ((0xFF & b7) << 8) + (0xFF & b8);
                        break;
                    }
                }

                if (indexOffset == -1 || indexLength == -1) {
                    return new String[]{"", "", ""};
                }

                // 读取数据区
                byte[] areaBytes = new byte[indexLength];
                dataBuffer.position(offset + (int) indexOffset - 262144);
                dataBuffer.get(areaBytes, 0, indexLength);
                
                String result = new String(areaBytes, StandardCharsets.UTF_8);
                return result.split("\t", -1);
            }

        } catch (Exception e) {
            LOG.debug("IPv4查询异常: {}", ip, e);
            return new String[]{"", "", ""};
        }
    }

    // ========== 辅助方法（与原版本完全一致）==========

    private long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private int str2Ip(String ip) {
        String[] ss = ip.split("\\.");
        int a = Integer.parseInt(ss[0]);
        int b = Integer.parseInt(ss[1]);
        int c = Integer.parseInt(ss[2]);
        int d = Integer.parseInt(ss[3]);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private long ip2long(String ip) {
        return int2long(str2Ip(ip));
    }

    private long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }

    /**
     * 获取文件大小
     */
    public int getFileSize() {
        return fileSize;
    }
}
