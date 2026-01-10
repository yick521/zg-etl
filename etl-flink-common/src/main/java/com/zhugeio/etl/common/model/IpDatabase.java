package com.zhugeio.etl.common.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * IPv4 数据库 (内存版本)
 * 使用简单二进制格式 (256×256 索引)
 * 
 * 对应 Scala: IP.java
 * 
 * 注意: 整个文件使用 LITTLE_ENDIAN 字节序
 */
public class IpDatabase implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IpDatabase.class);
    private static final long serialVersionUID = 1L;

    private final byte[] data;
    private transient ByteBuffer dataBuffer;
    private transient ByteBuffer indexBuffer;

    private int offset;
    private int[] index;

    public IpDatabase(byte[] data) {
        this.data = data;
        this.initialize();
    }

    /**
     * 初始化数据库
     */
    private void initialize() {
        dataBuffer = ByteBuffer.wrap(data);
        // 重要：设置为小端字节序，与文件格式一致
        dataBuffer.order(ByteOrder.LITTLE_ENDIAN);
        dataBuffer.position(0);

        offset = dataBuffer.getInt(); // indexLength
        
        // 校验 offset 合法性
        if (offset <= 4 || offset > data.length) {
            throw new IllegalArgumentException(
                "Invalid IP database format: offset=" + offset + 
                ", dataLength=" + data.length + 
                ". First 16 bytes: " + bytesToHex(data, 16));
        }

        byte[] indexBytes = new byte[offset - 4];
        dataBuffer.get(indexBytes, 0, offset - 4);

        indexBuffer = ByteBuffer.wrap(indexBytes);
        indexBuffer.order(ByteOrder.LITTLE_ENDIAN);

        index = new int[65536];
        for (int i = 0; i < 256; i++) {
            for (int j = 0; j < 256; j++) {
                index[i * 256 + j] = indexBuffer.getInt();
            }
        }

        // 保持 LITTLE_ENDIAN，不要改回 BIG_ENDIAN
        // find() 方法中的 indexBuffer 操作也需要 LITTLE_ENDIAN

        LOG.info("IPv4数据库初始化完成: offset={}, dataSize={}MB, indexSize={}", 
                 offset, data.length / 1024 / 1024, index.length);
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

            for (start = start * 9 + 262144; start < maxCompLen; start += 9) {
                tmpInt = int2long(indexBuffer.getInt(start));
                if (tmpInt >= ip2long) {
                    indexOffset = bytesToLong(b,
                            indexBuffer.get(start + 6),
                            indexBuffer.get(start + 5),
                            indexBuffer.get(start + 4));
                    indexLength = ((0xFF & indexBuffer.get(start + 7)) << 8)
                            + (0xFF & indexBuffer.get(start + 8));
                    break;
                }
            }

            if (indexOffset == -1 || indexLength == -1) {
                return new String[]{"", "", ""};
            }

            byte[] areaBytes = new byte[indexLength];
            synchronized (dataBuffer) {
                dataBuffer.position(offset + (int) indexOffset - 262144);
                dataBuffer.get(areaBytes, 0, indexLength);
            }

            String result = new String(areaBytes, StandardCharsets.UTF_8);
            return result.split("\t", -1);

        } catch (Exception e) {
            LOG.error("IPv4查询异常: {}", ip, e);
            return new String[]{"", "", ""};
        }
    }

    // ========== 辅助方法 ==========

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
     * 调试用：将字节数组转为十六进制字符串
     */
    private static String bytesToHex(byte[] bytes, int length) {
        StringBuilder sb = new StringBuilder();
        int len = Math.min(bytes.length, length);
        for (int i = 0; i < len; i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        return sb.toString().trim();
    }
}
