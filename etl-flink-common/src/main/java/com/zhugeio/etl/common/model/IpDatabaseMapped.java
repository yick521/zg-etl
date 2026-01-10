package com.zhugeio.etl.common.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 内存映射版本的 AWDB IP 数据库读取器
 * 基于 AWReader.java 改写，使用 MappedByteBuffer 减少堆内存占用
 */
public class IpDatabaseMapped implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseMapped.class);

    private static final int DATA_SECTION_SEPARATOR_SIZE = 16;
    private static final byte[] METADATA_START_MARKER = {
            (byte) 0xAB, (byte) 0xCD, (byte) 0xEF,
            'i', 'p', 'p', 'l', 'u', 's', '3', '6', '0', '.', 'c', 'o', 'm'
    };

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final File file;
    private final RandomAccessFile raf;
    private final MappedByteBuffer buffer;
    private final int fileSize;

    // 元数据
    private final int ipVersion;
    private final int nodeCount;
    private final int recordSize;
    private final int nodeByteSize;
    private final int searchTreeSize;

    // IPv4 起始节点（用于 IPv6 数据库中查找 IPv4）
    private final int ipV4Start;

    /**
     * 从文件构造 AWDB 读取器
     */
    public IpDatabaseMapped(File file) throws IOException {
        this.file = file;
        this.fileSize = (int) file.length();
        this.raf = new RandomAccessFile(file, "r");

        FileChannel channel = raf.getChannel();
        this.buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

        LOG.info("AWDB IP库文件: {}, 大小: {} bytes ({} MB)",
                file.getName(), fileSize, fileSize / 1024 / 1024);

        // 查找并解析元数据
        int metadataStart = findMetadataStart();
        JsonNode metadata = decodeMetadata(metadataStart);

        this.ipVersion = metadata.get("ip_version").asInt();
        this.nodeCount = metadata.get("node_count").asInt();
        this.recordSize = metadata.get("record_size").asInt();
        this.nodeByteSize = this.recordSize / 4;
        this.searchTreeSize = this.nodeCount * this.nodeByteSize;

        LOG.info("AWDB 元数据: ipVersion={}, nodeCount={}, recordSize={}, searchTreeSize={}",
                ipVersion, nodeCount, recordSize, searchTreeSize);

        // 查找 IPv4 起始节点
        this.ipV4Start = findIpV4StartNode();

        LOG.info("AWDB IP库加载完成: ipV4Start={}", ipV4Start);
    }

    /**
     * 查找 IP 地址的地理位置
     * @param ip IP 地址字符串
     * @return [国家, 省份, 城市]
     */
    public String[] find(String ip) {
        LOG.debug("[AWDB] 开始查询 IP: {}", ip);
        try {
            InetAddress ipAddress = InetAddress.getByName(ip);
            LOG.debug("[AWDB] IP {} 解析成功, 类型: {}", ip, ipAddress.getClass().getSimpleName());
            return get(ipAddress);
        } catch (Exception e) {
            LOG.debug("[AWDB] IP {} 查询失败: {}", ip, e.getMessage(), e);
            return new String[]{"", "", ""};
        }
    }

    /**
     * 查找 IP 地址的地理位置
     */
    public String[] get(InetAddress ipAddress) throws IOException {
        String country = "";
        String prov = "";
        String city = "";

        LOG.debug("[AWDB] get() 开始, IP: {}, 是否IPv4: {}",
                ipAddress.getHostAddress(), ipAddress instanceof Inet4Address);

        if (ipAddress instanceof Inet4Address) {
            JsonNode value = getRecordData(ipAddress);
            LOG.debug("[AWDB] getRecordData 返回: {}", value);

            if (value != null) {
                JsonNode countryNode = value.get("country");
                JsonNode multiAreas = value.get("multiAreas");

                LOG.debug("[AWDB] countryNode: {}, multiAreas: {}", countryNode, multiAreas);

                if (countryNode != null) {
                    country = countryNode.asText("").trim();
                }
                if (multiAreas != null && multiAreas.size() > 0) {
                    JsonNode area = multiAreas.get(0);
                    JsonNode provNode = area.get("prov");
                    JsonNode cityNode = area.get("city");
                    if (provNode != null) {
                        prov = provNode.asText("").trim();
                    }
                    if (cityNode != null) {
                        city = cityNode.asText("").trim();
                    }
                }

                LOG.info("[AWDB] IP {} 查询成功: country={}, prov={}, city={}",
                        ipAddress.getHostAddress(), country, prov, city);
            } else {
                LOG.warn("[AWDB] IP {} 未找到记录数据", ipAddress.getHostAddress());
            }
        } else {
            LOG.debug("[AWDB] 非 IPv4 地址，跳过: {}", ipAddress.getHostAddress());
        }

        return new String[]{country, prov, city};
    }

    /**
     * 获取 IP 地址对应的记录数据
     */
    private JsonNode getRecordData(InetAddress ipAddress) throws IOException {
        byte[] rawAddress = ipAddress.getAddress();
        int bitLength = rawAddress.length * 8;
        int record = startNode(bitLength);

        LOG.debug("[AWDB] getRecordData: IP={}, bitLength={}, startRecord={}, nodeCount={}",
                ipAddress.getHostAddress(), bitLength, record, nodeCount);

        synchronized (buffer) {
            ByteBuffer buf = buffer.duplicate();

            int iterations = 0;
            for (int pl = 0; pl < bitLength && record < nodeCount; pl++) {
                int b = 0xFF & rawAddress[pl / 8];
                int bit = 1 & (b >> 7 - (pl % 8));
                int oldRecord = record;
                record = readNode(buf, record, bit);
                iterations++;

                // 只打印前几次迭代
                if (iterations <= 5) {
                    LOG.debug("[AWDB] 遍历树: pl={}, bit={}, oldRecord={} -> newRecord={}",
                            pl, bit, oldRecord, record);
                }
            }

            LOG.debug("[AWDB] 遍历完成: iterations={}, finalRecord={}, nodeCount={}, isDataPointer={}",
                    iterations, record, nodeCount, record > nodeCount);

            if (record > nodeCount) {
                // record 是数据指针
                LOG.debug("[AWDB] 找到数据指针: {}, 开始解析数据...", record);
                return resolveDataPointer(buf, record);
            } else if (record == nodeCount) {
                LOG.debug("[AWDB] record == nodeCount, 表示没有数据");
            } else {
                LOG.debug("[AWDB] record < nodeCount, 循环提前结束");
            }
        }

        return null;
    }

    /**
     * 获取起始节点
     */
    private int startNode(int bitLength) {
        // 如果在 IPv6 数据库中查找 IPv4 地址，跳过前 96 个节点
        if (ipVersion == 6 && bitLength == 32) {
            return ipV4Start;
        }
        return 0;
    }

    /**
     * 查找 IPv4 起始节点（用于 IPv6 数据库）
     */
    private int findIpV4StartNode() {
        if (ipVersion == 4) {
            return 0;
        }

        synchronized (buffer) {
            ByteBuffer buf = buffer.duplicate();
            int node = 0;
            for (int i = 0; i < 96 && node < nodeCount; i++) {
                node = readNode(buf, node, 0);
            }
            return node;
        }
    }

    /**
     * 读取节点
     */
    private int readNode(ByteBuffer buf, int nodeNumber, int index) {
        int baseOffset = nodeNumber * nodeByteSize;

        switch (recordSize) {
            case 24:
                buf.position(baseOffset + index * 3);
                return decodeInteger(buf, 0, 3);
            case 28:
                int middle = buf.get(baseOffset + 3);
                if (index == 0) {
                    middle = (0xF0 & middle) >>> 4;
                } else {
                    middle = 0x0F & middle;
                }
                buf.position(baseOffset + index * 4);
                return decodeInteger(buf, middle, 3);
            case 32:
                buf.position(baseOffset + index * 4);
                return decodeInteger(buf, 0, 4);
            default:
                throw new IllegalStateException("Unknown record size: " + recordSize);
        }
    }

    /**
     * 解析数据指针
     */
    private JsonNode resolveDataPointer(ByteBuffer buf, int pointer) throws IOException {
        int resolved = (pointer - nodeCount) + searchTreeSize;

        LOG.debug("[AWDB] resolveDataPointer: pointer={}, nodeCount={}, searchTreeSize={}, resolved={}, capacity={}",
                pointer, nodeCount, searchTreeSize, resolved, buf.capacity());

        if (resolved >= buf.capacity()) {
            LOG.error("[AWDB] Invalid data pointer! resolved={} >= capacity={}", resolved, buf.capacity());
            throw new IOException("Invalid data pointer: " + pointer);
        }

        // 解码数据，pointerBase 是数据区的起始位置
        int pointerBase = searchTreeSize + DATA_SECTION_SEPARATOR_SIZE;
        LOG.debug("[AWDB] 开始解码数据: offset={}, pointerBase={}", resolved, pointerBase);

        JsonNode result = decode(buf, resolved, pointerBase);
        LOG.debug("[AWDB] 解码结果: {}", result);
        return result;
    }

    /**
     * 解码数据
     */
    private JsonNode decode(ByteBuffer buf, int offset, int pointerBase) throws IOException {
        if (offset >= buf.capacity()) {
            throw new IOException("Invalid offset: " + offset);
        }

        buf.position(offset);
        return decodeValue(buf, pointerBase);
    }

    /**
     * 解码值
     */
    private JsonNode decodeValue(ByteBuffer buf, int pointerBase) throws IOException {
        int ctrlByte = 0xFF & buf.get();
        int type = (ctrlByte >>> 5);

        // 指针类型 (type == 1)
        if (type == 1) {
            int pointerSize = ((ctrlByte >>> 3) & 0x3) + 1;
            int base = pointerSize == 4 ? 0 : (ctrlByte & 0x7);
            int packed = decodeInteger(buf, base, pointerSize);

            int[] pointerValueOffsets = {0, 0, 1 << 11, (1 << 19) + (1 << 11), 0};
            int pointer = packed + pointerBase + pointerValueOffsets[pointerSize];

            int position = buf.position();
            JsonNode node = decode(buf, pointer, pointerBase);
            buf.position(position);
            return node;
        }

        // 扩展类型 (type == 0)
        if (type == 0) {
            int nextByte = 0xFF & buf.get();
            type = nextByte + 7;
        }

        // 计算大小
        int size = ctrlByte & 0x1f;
        if (size >= 29) {
            switch (size) {
                case 29:
                    size = 29 + (0xFF & buf.get());
                    break;
                case 30:
                    size = 285 + decodeInteger(buf, 0, 2);
                    break;
                default:
                    size = 65821 + decodeInteger(buf, 0, 3);
            }
        }

        return decodeByType(buf, type, size, pointerBase);
    }

    /**
     * 根据类型解码
     */
    private JsonNode decodeByType(ByteBuffer buf, int type, int size, int pointerBase) throws IOException {
        switch (type) {
            case 2: // UTF8_STRING
                return decodeString(buf, size);
            case 3: // DOUBLE
                return OBJECT_MAPPER.getNodeFactory().numberNode(buf.getDouble());
            case 4: // BYTES (作为字符串处理)
                return decodeString(buf, size);
            case 5: // UINT16
            case 6: // UINT32
                return OBJECT_MAPPER.getNodeFactory().numberNode(decodeLong(buf, size));
            case 7: // MAP
                return decodeMap(buf, size, pointerBase);
            case 8: // INT32
                return OBJECT_MAPPER.getNodeFactory().numberNode(decodeInteger(buf, 0, size));
            case 9: // UINT64
            case 10: // UINT128
                return OBJECT_MAPPER.getNodeFactory().numberNode(decodeLong(buf, size));
            case 11: // ARRAY
                return decodeArray(buf, size, pointerBase);
            case 14: // BOOLEAN
                return OBJECT_MAPPER.getNodeFactory().booleanNode(size != 0);
            case 15: // FLOAT
                return OBJECT_MAPPER.getNodeFactory().numberNode(buf.getFloat());
            default:
                throw new IOException("Unknown type: " + type);
        }
    }

    /**
     * 解码字符串
     */
    private JsonNode decodeString(ByteBuffer buf, int size) throws IOException {
        if (size == 0) {
            return new TextNode("");
        }
        int oldLimit = buf.limit();
        buf.limit(buf.position() + size);
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        String s = decoder.decode(buf).toString();
        buf.limit(oldLimit);
        return new TextNode(s);
    }

    /**
     * 解码 Map
     */
    private JsonNode decodeMap(ByteBuffer buf, int size, int pointerBase) throws IOException {
        Map<String, JsonNode> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = decodeValue(buf, pointerBase).asText();
            JsonNode value = decodeValue(buf, pointerBase);
            map.put(key, value);
        }
        return new ObjectNode(OBJECT_MAPPER.getNodeFactory(), Collections.unmodifiableMap(map));
    }

    /**
     * 解码数组
     */
    private JsonNode decodeArray(ByteBuffer buf, int size, int pointerBase) throws IOException {
        ArrayNode array = OBJECT_MAPPER.createArrayNode();
        for (int i = 0; i < size; i++) {
            array.add(decodeValue(buf, pointerBase));
        }
        return array;
    }

    /**
     * 解码整数
     */
    private static int decodeInteger(ByteBuffer buf, int base, int size) {
        int integer = base;
        for (int i = 0; i < size; i++) {
            integer = (integer << 8) | (buf.get() & 0xFF);
        }
        return integer;
    }

    /**
     * 解码长整数
     */
    private static long decodeLong(ByteBuffer buf, int size) {
        long value = 0;
        for (int i = 0; i < size; i++) {
            value = (value << 8) | (buf.get() & 0xFF);
        }
        return value;
    }

    /**
     * 查找元数据起始位置
     */
    private int findMetadataStart() throws IOException {
        synchronized (buffer) {
            for (int i = 0; i < fileSize - METADATA_START_MARKER.length + 1; i++) {
                boolean found = true;
                for (int j = 0; j < METADATA_START_MARKER.length; j++) {
                    byte b = buffer.get(fileSize - i - j - 1);
                    if (b != METADATA_START_MARKER[METADATA_START_MARKER.length - j - 1]) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    return fileSize - i;
                }
            }
        }
        throw new IOException("Could not find AWDB metadata marker");
    }

    /**
     * 解码元数据
     */
    private JsonNode decodeMetadata(int start) throws IOException {
        synchronized (buffer) {
            ByteBuffer buf = buffer.duplicate();
            int pointerBase = searchTreeSize + DATA_SECTION_SEPARATOR_SIZE;
            // 元数据解码时 searchTreeSize 还未初始化，用 start 作为 pointerBase
            return decode(buf, start, start);
        }
    }

    /**
     * 获取关联的文件
     */
    public File getFile() {
        return file;
    }

    @Override
    public void close() throws IOException {
        // 1. 先 unmap MappedByteBuffer
        if (buffer != null) {
            unmap(buffer);
        }
        // 2. 关闭文件
        if (raf != null) {
            raf.close();
        }
    }

    /**
     * 手动 unmap MappedByteBuffer
     */
    private static void unmap(MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        try {
            // Java 9+ 方式
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            java.lang.reflect.Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Object unsafe = unsafeField.get(null);
            Method invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            invokeCleaner.invoke(unsafe, buffer);
        } catch (Exception e) {
            // Java 8 方式
            try {
                Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                cleanerMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(buffer);
                if (cleaner != null) {
                    Method cleanMethod = cleaner.getClass().getMethod("clean");
                    cleanMethod.setAccessible(true);
                    cleanMethod.invoke(cleaner);
                }
            } catch (Exception e2) {
                LOG.debug("无法手动 unmap buffer: {}", e2.getMessage());
            }
        }
    }
}