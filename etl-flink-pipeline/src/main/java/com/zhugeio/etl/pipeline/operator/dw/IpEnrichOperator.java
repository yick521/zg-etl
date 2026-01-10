package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.util.IpDatabaseLoader;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * IP 地理位置富化算子
 *
 * 设计说明:
 * IP 库使用 MappedByteBuffer 内存映射，查询是微秒级操作，无需异步。
 * 改为 RichMapFunction，简化代码，提升性能。
 *
 * 内存映射优势:
 * - 100MB IP库：堆内存占用仅 ~256KB（仅索引）
 * - 由操作系统管理页缓存，按需加载
 * - 查询速度：微秒级
 */
public class IpEnrichOperator extends RichMapFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IpEnrichOperator.class);

    private static final String NULL_VALUE = "\\N";

    private transient IpDatabaseLoader ipLoader;
    private transient OperatorMetrics metrics;

    // IPv4 配置
    private final String ipv4FileDir;
    private final boolean enableIpv4Reload;

    // IPv6 配置
    private final String ipv6FileDir;
    private final boolean enableIpv6;
    private final boolean enableIpv6Reload;

    // 公共配置
    private final long reloadIntervalSeconds;

    // HDFS 配置
    private final boolean isHdfsHA;
    private final String hdfsDefaultFs;
    private final String dfsNameservices;
    private final String dfsHaNamenodes;
    private final String dfsNamenodeRpcZ1;
    private final String dfsNamenodeRpcZ2;

    public IpEnrichOperator(
            String ipv4FileDir,
            boolean enableIpv4Reload,
            String ipv6FileDir,
            boolean enableIpv6,
            boolean enableIpv6Reload,
            long reloadIntervalSeconds,
            boolean isHdfsHA,
            String hdfsDefaultFs,
            String dfsNameservices,
            String dfsHaNamenodes,
            String dfsNamenodeRpcZ1,
            String dfsNamenodeRpcZ2) {

        this.ipv4FileDir = ipv4FileDir;
        this.enableIpv4Reload = enableIpv4Reload;
        this.ipv6FileDir = ipv6FileDir;
        this.enableIpv6 = enableIpv6;
        this.enableIpv6Reload = enableIpv6Reload;
        this.reloadIntervalSeconds = reloadIntervalSeconds;
        this.isHdfsHA = isHdfsHA;
        this.hdfsDefaultFs = hdfsDefaultFs;
        this.dfsNameservices = dfsNameservices;
        this.dfsHaNamenodes = dfsHaNamenodes;
        this.dfsNamenodeRpcZ1 = dfsNamenodeRpcZ1;
        this.dfsNamenodeRpcZ2 = dfsNamenodeRpcZ2;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ip_enrich", 30);

        IpDatabaseLoader.Builder builder = IpDatabaseLoader.builder();

        builder.ipv4HdfsPath(ipv4FileDir)
                .enableIpv4HotReload(enableIpv4Reload);

        if (enableIpv6) {
            builder.ipv6HdfsPath(ipv6FileDir)
                    .enableIpv6(true)
                    .enableIpv6HotReload(enableIpv6Reload);
        } else {
            builder.enableIpv6(false);
        }

        builder.reloadIntervalSeconds(reloadIntervalSeconds);

        if (isHdfsHA) {
            builder.hdfsHA(true)
                    .hdfsDefaultFs(hdfsDefaultFs)
                    .dfsNameservices(dfsNameservices)
                    .dfsHaNamenodes(dfsHaNamenodes)
                    .dfsNamenodeRpcZ1(dfsNamenodeRpcZ1)
                    .dfsNamenodeRpcZ2(dfsNamenodeRpcZ2);
        } else {
            builder.hdfsHA(false).hdfsDefaultFs(hdfsDefaultFs);
        }

        ipLoader = builder.build();
        ipLoader.init();

        LOG.info("[IP富化算子-{}] ✓ 初始化完成（同步版：内存映射查询）", subtaskIndex);
    }

    @Override
    public ZGMessage map(ZGMessage message) throws Exception {
        metrics.in();

        Map<String, Object> originalData = message.getData();

        if (originalData == null) {
            metrics.skip();
            return message;
        }

        Object ipObj = originalData.get("ip");

        if (ipObj == null) {
            metrics.skip();
            return message;
        }

        String ip = String.valueOf(ipObj);

        // 跳过条件3: ip 为空字符串、"null" 或 "0.0.0.0"
        if (ip.isEmpty() || "null".equals(ip) || "0.0.0.0".equals(ip)) {
            metrics.skip();
            return message;
        }

        // 【新增】跳过私有 IP 地址
        if (isPrivateIp(ip)) {
            Map<String, Object> newData = new HashMap<>(originalData.size() + 4);
            newData.putAll(originalData);
            newData.put("country", NULL_VALUE);
            newData.put("province", NULL_VALUE);
            newData.put("city", NULL_VALUE);
            message.setData(newData);
            metrics.skip();
            return message;
        }

        try {
            String[] result = ipLoader.find(ip);

            Map<String, Object> newData = new HashMap<>(originalData.size() + 4);
            newData.putAll(originalData);

            // 【新增】检查结果是否有效
            if (result != null && result.length >= 3 && isValidGeoResult(result)) {
                newData.put("country", result[0] != null ? result[0] : NULL_VALUE);
                newData.put("province", result[1] != null ? result[1] : NULL_VALUE);
                newData.put("city", result[2] != null ? result[2] : NULL_VALUE);
            } else {
                newData.put("country", NULL_VALUE);
                newData.put("province", NULL_VALUE);
                newData.put("city", NULL_VALUE);
            }

            message.setData(newData);
            metrics.out();

        } catch (Exception e) {
            LOG.error("[IP富化算子] 查询失败: ip={}, error={}", ip, e.getMessage());
            metrics.error();
        }

        return message;
    }

    /**
     * 判断是否为私有 IP 地址
     */
    private boolean isPrivateIp(String ip) {
        if (ip == null || ip.isEmpty()) {
            return true;
        }

        try {
            String[] parts = ip.split("\\.");
            if (parts.length != 4) {
                return true;
            }

            int first = Integer.parseInt(parts[0]);
            int second = Integer.parseInt(parts[1]);

            // 10.0.0.0 - 10.255.255.255
            if (first == 10) {
                return true;
            }

            // 172.16.0.0 - 172.31.255.255
            if (first == 172 && second >= 16 && second <= 31) {
                return true;
            }

            // 192.168.0.0 - 192.168.255.255
            if (first == 192 && second == 168) {
                return true;
            }

            // 127.0.0.0 - 127.255.255.255 (loopback)
            if (first == 127) {
                return true;
            }

            // 0.0.0.0
            if (first == 0) {
                return true;
            }

            return false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * 检查地理位置结果是否有效（防止乱码）
     */
    private boolean isValidGeoResult(String[] result) {
        if (result == null) {
            return false;
        }

        for (String s : result) {
            if (s != null && !s.isEmpty()) {
                // 检查是否包含异常字符
                for (char c : s.toCharArray()) {
                    // 控制字符（除了常见的空白字符）
                    if (c < 32 && c != '\t' && c != '\n' && c != '\r') {
                        return false;
                    }
                    // Unicode 替换字符（乱码标志）
                    if (c == 0xFFFD) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public void close() throws Exception {
        LOG.info("[IP富化算子] Metrics: in={}, out={}, error={}, skip={}",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip());

        if (ipLoader != null) {
            ipLoader.close();
        }
    }
}
