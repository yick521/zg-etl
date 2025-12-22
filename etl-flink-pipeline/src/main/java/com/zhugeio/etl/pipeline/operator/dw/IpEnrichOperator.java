package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.util.IpDatabaseLoader;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * IP 地理位置富化算子
 */
public class IpEnrichOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IpEnrichOperator.class);

    private transient IpDatabaseLoader ipLoader;

    // ========== 新增: Metrics ==========
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

        // ========== 新增: 初始化 Metrics ==========
        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "ip_enrich");

        // 构建 IP 数据库加载器
        IpDatabaseLoader.Builder builder = IpDatabaseLoader.builder();

        builder.ipv4HdfsPath(ipv4FileDir)
                .enableIpv4HotReload(enableIpv4Reload);

        if (enableIpv6) {
            builder.ipv6HdfsPath(ipv6FileDir)
                    .enableIpv6(true)
                    .enableIpv6HotReload(enableIpv6Reload);
            LOG.info("[IP富化算子-{}] ✓ IPv6 已启用, 目录: {}", subtaskIndex, ipv6FileDir);
        } else {
            builder.enableIpv6(false);
            LOG.info("[IP富化算子-{}] ✗ IPv6 已禁用 (ipv6.load=false)", subtaskIndex);
        }

        builder.reloadIntervalSeconds(reloadIntervalSeconds);

        if (isHdfsHA) {
            builder.hdfsHA(true)
                    .hdfsDefaultFs(hdfsDefaultFs)
                    .dfsNameservices(dfsNameservices)
                    .dfsHaNamenodes(dfsHaNamenodes)
                    .dfsNamenodeRpcZ1(dfsNamenodeRpcZ1)
                    .dfsNamenodeRpcZ2(dfsNamenodeRpcZ2);
            LOG.info("[IP富化算子-{}] ✓ HDFS HA 模式", subtaskIndex);
        } else {
            builder.hdfsHA(false)
                    .hdfsDefaultFs(hdfsDefaultFs);
            LOG.info("[IP富化算子-{}] ✓ HDFS 非 HA 模式", subtaskIndex);
        }

        ipLoader = builder.build();
        ipLoader.init();

        LOG.info("[IP富化算子-{}] ✓ 初始化完成", subtaskIndex);
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        // ========== 新增: 记录输入 ==========
        metrics.in();

        CompletableFuture.supplyAsync(() -> {
            try {
                Map<String, Object> data = message.getData();
                if (data == null) {
                    metrics.skip();  // 新增
                    return message;
                }

                Object ipObj = data.get("ip");
                if (ipObj == null) {
                    metrics.skip();  // 新增
                    return message;
                }

                String ip = String.valueOf(ipObj);
                if (ip.isEmpty() || "null".equals(ip) || "0.0.0.0".equals(ip)) {
                    metrics.skip();  // 新增
                    return message;
                }

                // 查询 IP 数据库
                String[] result = ipLoader.find(ip);

                if (result != null && result.length >= 3) {
                    data.put("country", result[0] != null ? result[0] : "\\N");
                    data.put("province", result[1] != null ? result[1] : "\\N");
                    data.put("city", result[2] != null ? result[2] : "\\N");
                } else {
                    data.put("country", "\\N");
                    data.put("province", "\\N");
                    data.put("city", "\\N");
                }

                metrics.out();  // 新增

            } catch (Exception e) {
                LOG.error("[IP富化算子] 处理失败: ip={}, error={}",
                        message.getData() != null ? message.getData().get("ip") : "null",
                        e.getMessage());
                metrics.error();  // 新增
            }

            return message;

        }).whenComplete((result, throwable) -> {
            if (throwable != null) {
                LOG.error("[IP富化算子] 异步处理异常", throwable);
                metrics.error();  // 新增
                resultFuture.complete(Collections.singleton(message));
            } else {
                resultFuture.complete(Collections.singleton(result));
            }
        });
    }

    @Override
    public void timeout(ZGMessage message, ResultFuture<ZGMessage> resultFuture) throws Exception {
        LOG.warn("[IP富化算子] 处理超时, offset={}", message.getOffset());
        metrics.error();  // 新增
        Map<String, Object> data = message.getData();
        if (data != null) {
            data.put("country", "\\N");
            data.put("province", "\\N");
            data.put("city", "\\N");
        }
        resultFuture.complete(Collections.singleton(message));
    }

    @Override
    public void close() throws Exception {
        // ========== 新增: 打印 Metrics ==========
        LOG.info("[IP富化算子] Metrics: in={}, out={}, error={}, skip={}",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip());

        if (ipLoader != null) {
            ipLoader.close();
            LOG.info("[IP富化算子] IP 数据库已关闭");
        }
    }
}