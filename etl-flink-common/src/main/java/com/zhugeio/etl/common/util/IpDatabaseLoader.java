package com.zhugeio.etl.common.util;

import com.zhugeio.etl.common.model.IpDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IP数据库加载器
 * 支持从HDFS加载IPv4和IPv6数据库，支持热更新
 */
public class IpDatabaseLoader implements Closeable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseLoader.class);
    private static final long serialVersionUID = 1L;

    // IPv4 数据库
    private final AtomicReference<IpDatabase> ipv4DatabaseRef = new AtomicReference<>();

    // IPv6 数据库 (AWDB格式)
    private final AtomicReference<AwdbReader> ipv6DatabaseRef = new AtomicReference<>();

    // ============================================================
    // IPv4 配置 (对应 Scala: ip.file.dir, reload.ip.file)
    // ============================================================
    private final String ipv4HdfsPath;
    private final boolean enableIpv4HotReload;

    // ============================================================
    // IPv6 配置 (对应 Scala: ipv6.file.dir, ipv6.load, reload.ipv6.file)
    // ============================================================
    private final String ipv6HdfsPath;
    private final boolean enableIpv6;
    private final boolean enableIpv6HotReload;

    // ============================================================
    // 公共配置 (对应 Scala: reload.rate.second)
    // ============================================================
    private final long reloadIntervalSeconds;

    // ============================================================
    // HDFS 配置 (对应 Scala HdfsFileScaner.initHdfsFilesys)
    // ============================================================
    private final boolean isHdfsHA;                    // flag.ha
    private final String hdfsDefaultFs;                // fs.defaultFS
    private final String dfsNameservices;              // dfs.nameservices
    private final String dfsHaNamenodes;               // dfs.ha.namenodes.namespace
    private final String dfsNamenodeRpcZ1;             // dfs.namenode.rpc-address.namespace.z1
    private final String dfsNamenodeRpcZ2;             // dfs.namenode.rpc-address.namespace.z2

    // 热更新状态
    private volatile long ipv4LastModifyTime = 0L;
    private volatile long ipv6LastModifyTime = 0L;
    private transient ScheduledExecutorService ipv4Scheduler;
    private transient ScheduledExecutorService ipv6Scheduler;

    private IpDatabaseLoader(Builder builder) {
        this.ipv4HdfsPath = builder.ipv4HdfsPath;
        this.enableIpv4HotReload = builder.enableIpv4HotReload;
        this.ipv6HdfsPath = builder.ipv6HdfsPath;
        this.enableIpv6 = builder.enableIpv6;
        this.enableIpv6HotReload = builder.enableIpv6HotReload;
        this.reloadIntervalSeconds = builder.reloadIntervalSeconds;
        this.isHdfsHA = builder.isHdfsHA;
        this.hdfsDefaultFs = builder.hdfsDefaultFs;
        this.dfsNameservices = builder.dfsNameservices;
        this.dfsHaNamenodes = builder.dfsHaNamenodes;
        this.dfsNamenodeRpcZ1 = builder.dfsNamenodeRpcZ1;
        this.dfsNamenodeRpcZ2 = builder.dfsNamenodeRpcZ2;
    }

    /**
     * 初始化数据库
     * 对齐 Scala AWReader.init() 逻辑
     */
    public void init() throws Exception {
        LOG.info("╔════════════════════════════════════════════════════════════╗");
        LOG.info("║  IP 数据库初始化 (Scala AWReader 风格)                      ║");
        LOG.info("╠════════════════════════════════════════════════════════════╣");
        LOG.info("║  IPv4 配置:                                                ║");
        LOG.info("║    ip.file.dir: {}", ipv4HdfsPath);
        LOG.info("║    reload.ip.file: {}", enableIpv4HotReload);
        LOG.info("╠════════════════════════════════════════════════════════════╣");
        LOG.info("║  IPv6 配置:                                                ║");
        LOG.info("║    ipv6.file.dir: {}", ipv6HdfsPath);
        LOG.info("║    ipv6.load: {}", enableIpv6);
        LOG.info("║    reload.ipv6.file: {}", enableIpv6HotReload);
        LOG.info("╠════════════════════════════════════════════════════════════╣");
        LOG.info("║  公共配置:                                                 ║");
        LOG.info("║    reload.rate.second: {}", reloadIntervalSeconds);
        LOG.info("╠════════════════════════════════════════════════════════════╣");
        LOG.info("║  HDFS 配置:                                                ║");
        LOG.info("║    flag.ha: {}", isHdfsHA);
        LOG.info("║    fs.defaultFS: {}", hdfsDefaultFs);
        if (isHdfsHA) {
            LOG.info("║    dfs.nameservices: {}", dfsNameservices);
            LOG.info("║    dfs.ha.namenodes.namespace: {}", dfsHaNamenodes);
            LOG.info("║    dfs.namenode.rpc-address.z1: {}", dfsNamenodeRpcZ1);
            LOG.info("║    dfs.namenode.rpc-address.z2: {}", dfsNamenodeRpcZ2);
        }
        LOG.info("╚════════════════════════════════════════════════════════════╝");

        // 1. 加载 IPv4 数据库 (总是加载)
        loadIpv4Database();

        // 2. 启动 IPv4 热更新 (如果 reload.ip.file = true)
        if (enableIpv4HotReload) {
            startIpv4HotReload();
        }

        // 3. 加载 IPv6 数据库 (如果 ipv6.load = true)
        if (enableIpv6 && ipv6HdfsPath != null && !ipv6HdfsPath.isEmpty()) {
            loadIpv6Database();

            // 4. 启动 IPv6 热更新 (如果 reload.ipv6.file = true)
            if (enableIpv6HotReload) {
                startIpv6HotReload();
            }
        }

        LOG.info("IP数据库初始化完成: IPv4={}, IPv6={}",
                ipv4DatabaseRef.get() != null,
                ipv6DatabaseRef.get() != null);
    }

    /**
     * 查询IP地址
     */
    public String[] find(String ip) {
        if (ip == null || ip.isEmpty()) {
            return new String[]{"", "", ""};
        }

        try {
            InetAddress addr = InetAddress.getByName(ip);
            return find(addr);
        } catch (Exception e) {
            LOG.debug("IP解析失败: {}", ip);
            return new String[]{"", "", ""};
        }
    }

    /**
     * 查询IP地址
     * 对齐 Scala AWReader.get(InetAddress)
     */
    public String[] find(InetAddress ipAddress) {
        if (ipAddress == null) {
            return new String[]{"", "", ""};
        }

        try {
            // IPv6 地址
            if (ipAddress instanceof java.net.Inet6Address) {
                AwdbReader ipv6Db = ipv6DatabaseRef.get();
                if (ipv6Db != null) {
                    return ipv6Db.get(ipAddress);
                } else {
                    LOG.debug("IPv6数据库未加载: {}", ipAddress.getHostAddress());
                    return new String[]{"", "", ""};
                }
            }

            // IPv4 地址
            IpDatabase ipv4Db = ipv4DatabaseRef.get();
            if (ipv4Db != null) {
                return ipv4Db.find(ipAddress.getHostAddress());
            } else {
                LOG.warn("IPv4数据库未加载");
                return new String[]{"", "", ""};
            }
        } catch (Exception e) {
            LOG.error("IP查询异常: {}", ipAddress, e);
            return new String[]{"", "", ""};
        }
    }

    // ========== 私有方法 ==========

    private void loadIpv4Database() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv4HdfsPath);

        if (fileStatus == null) {
            throw new IOException("IPv4 数据文件不存在: " + ipv4HdfsPath);
        }

        byte[] data = readFileToBytes(fs, fileStatus);
        IpDatabase database = new IpDatabase(data);
        ipv4DatabaseRef.set(database);
        ipv4LastModifyTime = fileStatus.getModificationTime();

        LOG.info("IPv4数据库加载完成: path={}, size={}bytes",
                fileStatus.getPath(), data.length);
    }

    private void loadIpv6Database() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv6HdfsPath);

        if (fileStatus == null) {
            LOG.warn("IPv6 数据文件不存在: {}", ipv6HdfsPath);
            return;
        }

        byte[] data = readFileToBytes(fs, fileStatus);
        AwdbReader database = new AwdbReader(data);
        ipv6DatabaseRef.set(database);
        ipv6LastModifyTime = fileStatus.getModificationTime();

        LOG.info("IPv6数据库(AWDB)加载完成: path={}, size={}bytes",
                fileStatus.getPath(), data.length);
    }

    /**
     * 获取 HDFS FileSystem
     * 完全对齐 Scala HdfsFileScaner.initHdfsFilesys()
     *
     * 注意: 不需要显式设置 fs.hdfs.impl 和 fs.file.impl
     * FileSystem.get() 会根据 fs.defaultFS 的 scheme 自动加载正确的实现
     */
    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();

        if (isHdfsHA) {
            // HA 模式 - 对齐 Scala HdfsFileScaner
            conf.set("fs.defaultFS", hdfsDefaultFs);
            conf.set("dfs.nameservices", dfsNameservices);

            // dfs.ha.namenodes.{nameservice}
            String haKey = "dfs.ha.namenodes." + dfsNameservices;
            conf.set(haKey, dfsHaNamenodes);

            // 解析 namenodes (例如 "realtime-1,realtime-2")
            if (dfsHaNamenodes != null && dfsNamenodeRpcZ1 != null && dfsNamenodeRpcZ2 != null) {
                String[] nodes = dfsHaNamenodes.split(",");
                if (nodes.length >= 2) {
                    // dfs.namenode.rpc-address.{nameservice}.{node1}
                    String rpcKey1 = "dfs.namenode.rpc-address." + dfsNameservices + "." + nodes[0].trim();
                    conf.set(rpcKey1, dfsNamenodeRpcZ1);

                    // dfs.namenode.rpc-address.{nameservice}.{node2}
                    String rpcKey2 = "dfs.namenode.rpc-address." + dfsNameservices + "." + nodes[1].trim();
                    conf.set(rpcKey2, dfsNamenodeRpcZ2);
                }
            }

            // Failover provider
            String providerKey = "dfs.client.failover.proxy.provider." + dfsNameservices;
            conf.set(providerKey, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        } else {
            // 非 HA 模式
            conf.set("fs.defaultFS", hdfsDefaultFs);
        }

        return FileSystem.get(conf);
    }

    /**
     * 获取目录下最新文件
     * 对齐 Scala HdfsFileScaner.getRecentFileInfo()
     */
    private FileStatus getRecentFile(FileSystem fs, String dirPath) throws IOException {
        Path path = new Path(dirPath);

        if (!fs.exists(path)) {
            return null;
        }

        if (fs.isFile(path)) {
            return fs.getFileStatus(path);
        }

        // 目录则找最新文件
        FileStatus recent = null;
        long maxModTime = 0L;

        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> iter =
                fs.listFiles(path, true);

        while (iter.hasNext()) {
            FileStatus fileStatus = iter.next();
            if (fileStatus.getModificationTime() > maxModTime) {
                maxModTime = fileStatus.getModificationTime();
                recent = fileStatus;
            }
        }

        return recent;
    }

    private byte[] readFileToBytes(FileSystem fs, FileStatus fileStatus) throws IOException {
        try (FSDataInputStream in = fs.open(fileStatus.getPath());
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[16 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            return out.toByteArray();
        }
    }

    /**
     * 启动 IPv4 热更新
     * 对齐 Scala AWReader.watch() for v4
     */
    private void startIpv4HotReload() {
        ipv4Scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ipv4-database-hot-reload");
            t.setDaemon(true);
            return t;
        });

        ipv4Scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndReloadIpv4();
            } catch (Exception e) {
                LOG.error("IPv4数据库热更新检查失败", e);
            }
        }, reloadIntervalSeconds, reloadIntervalSeconds, TimeUnit.SECONDS);

        LOG.info("IPv4数据库热更新已启动, 检查间隔: {}秒", reloadIntervalSeconds);
    }

    /**
     * 启动 IPv6 热更新
     * 对齐 Scala AWReader.watch() for v6
     */
    private void startIpv6HotReload() {
        ipv6Scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ipv6-database-hot-reload");
            t.setDaemon(true);
            return t;
        });

        ipv6Scheduler.scheduleAtFixedRate(() -> {
            try {
                checkAndReloadIpv6();
            } catch (Exception e) {
                LOG.error("IPv6数据库热更新检查失败", e);
            }
        }, reloadIntervalSeconds, reloadIntervalSeconds, TimeUnit.SECONDS);

        LOG.info("IPv6数据库热更新已启动, 检查间隔: {}秒", reloadIntervalSeconds);
    }

    private void checkAndReloadIpv4() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus status = getRecentFile(fs, ipv4HdfsPath);
        if (status != null && status.getModificationTime() > ipv4LastModifyTime) {
            LOG.info("检测到IPv4数据库更新, 重新加载...");
            loadIpv4Database();
        }
    }

    private void checkAndReloadIpv6() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus status = getRecentFile(fs, ipv6HdfsPath);
        if (status != null && status.getModificationTime() > ipv6LastModifyTime) {
            LOG.info("检测到IPv6数据库更新, 重新加载...");
            loadIpv6Database();
        }
    }

    @Override
    public void close() throws IOException {
        if (ipv4Scheduler != null) {
            ipv4Scheduler.shutdown();
            try {
                if (!ipv4Scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    ipv4Scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                ipv4Scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (ipv6Scheduler != null) {
            ipv6Scheduler.shutdown();
            try {
                if (!ipv6Scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    ipv6Scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                ipv6Scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        ipv4DatabaseRef.set(null);
        ipv6DatabaseRef.set(null);

        LOG.info("IP数据库已关闭");
    }

    /**
     * 检查 IPv6 是否可用
     */
    public boolean isIpv6Enabled() {
        return ipv6DatabaseRef.get() != null;
    }

    // ========== Builder ==========

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        // IPv4 配置
        private String ipv4HdfsPath;
        private boolean enableIpv4HotReload = false;

        // IPv6 配置
        private String ipv6HdfsPath;
        private boolean enableIpv6 = false;
        private boolean enableIpv6HotReload = false;

        // 公共配置
        private long reloadIntervalSeconds = 43200;

        // HDFS 配置
        private boolean isHdfsHA = true;
        private String hdfsDefaultFs = "hdfs://zhugeio";
        private String dfsNameservices = "zhugeio";
        private String dfsHaNamenodes = "realtime-1,realtime-2";
        private String dfsNamenodeRpcZ1 = "realtime-1:8020";
        private String dfsNamenodeRpcZ2 = "realtime-2:8020";

        // ============ IPv4 配置 ============

        /** 设置 IPv4 数据目录 (对应 ip.file.dir) */
        public Builder ipv4HdfsPath(String path) {
            this.ipv4HdfsPath = path;
            return this;
        }

        /** 设置 IPv4 热加载开关 (对应 reload.ip.file) */
        public Builder enableIpv4HotReload(boolean enable) {
            this.enableIpv4HotReload = enable;
            return this;
        }

        // ============ IPv6 配置 ============

        /** 设置 IPv6 数据目录 (对应 ipv6.file.dir) */
        public Builder ipv6HdfsPath(String path) {
            this.ipv6HdfsPath = path;
            return this;
        }

        /** 设置是否启用 IPv6 (对应 ipv6.load) */
        public Builder enableIpv6(boolean enable) {
            this.enableIpv6 = enable;
            return this;
        }

        /** 设置 IPv6 热加载开关 (对应 reload.ipv6.file) */
        public Builder enableIpv6HotReload(boolean enable) {
            this.enableIpv6HotReload = enable;
            return this;
        }

        // ============ 公共配置 ============

        /** 设置重载检查间隔秒数 (对应 reload.rate.second) */
        public Builder reloadIntervalSeconds(long seconds) {
            this.reloadIntervalSeconds = seconds;
            return this;
        }

        // ============ HDFS 配置 ============

        /** 设置是否 HA 模式 (对应 flag.ha) */
        public Builder hdfsHA(boolean isHA) {
            this.isHdfsHA = isHA;
            return this;
        }

        /** 设置 HDFS 默认地址 (对应 fs.defaultFS) */
        public Builder hdfsDefaultFs(String defaultFs) {
            this.hdfsDefaultFs = defaultFs;
            return this;
        }

        /** 设置 dfs.nameservices */
        public Builder dfsNameservices(String nameservices) {
            this.dfsNameservices = nameservices;
            return this;
        }

        /** 设置 dfs.ha.namenodes.namespace (例如 "realtime-1,realtime-2") */
        public Builder dfsHaNamenodes(String namenodes) {
            this.dfsHaNamenodes = namenodes;
            return this;
        }

        /** 设置 dfs.namenode.rpc-address.namespace.z1 */
        public Builder dfsNamenodeRpcZ1(String rpcAddress) {
            this.dfsNamenodeRpcZ1 = rpcAddress;
            return this;
        }

        /** 设置 dfs.namenode.rpc-address.namespace.z2 */
        public Builder dfsNamenodeRpcZ2(String rpcAddress) {
            this.dfsNamenodeRpcZ2 = rpcAddress;
            return this;
        }

        public IpDatabaseLoader build() {
            if (ipv4HdfsPath == null || ipv4HdfsPath.isEmpty()) {
                throw new IllegalArgumentException("ipv4HdfsPath is required");
            }
            return new IpDatabaseLoader(this);
        }
    }
}