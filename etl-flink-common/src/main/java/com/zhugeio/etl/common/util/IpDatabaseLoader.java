package com.zhugeio.etl.common.util;

import com.zhugeio.etl.common.model.IpDatabaseMapped;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IP数据库加载器 - 内存映射版本
 * 
 * 【核心优化】
 * - IPv4 使用 MappedByteBuffer 内存映射，数据不占用 JVM 堆内存
 * - 100MB IP库：堆内存从 512MB 降到 ~256KB
 * - 由操作系统管理页缓存，按需加载
 * 
 * 【内存对比】
 * | 方案           | 100MB 文件堆内存占用 |
 * |----------------|---------------------|
 * | 原版本          | 512 MB（扩容+复制）  |
 * | 预分配修复      | 100 MB              |
 * | 内存映射（本版本）| ~256 KB（仅索引）    |
 * 
 * 完全兼容原 IpDatabaseLoader 的接口和配置
 */
public class IpDatabaseLoader implements Closeable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseLoader.class);
    private static final long serialVersionUID = 1L;

    // 本地临时文件目录
    private static final String LOCAL_TEMP_DIR = "/tmp/ip-database";

    // IPv4 数据库（内存映射版本）
    private final AtomicReference<IpDatabaseMapped> ipv4DatabaseRef = new AtomicReference<>();

    // IPv6 数据库 (AWDB格式)
    private final AtomicReference<AwdbReader> ipv6DatabaseRef = new AtomicReference<>();

    // 内存映射资源
    private volatile RandomAccessFile ipv4Raf;
    private volatile MappedByteBuffer ipv4MappedBuffer;
    private volatile File ipv4LocalFile;

    // ============================================================
    // IPv4 配置
    // ============================================================
    private final String ipv4HdfsPath;
    private final boolean enableIpv4HotReload;

    // ============================================================
    // IPv6 配置
    // ============================================================
    private final String ipv6HdfsPath;
    private final boolean enableIpv6;
    private final boolean enableIpv6HotReload;

    // ============================================================
    // 公共配置
    // ============================================================
    private final long reloadIntervalSeconds;

    // ============================================================
    // HDFS 配置
    // ============================================================
    private final boolean isHdfsHA;
    private final String hdfsDefaultFs;
    private final String dfsNameservices;
    private final String dfsHaNamenodes;
    private final String dfsNamenodeRpcZ1;
    private final String dfsNamenodeRpcZ2;

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
     */
    public void init() throws Exception {
        LOG.info("╔════════════════════════════════════════════════════════════╗");
        LOG.info("║  IP 数据库初始化 (内存映射版本 - 零堆内存占用)               ║");
        LOG.info("╠════════════════════════════════════════════════════════════╣");
        LOG.info("║  IPv4 配置:                                                ║");
        LOG.info("║    ip.file.dir: {}", ipv4HdfsPath);
        LOG.info("║    reload.ip.file: {}", enableIpv4HotReload);
        LOG.info("║    加载方式: MappedByteBuffer (堆内存≈0)");
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

        // 创建临时目录
        File tempDir = new File(LOCAL_TEMP_DIR);
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }

        // 1. 加载 IPv4 数据库（内存映射）
        loadIpv4DatabaseMapped();

        // 2. 启动 IPv4 热更新
        if (enableIpv4HotReload) {
            startIpv4HotReload();
        }

        // 3. 加载 IPv6 数据库
        if (enableIpv6 && ipv6HdfsPath != null && !ipv6HdfsPath.isEmpty()) {
            loadIpv6Database();

            if (enableIpv6HotReload) {
                startIpv6HotReload();
            }
        }

        LOG.info("IP数据库初始化完成: IPv4(mapped)={}, IPv6={}",
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

            // IPv4 地址（使用内存映射版本）
            IpDatabaseMapped ipv4Db = ipv4DatabaseRef.get();
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

    // ========== IPv4 内存映射加载 ==========

    /**
     * 使用内存映射加载 IPv4 数据库
     */
    private void loadIpv4DatabaseMapped() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv4HdfsPath);

        if (fileStatus == null) {
            throw new IOException("IPv4 数据文件不存在: " + ipv4HdfsPath);
        }

        long fileSize = fileStatus.getLen();
        LOG.info("开始加载 IPv4 数据库: path={}, size={} MB", 
                fileStatus.getPath(), fileSize / 1024 / 1024);

        // 1. 从 HDFS 下载到本地临时文件
        File localFile = downloadToLocal(fs, fileStatus);

        // 2. 内存映射
        RandomAccessFile raf = new RandomAccessFile(localFile, "r");
        MappedByteBuffer mappedBuffer = raf.getChannel().map(
                FileChannel.MapMode.READ_ONLY, 0, raf.length());

        // 3. 创建内存映射版本的数据库
        IpDatabaseMapped database = new IpDatabaseMapped(mappedBuffer);

        // 4. 清理旧资源
        cleanupOldMappedResources();

        // 5. 保存新资源引用
        this.ipv4Raf = raf;
        this.ipv4MappedBuffer = mappedBuffer;
        this.ipv4LocalFile = localFile;
        this.ipv4DatabaseRef.set(database);
        this.ipv4LastModifyTime = fileStatus.getModificationTime();

        LOG.info("╔════════════════════════════════════════════════════════════╗");
        LOG.info("║  IPv4 数据库加载完成 (内存映射)                              ║");
        LOG.info("║  文件大小: {} MB                                           ", fileSize / 1024 / 1024);
        LOG.info("║  堆内存占用: ~256 KB (仅索引数组)                           ║");
        LOG.info("║  数据存储: 操作系统页缓存 (不占 JVM 堆)                      ║");
        LOG.info("╚════════════════════════════════════════════════════════════╝");
    }

    /**
     * 从 HDFS 下载文件到本地
     */
    private File downloadToLocal(FileSystem fs, FileStatus fileStatus) throws IOException {
        String fileName = "ipv4-" + System.currentTimeMillis() + ".dat";
        File localFile = new File(LOCAL_TEMP_DIR, fileName);

        LOG.info("下载 HDFS 文件到本地: {} -> {}", fileStatus.getPath(), localFile);

        long fileSize = fileStatus.getLen();
        
        try (FSDataInputStream in = fs.open(fileStatus.getPath());
             FileOutputStream out = new FileOutputStream(localFile)) {
            
            byte[] buffer = new byte[64 * 1024]; // 64KB buffer
            int bytesRead;
            long totalRead = 0;
            
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }
            
            LOG.info("文件下载完成: {} bytes ({} MB)", totalRead, totalRead / 1024 / 1024);
        }

        return localFile;
    }

    /**
     * 清理旧的内存映射资源
     */
    private void cleanupOldMappedResources() {
        try {
            // 清理 MappedByteBuffer
            if (ipv4MappedBuffer != null) {
                unmap(ipv4MappedBuffer);
                ipv4MappedBuffer = null;
            }
            
            // 关闭文件
            if (ipv4Raf != null) {
                ipv4Raf.close();
                ipv4Raf = null;
            }
            
            // 删除临时文件
            if (ipv4LocalFile != null && ipv4LocalFile.exists()) {
                if (ipv4LocalFile.delete()) {
                    LOG.debug("删除旧临时文件: {}", ipv4LocalFile);
                }
                ipv4LocalFile = null;
            }
        } catch (Exception e) {
            LOG.warn("清理旧资源异常", e);
        }
    }

    /**
     * 强制释放 MappedByteBuffer
     * 
     * 注意：这是一个 hack 方法，因为 Java 没有提供标准的 unmap 方法
     */
    private void unmap(MappedByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        
        try {
            // Java 9+ 使用 Unsafe.invokeCleaner
            // Java 8 使用 sun.misc.Cleaner
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    Method cleanerMethod = buffer.getClass().getMethod("cleaner");
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(buffer);
                    if (cleaner != null) {
                        Method cleanMethod = cleaner.getClass().getMethod("clean");
                        cleanMethod.setAccessible(true);
                        cleanMethod.invoke(cleaner);
                    }
                } catch (Exception e) {
                    // 忽略，让 GC 处理
                    LOG.debug("无法手动释放 MappedByteBuffer", e);
                }
                return null;
            });
        } catch (Exception e) {
            LOG.debug("unmap 异常", e);
        }
    }

    // ========== IPv6 加载（优化版）==========

    private void loadIpv6Database() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv6HdfsPath);

        if (fileStatus == null) {
            LOG.warn("IPv6 数据文件不存在: {}", ipv6HdfsPath);
            return;
        }

        // 使用预分配方式读取，避免 ByteArrayOutputStream 扩容
        byte[] data = readFileToBytesOptimized(fs, fileStatus);
        AwdbReader database = new AwdbReader(data);
        ipv6DatabaseRef.set(database);
        ipv6LastModifyTime = fileStatus.getModificationTime();

        LOG.info("IPv6数据库(AWDB)加载完成: path={}, size={} MB",
                fileStatus.getPath(), data.length / 1024 / 1024);
    }

    /**
     * 优化的文件读取（预分配数组大小，避免扩容）
     */
    private byte[] readFileToBytesOptimized(FileSystem fs, FileStatus fileStatus) throws IOException {
        long fileSize = fileStatus.getLen();
        
        // 文件大小检查
        if (fileSize > 200 * 1024 * 1024) { // 200MB
            throw new IOException("IPv6 文件过大: " + fileSize + " bytes");
        }
        
        byte[] data = new byte[(int) fileSize];

        try (FSDataInputStream in = fs.open(fileStatus.getPath())) {
            int offset = 0;
            int remaining = (int) fileSize;

            while (remaining > 0) {
                int bytesRead = in.read(data, offset, remaining);
                if (bytesRead == -1) break;
                offset += bytesRead;
                remaining -= bytesRead;
            }
        }

        return data;
    }

    // ========== HDFS 操作 ==========

    private FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();

        if (isHdfsHA) {
            conf.set("fs.defaultFS", hdfsDefaultFs);
            conf.set("dfs.nameservices", dfsNameservices);

            String haKey = "dfs.ha.namenodes." + dfsNameservices;
            conf.set(haKey, dfsHaNamenodes);

            if (dfsHaNamenodes != null && dfsNamenodeRpcZ1 != null && dfsNamenodeRpcZ2 != null) {
                String[] nodes = dfsHaNamenodes.split(",");
                if (nodes.length >= 2) {
                    String rpcKey1 = "dfs.namenode.rpc-address." + dfsNameservices + "." + nodes[0].trim();
                    conf.set(rpcKey1, dfsNamenodeRpcZ1);

                    String rpcKey2 = "dfs.namenode.rpc-address." + dfsNameservices + "." + nodes[1].trim();
                    conf.set(rpcKey2, dfsNamenodeRpcZ2);
                }
            }

            String providerKey = "dfs.client.failover.proxy.provider." + dfsNameservices;
            conf.set(providerKey, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        } else {
            conf.set("fs.defaultFS", hdfsDefaultFs);
        }

        return FileSystem.get(conf);
    }

    private FileStatus getRecentFile(FileSystem fs, String dirPath) throws IOException {
        Path path = new Path(dirPath);

        if (!fs.exists(path)) {
            return null;
        }

        if (fs.isFile(path)) {
            return fs.getFileStatus(path);
        }

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

    // ========== 热更新 ==========

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
            loadIpv4DatabaseMapped();
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
        // 停止热更新
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

        // 清理内存映射资源
        cleanupOldMappedResources();

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
        private String ipv4HdfsPath;
        private boolean enableIpv4HotReload = false;
        private String ipv6HdfsPath;
        private boolean enableIpv6 = false;
        private boolean enableIpv6HotReload = false;
        private long reloadIntervalSeconds = 43200;
        private boolean isHdfsHA = true;
        private String hdfsDefaultFs = "hdfs://zhugeio";
        private String dfsNameservices = "zhugeio";
        private String dfsHaNamenodes = "realtime-1,realtime-2";
        private String dfsNamenodeRpcZ1 = "realtime-1:8020";
        private String dfsNamenodeRpcZ2 = "realtime-2:8020";

        public Builder ipv4HdfsPath(String path) {
            this.ipv4HdfsPath = path;
            return this;
        }

        public Builder enableIpv4HotReload(boolean enable) {
            this.enableIpv4HotReload = enable;
            return this;
        }

        public Builder ipv6HdfsPath(String path) {
            this.ipv6HdfsPath = path;
            return this;
        }

        public Builder enableIpv6(boolean enable) {
            this.enableIpv6 = enable;
            return this;
        }

        public Builder enableIpv6HotReload(boolean enable) {
            this.enableIpv6HotReload = enable;
            return this;
        }

        public Builder reloadIntervalSeconds(long seconds) {
            this.reloadIntervalSeconds = seconds;
            return this;
        }

        public Builder hdfsHA(boolean isHA) {
            this.isHdfsHA = isHA;
            return this;
        }

        public Builder hdfsDefaultFs(String defaultFs) {
            this.hdfsDefaultFs = defaultFs;
            return this;
        }

        public Builder dfsNameservices(String nameservices) {
            this.dfsNameservices = nameservices;
            return this;
        }

        public Builder dfsHaNamenodes(String namenodes) {
            this.dfsHaNamenodes = namenodes;
            return this;
        }

        public Builder dfsNamenodeRpcZ1(String rpcAddress) {
            this.dfsNamenodeRpcZ1 = rpcAddress;
            return this;
        }

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
