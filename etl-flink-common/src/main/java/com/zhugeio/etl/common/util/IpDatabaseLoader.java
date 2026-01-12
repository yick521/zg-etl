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
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IP数据库加载器 - 内存映射版本 (单例 + 引用计数)
 *
 * 【核心优化】
 * - 单例模式：同一 JVM 内所有 Flink slot/task 共享一个实例
 * - 引用计数：只有最后一个使用者关闭时才清理资源
 * - IPv4 使用 MappedByteBuffer 内存映射，数据不占用 JVM 堆内存
 * - 文件锁：解决多进程/多 TaskManager 并发下载问题
 *
 * 【Flink 多 slot 场景】
 * - 所有 slot 共享同一个 IP 库文件和内存映射
 * - 热更新时原子替换，不影响正在查询的请求
 * - JVM 退出时才清理临时文件
 */
public class IpDatabaseLoader implements Closeable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseLoader.class);
    private static final long serialVersionUID = 1L;

    // ========== 单例相关 ==========
    private static volatile IpDatabaseLoader INSTANCE;
    private static final Object LOCK = new Object();
    private static final AtomicInteger REF_COUNT = new AtomicInteger(0);

    // 本地临时文件目录
    private static final String LOCAL_TEMP_DIR = "/tmp/ip-database";

    // 文件锁等待超时（秒）
    private static final int FILE_LOCK_TIMEOUT_SECONDS = 120;

    // IPv4 数据库（内存映射版本）
    private final AtomicReference<IpDatabaseMapped> ipv4DatabaseRef = new AtomicReference<>();

    // IPv6 数据库 (AWDB格式)
    private final AtomicReference<AwdbReader> ipv6DatabaseRef = new AtomicReference<>();

    // 当前使用的本地文件（用于热更新时的清理判断）
    private final AtomicReference<File> currentIpv4File = new AtomicReference<>();

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
    private transient ScheduledExecutorService reloadScheduler;

    // 初始化标记
    private volatile boolean initialized = false;

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
     * 获取或创建单例实例
     *
     * @param builder 构建器（仅首次创建时使用）
     * @return 单例实例
     */
    public static IpDatabaseLoader getOrCreate(Builder builder) throws Exception {
        if (INSTANCE == null) {
            synchronized (LOCK) {
                if (INSTANCE == null) {
                    INSTANCE = builder.build();
                    INSTANCE.init();
                    LOG.info("IP数据库单例实例已创建");
                }
            }
        }

        int count = REF_COUNT.incrementAndGet();
        LOG.info("IP数据库引用计数增加: {}", count);

        return INSTANCE;
    }

    /**
     * 获取已存在的单例实例（不创建新实例）
     */
    public static IpDatabaseLoader getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("IpDatabaseLoader 尚未初始化，请先调用 getOrCreate()");
        }
        return INSTANCE;
    }

    /**
     * 检查单例是否已初始化
     */
    public static boolean isInitialized() {
        return INSTANCE != null && INSTANCE.initialized;
    }

    /**
     * 初始化数据库（仅首次调用有效）
     */
    private synchronized void init() throws Exception {
        if (initialized) {
            LOG.info("IP数据库已初始化，跳过");
            return;
        }

        LOG.info("╔════════════════════════════════════════════════════════════╗");
        LOG.info("║  IP 数据库初始化 (单例模式 + 内存映射)                       ║");
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
        LOG.info("╚════════════════════════════════════════════════════════════╝");

        // 创建临时目录
        File tempDir = new File(LOCAL_TEMP_DIR);
        if (!tempDir.exists()) {
            tempDir.mkdirs();
        }

        // 1. 加载 IPv4 数据库（内存映射）
        loadIpv4DatabaseMapped();

        // 2. 加载 IPv6 数据库
        if (enableIpv6 && ipv6HdfsPath != null && !ipv6HdfsPath.isEmpty()) {
            loadIpv6Database();
        }

        // 3. 启动热更新（单一调度器处理所有更新）
        if (enableIpv4HotReload || enableIpv6HotReload) {
            startHotReload();
        }

        initialized = true;

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

        long currentModTime = fileStatus.getModificationTime();

        // 检查是否需要更新
        if (ipv4LastModifyTime > 0 && currentModTime <= ipv4LastModifyTime) {
            LOG.debug("IPv4 数据库无更新，跳过加载");
            return;
        }

        long fileSize = fileStatus.getLen();
        LOG.info("开始加载 IPv4 数据库: path={}, size={} MB",
                fileStatus.getPath(), fileSize / 1024 / 1024);

        // 1. 从 HDFS 下载到本地临时文件（使用文件锁避免并发问题）
        File localFile = downloadToLocalWithLock(fs, fileStatus, "ipv4", currentModTime);

        // 2. 创建新的内存映射数据库
        IpDatabaseMapped newDatabase;
        try {
            newDatabase = new IpDatabaseMapped(localFile);
        } catch (Exception e) {
            LOG.error("创建内存映射失败: {}", localFile, e);
            throw e;
        }

        // 3. 原子替换（旧的数据库保留，让正在使用的查询自然结束）
        IpDatabaseMapped oldDatabase = ipv4DatabaseRef.getAndSet(newDatabase);
        File oldFile = currentIpv4File.getAndSet(localFile);
        ipv4LastModifyTime = currentModTime;

        // 4. 延迟清理旧资源（给正在进行的查询一些时间）
        if (oldDatabase != null && oldFile != null && !oldFile.equals(localFile)) {
            scheduleOldResourceCleanup(oldDatabase, oldFile);
        }

        LOG.info("╔════════════════════════════════════════════════════════════╗");
        LOG.info("║  IPv4 数据库加载完成 (AWDB 内存映射)                         ║");
        LOG.info("║  文件大小: {} MB                                           ", fileSize / 1024 / 1024);
        LOG.info("║  本地文件: {}                                              ", localFile.getAbsolutePath());
        LOG.info("╚════════════════════════════════════════════════════════════╝");
    }

    /**
     * 从 HDFS 下载文件到本地（使用文件锁解决多进程并发问题）
     *
     * 解决场景：多个 TaskManager 或多个 slot 同时启动时，避免重复下载和重命名冲突
     */
    private File downloadToLocalWithLock(FileSystem fs, FileStatus fileStatus,
                                         String prefix, long modTime) throws IOException {
        // 使用修改时间作为文件名的一部分，确保不同版本使用不同文件
        String fileName = prefix + "-" + modTime + ".dat";
        File localFile = new File(LOCAL_TEMP_DIR, fileName);
        File lockFile = new File(LOCAL_TEMP_DIR, fileName + ".lock");

        // 快速路径：如果文件已存在且大小正确，直接使用
        if (localFile.exists() && localFile.length() == fileStatus.getLen()) {
            LOG.info("使用已缓存的本地文件: {}", localFile);
            return localFile;
        }

        // 需要下载，使用文件锁确保只有一个进程执行下载
        LOG.info("需要下载文件，尝试获取文件锁: {}", lockFile);

        try (FileOutputStream lockFos = new FileOutputStream(lockFile);
             FileChannel lockChannel = lockFos.getChannel()) {

            FileLock lock = null;
            long startTime = System.currentTimeMillis();
            long timeoutMs = FILE_LOCK_TIMEOUT_SECONDS * 1000L;

            // 尝试获取锁，带超时
            while (lock == null) {
                lock = lockChannel.tryLock();
                if (lock == null) {
                    // 等待一段时间后重试
                    if (System.currentTimeMillis() - startTime > timeoutMs) {
                        throw new IOException("获取文件锁超时: " + lockFile);
                    }
                    LOG.info("文件锁被占用，等待中... (已等待 {}ms)", System.currentTimeMillis() - startTime);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("等待文件锁时被中断", e);
                    }

                    // 再次检查文件是否已被其他进程下载完成
                    if (localFile.exists() && localFile.length() == fileStatus.getLen()) {
                        LOG.info("文件已被其他进程下载完成: {}", localFile);
                        return localFile;
                    }
                }
            }

            try {
                // 获取锁后再次检查（双重检查）
                if (localFile.exists() && localFile.length() == fileStatus.getLen()) {
                    LOG.info("文件已存在（双重检查通过）: {}", localFile);
                    return localFile;
                }

                // 执行实际下载
                return doDownload(fs, fileStatus, localFile);
            } finally {
                lock.release();
            }
        } finally {
            // 清理锁文件（best effort）
            try {
                if (lockFile.exists()) {
                    lockFile.delete();
                }
            } catch (Exception e) {
                // 忽略删除失败
            }
        }
    }

    /**
     * 执行实际的下载操作
     */
    private File doDownload(FileSystem fs, FileStatus fileStatus, File localFile) throws IOException {
        LOG.info("开始下载 HDFS 文件: {} -> {}", fileStatus.getPath(), localFile);

        // 下载到临时文件
        File tempFile = new File(localFile.getParent(), localFile.getName() + "." + Thread.currentThread().getId() + ".tmp");

        try (FSDataInputStream in = fs.open(fileStatus.getPath());
             FileOutputStream out = new FileOutputStream(tempFile)) {

            byte[] buffer = new byte[64 * 1024]; // 64KB buffer
            int bytesRead;
            long totalRead = 0;

            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                totalRead += bytesRead;
            }

            out.flush();
            LOG.info("文件下载完成: {} bytes ({} MB)", totalRead, totalRead / 1024 / 1024);
        } catch (Exception e) {
            // 下载失败，清理不完整的文件
            if (tempFile.exists()) {
                tempFile.delete();
            }
            throw e;
        }

        // 使用 Files.move 进行原子重命名（比 renameTo 更可靠）
        try {
            Files.move(tempFile.toPath(), localFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOG.info("文件重命名成功: {} -> {}", tempFile.getName(), localFile.getName());
        } catch (Exception e) {
            // 如果目标文件已存在且大小正确，说明其他进程已完成，直接使用
            if (localFile.exists() && localFile.length() == fileStatus.getLen()) {
                LOG.info("目标文件已存在（可能被其他进程创建），使用现有文件: {}", localFile);
                tempFile.delete();
                return localFile;
            }
            throw new IOException("重命名失败: " + tempFile + " -> " + localFile, e);
        }

        return localFile;
    }

    /**
     * 延迟清理旧资源
     */
    private void scheduleOldResourceCleanup(IpDatabaseMapped oldDatabase, File oldFile) {
        // 使用单独线程延迟清理，给正在进行的查询时间完成
        Thread cleanupThread = new Thread(() -> {
            try {
                // 等待 30 秒，让正在进行的查询完成
                Thread.sleep(30000);

                try {
                    oldDatabase.close();
                } catch (Exception e) {
                    LOG.debug("关闭旧数据库异常", e);
                }

                // 删除旧文件（但不删除当前正在使用的文件）
                File currentFile = currentIpv4File.get();
                if (oldFile.exists() && !oldFile.equals(currentFile)) {
                    if (oldFile.delete()) {
                        LOG.info("清理旧IP库文件: {}", oldFile.getName());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "ip-database-cleanup");

        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    // ========== IPv6 加载 ==========

    private void loadIpv6Database() throws Exception {
        FileSystem fs = getFileSystem();
        FileStatus fileStatus = getRecentFile(fs, ipv6HdfsPath);

        if (fileStatus == null) {
            LOG.warn("IPv6 数据文件不存在: {}", ipv6HdfsPath);
            return;
        }

        long currentModTime = fileStatus.getModificationTime();

        // 检查是否需要更新
        if (ipv6LastModifyTime > 0 && currentModTime <= ipv6LastModifyTime) {
            LOG.debug("IPv6 数据库无更新，跳过加载");
            return;
        }

        // 使用预分配方式读取
        byte[] data = readFileToBytesOptimized(fs, fileStatus);
        AwdbReader database = new AwdbReader(data);
        ipv6DatabaseRef.set(database);
        ipv6LastModifyTime = currentModTime;

        LOG.info("IPv6数据库(AWDB)加载完成: path={}, size={} MB",
                fileStatus.getPath(), data.length / 1024 / 1024);
    }

    /**
     * 优化的文件读取（预分配数组大小，避免扩容）
     */
    private byte[] readFileToBytesOptimized(FileSystem fs, FileStatus fileStatus) throws IOException {
        long fileSize = fileStatus.getLen();

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

    private void startHotReload() {
        reloadScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ip-database-hot-reload");
            t.setDaemon(true);
            return t;
        });

        reloadScheduler.scheduleAtFixedRate(() -> {
            try {
                if (enableIpv4HotReload) {
                    loadIpv4DatabaseMapped();
                }
                if (enableIpv6HotReload && enableIpv6) {
                    loadIpv6Database();
                }
            } catch (Exception e) {
                LOG.error("IP数据库热更新失败", e);
            }
        }, reloadIntervalSeconds, reloadIntervalSeconds, TimeUnit.SECONDS);

        LOG.info("IP数据库热更新已启动, 检查间隔: {}秒", reloadIntervalSeconds);
    }

    /**
     * 释放引用（引用计数减一）
     * 只有当引用计数归零时才真正关闭资源
     */
    @Override
    public void close() throws IOException {
        int count = REF_COUNT.decrementAndGet();
        LOG.info("IP数据库引用计数减少: {}", count);

        if (count > 0) {
            // 还有其他使用者，不关闭
            return;
        }

        // 最后一个使用者，执行真正的关闭
        synchronized (LOCK) {
            if (REF_COUNT.get() > 0) {
                // 双重检查，防止并发问题
                return;
            }

            doClose();
            INSTANCE = null;
        }
    }

    /**
     * 强制关闭（不检查引用计数，用于 JVM 关闭钩子）
     */
    public static void forceClose() {
        synchronized (LOCK) {
            if (INSTANCE != null) {
                try {
                    INSTANCE.doClose();
                } catch (Exception e) {
                    LOG.warn("强制关闭异常", e);
                }
                INSTANCE = null;
                REF_COUNT.set(0);
            }
        }
    }

    /**
     * 实际关闭操作
     */
    private void doClose() {
        LOG.info("开始关闭IP数据库...");

        // 停止热更新
        if (reloadScheduler != null) {
            reloadScheduler.shutdown();
            try {
                if (!reloadScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    reloadScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                reloadScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 关闭 IPv4 数据库
        IpDatabaseMapped ipv4Db = ipv4DatabaseRef.getAndSet(null);
        if (ipv4Db != null) {
            try {
                ipv4Db.close();
            } catch (Exception e) {
                LOG.warn("关闭IPv4数据库异常", e);
            }
        }

        // 清理临时文件（只在真正关闭时才删除）
        File ipv4File = currentIpv4File.getAndSet(null);
        if (ipv4File != null && ipv4File.exists()) {
            if (ipv4File.delete()) {
                LOG.info("删除IPv4临时文件: {}", ipv4File);
            }
        }

        ipv6DatabaseRef.set(null);
        initialized = false;

        LOG.info("IP数据库已关闭");
    }

    /**
     * 检查 IPv6 是否可用
     */
    public boolean isIpv6Enabled() {
        return ipv6DatabaseRef.get() != null;
    }

    /**
     * 获取当前引用计数（用于调试）
     */
    public static int getRefCount() {
        return REF_COUNT.get();
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

        /**
         * 构建实例（内部使用）
         */
        IpDatabaseLoader build() {
            if (ipv4HdfsPath == null || ipv4HdfsPath.isEmpty()) {
                throw new IllegalArgumentException("ipv4HdfsPath is required");
            }
            return new IpDatabaseLoader(this);
        }

        /**
         * 获取或创建单例实例（推荐使用）
         */
        public IpDatabaseLoader getOrCreate() throws Exception {
            return IpDatabaseLoader.getOrCreate(this);
        }
    }
}