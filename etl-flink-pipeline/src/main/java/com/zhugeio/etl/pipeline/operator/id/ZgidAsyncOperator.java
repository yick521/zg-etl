package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.SnowflakeIdGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 诸葛ID映射算子 (完整版 - 支持用户设备绑定)
 * 核心逻辑:
 * 1. 从 pr 对象中获取 $zg_uid 和 $zg_did
 * 2. 优先使用用户维度 ($zg_uid) 查找zgid
 * 3. 如果用户维度存在zgid，将设备也绑定到该zgid
 * 4. 如果用户维度不存在，查找设备维度
 * 5. 如果设备存在zgid但未绑定用户，建立绑定关系
 * 6. 如果都不存在，生成新的zgid
 * 7. 将 $zg_zgid 写入 pr 对象
 */
public class ZgidAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, Long> localCache;
    private transient SnowflakeIdGenerator idGenerator;

    // 监控指标
    private transient AtomicLong writeFailureCount;
    private transient AtomicLong totalWriteCount;
    private transient AtomicLong newZgidCount;
    private transient AtomicLong bindingCount;

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final boolean waitForWrite;

    public ZgidAsyncOperator() {
        this(null, 0, true, false);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort) {
        this(kvrocksHost, kvrocksPort, true, false);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this(kvrocksHost, kvrocksPort, kvrocksCluster, false);
    }

    public ZgidAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster, boolean waitForWrite) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.waitForWrite = waitForWrite;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        int workerId = generateSnowflakeWorkerId();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 1024) {
            throw new RuntimeException(
                    "Zgid算子最多支持1024个并行度,当前: " + totalSubtasks);
        }

        idGenerator = new SnowflakeIdGenerator(workerId);

        localCache = Caffeine.newBuilder()
                .maximumSize(20000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();

        // 初始化监控指标
        writeFailureCount = new AtomicLong(0);
        totalWriteCount = new AtomicLong(0);
        newZgidCount = new AtomicLong(0);
        bindingCount = new AtomicLong(0);

        System.out.printf(
                "[Zgid算子-%d] 雪花算法初始化成功, workerId=%d, waitForWrite=%s%n",
                getRuntimeContext().getIndexOfThisSubtask(), workerId, waitForWrite
        );
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        JSONArray dataArray = (JSONArray) input.getData().get("data");
        Integer appId = input.getAppId();

        if (dataArray == null || dataArray.isEmpty()) {
            resultFuture.complete(Collections.singleton(input));
            return;
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // 处理 data 数组中的所有事件项
        for (int i = 0; i < dataArray.size(); i++) {
            JSONObject item = dataArray.getJSONObject(i);
            if (item == null) continue;

            Object pr = item.get("pr");
            if (!(pr instanceof JSONObject)) continue;

            JSONObject prObject = (JSONObject) pr;

            // 从 pr 对象中提取 $zg_uid 和 $zg_did
            Long zgUserId = prObject.getLong("$zg_uid");   // 可能为null
            Long zgDeviceId = prObject.getLong("$zg_did"); // 必须存在

            if (zgDeviceId == null) {
                // 没有设备ID，无法处理
                continue;
            }

            String appIdStr = String.valueOf(appId);

            String userKey = zgUserId != null ? "uz:" + appIdStr + ":" + zgUserId : null;
            String deviceKey = "dz:" + appIdStr + ":" + zgDeviceId;

            if (userKey != null) {
                Long cachedUserZgid = localCache.getIfPresent(userKey);
                if (cachedUserZgid != null) {
                    prObject.put("$zg_zgid", cachedUserZgid);
                    asyncBindDeviceIfNeeded(deviceKey, cachedUserZgid);
                    continue;
                }
            }

            Long cachedDeviceZgid = localCache.getIfPresent(deviceKey);
            if (cachedDeviceZgid != null && userKey == null) {
                prObject.put("$zg_zgid", cachedDeviceZgid);
                continue;
            }

            CompletableFuture<Void> future = resolveZgid(
                    prObject, appIdStr, zgUserId, zgDeviceId, userKey, deviceKey
            );
            futures.add(future);
        }

        if (futures.isEmpty()) {
            resultFuture.complete(Collections.singleton(input));
        } else {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .whenComplete((v, throwable) -> {
                        if (throwable != null) {
                            System.err.println(String.format(
                                    "[Zgid算子-%d] 批量处理Zgid时出错",
                                    getRuntimeContext().getIndexOfThisSubtask()
                            ));
                            throwable.printStackTrace();
                        }
                        resultFuture.complete(Collections.singleton(input));
                    });
        }
    }

    /**
     * 完整的zgid解析逻辑（参考流程图）
     * 返回CompletableFuture<Void>，直接修改prObject
     */
    private CompletableFuture<Void> resolveZgid(JSONObject prObject, String appId, Long zgUserId, Long zgDeviceId,
                                                String userKey, String deviceKey) {

        // 并行查询用户和设备的zgid
        CompletableFuture<String> userZgidFuture = userKey != null
                ? kvrocks.asyncGet(userKey)
                : CompletableFuture.completedFuture(null);

        CompletableFuture<String> deviceZgidFuture = kvrocks.asyncGet(deviceKey);

        return CompletableFuture.allOf(userZgidFuture, deviceZgidFuture)
                .thenCompose(v -> {
                    String userZgidStr = userZgidFuture.join();
                    String deviceZgidStr = deviceZgidFuture.join();


                    // 实名用户
                    if (zgUserId != null) {
                        if (userZgidStr == null) {
                            if (deviceZgidStr == null) {
                                return createNewZgidAndBind(appId, zgUserId, zgDeviceId, userKey, deviceKey);
                            } else {
                                Long existingZgid = Long.parseLong(deviceZgidStr);
                                return bindUserToExistingZgid(appId, zgUserId, zgDeviceId, userKey, deviceKey, existingZgid);
                            }
                        } else {
                            Long userZgid = Long.parseLong(userZgidStr);
                            if (deviceZgidStr == null || !deviceZgidStr.equals(userZgidStr)) {
                                return bindDeviceToUser(appId, zgDeviceId, deviceKey, userZgid);
                            } else {
                                return CompletableFuture.completedFuture(new ZgidResult(userZgid, false));
                            }
                        }
                    } else {
                        // 匿名用户
                        if (deviceZgidStr != null) {
                            Long deviceZgid = Long.parseLong(deviceZgidStr);
                            return CompletableFuture.completedFuture(new ZgidResult(deviceZgid, false));
                        } else {
                            return createNewZgidForDevice(appId, zgDeviceId, deviceKey);
                        }
                    }
                })
                .thenAccept(result -> {
                    prObject.put("$zg_zgid", result.zgid);
                })
                .exceptionally(throwable -> {
                    System.err.println(String.format(
                            "[Zgid算子-%d] 处理失败: appId=%s, userId=%s, deviceId=%s, error=%s",
                            getRuntimeContext().getIndexOfThisSubtask(),
                            appId, zgUserId, zgDeviceId, throwable.getMessage()
                    ));
                    throwable.printStackTrace();
                    return null;
                });
    }

    /**
     * 创建新zgid并绑定用户和设备
     */
    private CompletableFuture<ZgidResult> createNewZgidAndBind(String appId, Long zgUserId,
                                                               Long zgDeviceId, String userKey,
                                                               String deviceKey) {
        Long newZgid = idGenerator.nextId();
        newZgidCount.incrementAndGet();

        String zgidStr = String.valueOf(newZgid);
        String reverseKey = "zu:" + appId + ":" + newZgid;

        localCache.put(userKey, newZgid);
        localCache.put(deviceKey, newZgid);

        if (waitForWrite) {
            return writeMultipleKeys(
                    new KeyValue(userKey, zgidStr),
                    new KeyValue(deviceKey, zgidStr)
            ).thenApply(v -> new ZgidResult(newZgid, true));
        } else {
            writeMultipleKeysAsync(
                    new KeyValue(userKey, zgidStr),
                    new KeyValue(deviceKey, zgidStr)
            );
            return CompletableFuture.completedFuture(new ZgidResult(newZgid, true));
        }
    }

    /**
     * 绑定用户到已存在的zgid（新用户，老设备）
     */
    private CompletableFuture<ZgidResult> bindUserToExistingZgid(String appId, Long zgUserId,
                                                                 Long zgDeviceId, String userKey,
                                                                 String deviceKey, Long existingZgid) {
        bindingCount.incrementAndGet();

        String zgidStr = String.valueOf(existingZgid);

        localCache.put(userKey, existingZgid);
        localCache.put(deviceKey, existingZgid);

        if (waitForWrite) {
            return kvrocks.asyncSet(userKey, zgidStr)
                    .thenApply(v -> new ZgidResult(existingZgid, false));
        } else {
            kvrocks.asyncSet(userKey, zgidStr);
            return CompletableFuture.completedFuture(new ZgidResult(existingZgid, false));
        }
    }

    /**
     * 绑定设备到用户的zgid（老用户，新设备）
     */
    private CompletableFuture<ZgidResult> bindDeviceToUser(String appId, Long zgDeviceId,
                                                           String deviceKey, Long userZgid) {
        bindingCount.incrementAndGet();

        String zgidStr = String.valueOf(userZgid);
        localCache.put(deviceKey, userZgid);

        if (waitForWrite) {
            return kvrocks.asyncSet(deviceKey, zgidStr)
                    .thenApply(v -> new ZgidResult(userZgid, false));
        } else {
            kvrocks.asyncSet(deviceKey, zgidStr);
            return CompletableFuture.completedFuture(new ZgidResult(userZgid, false));
        }
    }

    /**
     * 为新设备创建新zgid（匿名用户）
     */
    private CompletableFuture<ZgidResult> createNewZgidForDevice(String appId, Long zgDeviceId,
                                                                 String deviceKey) {
        Long newZgid = idGenerator.nextId();
        newZgidCount.incrementAndGet();

        String zgidStr = String.valueOf(newZgid);
        localCache.put(deviceKey, newZgid);

        if (waitForWrite) {
            return kvrocks.asyncSet(deviceKey, zgidStr)
                    .thenApply(v -> new ZgidResult(newZgid, true));
        } else {
            kvrocks.asyncSet(deviceKey, zgidStr);
            return CompletableFuture.completedFuture(new ZgidResult(newZgid, true));
        }
    }

    /**
     * 异步绑定设备（如果还没绑定）- Fire-and-Forget
     */
    private void asyncBindDeviceIfNeeded(String deviceKey, Long zgid) {
        Long cachedDeviceZgid = localCache.getIfPresent(deviceKey);
        if (cachedDeviceZgid == null || !cachedDeviceZgid.equals(zgid)) {
            localCache.put(deviceKey, zgid);
            kvrocks.asyncSet(deviceKey, String.valueOf(zgid))
                    .exceptionally(ex -> {
                        System.err.println(String.format(
                                "[Zgid算子-%d] 设备绑定失败: key=%s, zgid=%d",
                                getRuntimeContext().getIndexOfThisSubtask(), deviceKey, zgid
                        ));
                        return null;
                    });
        }
    }

    /**
     * 批量写入多个key
     */
    private CompletableFuture<Void> writeMultipleKeys(KeyValue... keyValues) {
        totalWriteCount.addAndGet(keyValues.length);

        CompletableFuture<Void>[] futures = new CompletableFuture[keyValues.length];
        for (int i = 0; i < keyValues.length; i++) {
            KeyValue kv = keyValues[i];
            futures[i] = kvrocks.asyncSet(kv.key, kv.value);
        }

        return CompletableFuture.allOf(futures)
                .exceptionally(throwable -> {
                    writeFailureCount.incrementAndGet();
                    throw new RuntimeException("批量写入失败", throwable);
                });
    }

    /**
     * 批量写入多个key - Fire-and-Forget
     */
    private void writeMultipleKeysAsync(KeyValue... keyValues) {
        totalWriteCount.addAndGet(keyValues.length);

        for (KeyValue kv : keyValues) {
            kvrocks.asyncSet(kv.key, kv.value)
                    .exceptionally(ex -> {
                        writeFailureCount.incrementAndGet();
                        System.err.println(String.format(
                                "[Zgid算子-%d] 写入失败: key=%s, value=%s, error=%s",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                kv.key, kv.value, ex.getMessage()
                        ));
                        return null;
                    });
        }
    }

    @Override
    public void close() throws Exception {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        // 输出监控统计
        if (totalWriteCount != null && writeFailureCount != null) {
            long total = totalWriteCount.get();
            long failed = writeFailureCount.get();
            long newZgids = newZgidCount.get();
            long bindings = bindingCount.get();
            double failureRate = total > 0 ? (failed * 100.0 / total) : 0;

            System.out.printf(
                    "[Zgid算子-%d] 关闭统计: 总写入=%d, 失败=%d (%.2f%%), 新建zgid=%d, 绑定操作=%d%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    total, failed, failureRate, newZgids, bindings
            );
        }

        if (localCache != null) {
            System.out.printf(
                    "[Zgid算子-%d] 缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    localCache.stats()
            );
        }
    }

    private int generateSnowflakeWorkerId() {
        try {
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            int slotId = getRuntimeContext().getIndexOfThisSubtask();
            return Math.abs((hostName.hashCode() * 31 + slotId)) % 256;
        } catch (Exception e) {
            return getRuntimeContext().getIndexOfThisSubtask() % 256;
        }
    }

    private static class ZgidResult {
        final Long zgid;
        final boolean isNew;

        ZgidResult(Long zgid, boolean isNew) {
            this.zgid = zgid;
            this.isNew = isNew;
        }
    }

    private static class KeyValue {
        final String key;
        final String value;

        KeyValue(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}