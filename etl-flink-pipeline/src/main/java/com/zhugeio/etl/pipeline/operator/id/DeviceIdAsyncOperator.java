package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.common.util.SnowflakeIdGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 设备ID异步映射算子 (雪花算法版本)
 * <p>
 * 优化点:
 * 1. ✅ 使用雪花算法生成ID,无需同步调用KVRocks incr
 * 2. ✅ 所有操作都是异步的
 * 3. ✅ workerId范围: 0-255 (与其他算子隔离)
 */
public class DeviceIdAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    private transient KvrocksClient kvrocks;
    private transient Cache<String, Long> deviceCache;
    private transient SnowflakeIdGenerator idGenerator;  // ✅ 新增

    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;

    public DeviceIdAsyncOperator(String kvrocksHost, int kvrocksPort, boolean kvrocksCluster) {
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
    }

    @Override
    public void open(Configuration parameters) {
        // 初始化KVRocks客户端
        kvrocks = new KvrocksClient(kvrocksHost, kvrocksPort, kvrocksCluster);
        kvrocks.init();

        // 测试连接
        if (!kvrocks.testConnection()) {
            throw new RuntimeException("KVRocks连接失败!");
        }

        // ✅ 初始化雪花算法生成器
        // 使用主机名+slot ID生成workerId
        int workerId = generateSnowflakeWorkerId();
        int totalSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        if (totalSubtasks > 1024) {
            throw new RuntimeException(
                    "DeviceId算子最多支持1024个并行度,当前: " + totalSubtasks);
        }

        idGenerator = new SnowflakeIdGenerator(workerId);

        // 初始化Caffeine缓存
        deviceCache = Caffeine.newBuilder()
                .maximumSize(100000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .recordStats()
                .build();

        System.out.printf(
                "[DeviceId算子-%d] 初始化成功, KVRocks: %s:%d, workerId=%d%n",
                getRuntimeContext().getIndexOfThisSubtask(), kvrocksHost, kvrocksPort, workerId
        );
    }

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {

        JSONObject usr = (JSONObject)input.getData().get("usr");
        String did = usr.get("did").toString();
        boolean isExistDid = StringUtils.isNotBlank(did);

        if(input.getResult() != -1 && isExistDid){
            String cacheKey = "d:" + input.getAppId() + ":" + did;

            Long cachedDeviceId = deviceCache.getIfPresent(cacheKey);

            if (cachedDeviceId != null) {
                ZGMessage output = createOutput(input, cachedDeviceId, false);
                resultFuture.complete(Collections.singleton(output));
                return;
            }

            kvrocks.asyncGet(cacheKey).thenCompose(zgDeviceIdStr -> {
                if (zgDeviceIdStr != null) {
                    Long zgDeviceId = Long.parseLong(zgDeviceIdStr);
                    deviceCache.put(cacheKey, zgDeviceId);
                    ZGMessage output = createOutput(input, zgDeviceId, false);
                    return CompletableFuture.completedFuture(output);
                } else {
                    Long newDeviceId = idGenerator.nextId();
                    // 使用asyncHSetIfAbsent替代asyncSet，避免并发问题
                    kvrocks.asyncHSetIfAbsent("device_ids", cacheKey, newDeviceId.toString())
                            .whenComplete((result, throwable) -> {
                                if (throwable != null) {
                                    System.err.println("DeviceId写入失败: " + cacheKey + ":" + newDeviceId);
                                } else if (Boolean.TRUE.equals(result)) {
                                    // 成功设置了新值
                                    System.out.println("成功为设备分配新ID: " + cacheKey + " -> " + newDeviceId);
                                } else {
                                    // 值已经存在（可能被其他实例设置了）
                                    System.out.println("设备ID已存在: " + cacheKey);
                                }
                            });

                    deviceCache.put(cacheKey, newDeviceId);
                    return CompletableFuture.completedFuture(
                            createOutput(input, newDeviceId, true));
                }
            }).whenComplete((output, throwable) -> {
                if (throwable != null) {
                    resultFuture.completeExceptionally(throwable);
                } else {
                    resultFuture.complete(Collections.singleton(output));
                }
            });
        }else {
            resultFuture.complete(Collections.singleton(input));
        }
    }

    private ZGMessage createOutput(ZGMessage input, Long zgDeviceId, boolean isNew) {
        JSONObject data = (JSONObject) input.getData();
        if (data == null) {
            throw new IllegalArgumentException("input data is not a JSONObject");
        }

        JSONObject usr = (JSONObject) data.get("usr");
        if (usr != null) {
            usr.put("$zg_did", zgDeviceId);
        }

        if (data.get("data") instanceof JSONArray) {
            JSONArray dataArray = (JSONArray) data.get("data");
            for (Object item : dataArray) {
                if (item instanceof JSONObject) {
                    JSONObject jsonItem = (JSONObject) item;
                    Object pr = jsonItem.get("pr");
                    if (pr instanceof JSONObject) {
                        JSONObject prObject = (JSONObject) pr;
                        prObject.put("$zg_did", zgDeviceId);
                    }
                }
            }
        }

        return input;
    }

    @Override
    public void close() {
        if (kvrocks != null) {
            kvrocks.shutdown();
        }

        if (deviceCache != null) {
            System.out.printf(
                    "[DeviceId算子-%d] 缓存统计: %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    deviceCache.stats()
            );
        }
    }

    /**
     * 使用主机名和slot ID组合生成workerID，确保在分布式环境中的唯一性
     * @return workerId
     */
    private int generateSnowflakeWorkerId() {
        try {
            // 使用主机信息和 slot 组合来生成 workerId
            String hostName = java.net.InetAddress.getLocalHost().getHostName();
            int slotId = getRuntimeContext().getIndexOfThisSubtask();

            return Math.abs((hostName.hashCode() * 31 + slotId)) % 256;
        } catch (Exception e) {
            // 回退到 subtask index
            return getRuntimeContext().getIndexOfThisSubtask() % 256;
        }
    }

}