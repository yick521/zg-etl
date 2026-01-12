package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.CacheServiceFactory;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import com.zhugeio.etl.common.util.Dims;
import com.zhugeio.etl.pipeline.dataquality.DataQualityContext;
import com.zhugeio.etl.pipeline.dataquality.DataQualityKafkaService;
import com.zhugeio.etl.pipeline.dataquality.DataValidator;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.transfer.DeviceTransfer;
import com.zhugeio.etl.pipeline.transfer.EventAttrTransfer;
import com.zhugeio.etl.pipeline.transfer.UserPropertyTransfer;
import com.zhugeio.etl.pipeline.transfer.UserTransfer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据路由异步算子
 *
 * 功能说明:
 * 1. 继承 RichAsyncFunction 实现异步处理
 * 2. 输出统一包装类 RouterOutput，包含所有类型的输出
 * 3. 下游使用 ProcessFunction 解包并路由到侧输出
 *
 * 注意:
 * - 关键词富化已在上游 SearchKeywordEnrichOperator 中完成
 * - IP/UA 富化已在上游 IpEnrichOperator/UserAgentEnrichOperator 中完成
 * - 本算子只负责数据转换和路由
 */
public class DataRouterOperator extends RichAsyncFunction<ZGMessage, RouterOutput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataRouterOperator.class);

    // 线程安全的时间格式化器
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    // 配置参数
    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final int localCacheSize;
    private final int localCacheExpireMinutes;
    private final String blackAppIdsStr;
    private final int expireSubDays;
    private final int expireAddDays;
    private final int eventAttrLengthLimit;
    private final boolean dataQualityEnabled;

    // 运行时组件
    private transient Set<String> blackAppIds;
    private transient ConfigCacheService configCacheService;
    private transient UserTransfer userTransfer;
    private transient DeviceTransfer deviceTransfer;
    private transient UserPropertyTransfer userPropertyTransfer;
    private transient EventAttrTransfer eventAttrTransfer;
    private transient DataValidator dataValidator;
    private transient OperatorMetrics metrics;

    // 统计
    private transient AtomicLong userCount;
    private transient AtomicLong deviceCount;
    private transient AtomicLong userPropertyCount;
    private transient AtomicLong eventAttrCount;

    public DataRouterOperator(
            String kvrocksHost, int kvrocksPort, boolean kvrocksCluster,
            int localCacheSize, int localCacheExpireMinutes,
            String blackAppIdsStr,
            int expireSubDays, int expireAddDays, int eventAttrLengthLimit,
            boolean dataQualityEnabled) {

        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.localCacheSize = localCacheSize;
        this.localCacheExpireMinutes = localCacheExpireMinutes;
        this.blackAppIdsStr = blackAppIdsStr;
        this.expireSubDays = expireSubDays;
        this.expireAddDays = expireAddDays;
        this.eventAttrLengthLimit = eventAttrLengthLimit;
        this.dataQualityEnabled = dataQualityEnabled;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "data_router_async", 30);

        // 初始化黑名单
        blackAppIds = new HashSet<>(Arrays.asList(blackAppIdsStr.split(",")));

        // 初始化缓存服务
        CacheConfig cacheConfig = CacheConfig.builder()
                .kvrocksHost(kvrocksHost)
                .kvrocksPort(kvrocksPort)
                .kvrocksCluster(kvrocksCluster)
                .build();
        configCacheService = CacheServiceFactory.getInstance(cacheConfig);

        // 初始化转换器
        userTransfer = new UserTransfer();
        deviceTransfer = new DeviceTransfer();
        userPropertyTransfer = new UserPropertyTransfer(configCacheService);
        eventAttrTransfer = new EventAttrTransfer(configCacheService, expireSubDays, expireAddDays, eventAttrLengthLimit);

        // 初始化数据质量校验器
        if (dataQualityEnabled) {
            DataQualityKafkaService dqService = DataQualityKafkaService.getInstance();
            dataValidator = new DataValidator(expireSubDays, expireAddDays, dqService);
        }

        userCount = new AtomicLong(0);
        deviceCount = new AtomicLong(0);
        userPropertyCount = new AtomicLong(0);
        eventAttrCount = new AtomicLong(0);

        LOG.info("[DataRouterAsync-{}] 初始化完成", subtaskIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void asyncInvoke(ZGMessage message, ResultFuture<RouterOutput> resultFuture) throws Exception {
        metrics.in();


        Map<String, Object> msgData = message.getData();

        // 快速跳过检查（同步，无网络调用）
        if (msgData == null) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(RouterOutput.empty()));
            return;
        }

        Integer appId = message.getAppId();
        if (appId == null || appId == 0) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(RouterOutput.empty()));
            return;
        }

        if (blackAppIds.contains(String.valueOf(appId))) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(RouterOutput.empty()));
            return;
        }

        Object dataArrayObj = msgData.get("data");
        if (!(dataArrayObj instanceof List)) {
            metrics.skip();
            resultFuture.complete(Collections.singleton(RouterOutput.empty()));
            return;
        }

        // ★★★ 异步处理消息 ★★★
        processMessageAsync(message, msgData, appId, (List<Map<String, Object>>) dataArrayObj)
                .whenComplete((output, ex) -> {
                    if (ex != null) {
                        LOG.error("[DataRouterAsync] 处理失败", ex);
                        metrics.error();
                        resultFuture.complete(Collections.singleton(RouterOutput.empty()));
                    } else {
                        metrics.out();
                        resultFuture.complete(Collections.singleton(output));
                    }
                });
    }

    /**
     * 异步处理消息 - 无阻塞
     */
    private CompletableFuture<RouterOutput> processMessageAsync(ZGMessage message, Map<String, Object> msgData,
                                                                Integer appId, List<Map<String, Object>> dataList) {
        String business = message.getBusiness();
        Integer platform = Dims.sdk(getStringValue(msgData, "pl"));
        String ua = getStringValue(msgData, "ua");
        String ip = getStringValue(msgData, "ip");
        String sdk = getStringValue(msgData, "sdk");
        String pl = getStringValue(msgData, "pl");
        Long msgSt = getLongValue(msgData, "st", null);

        String deviceMd5 = null;
        Object usrObj = msgData.get("usr");
        if (usrObj instanceof Map) {
            Map<String, Object> usr = (Map<String, Object>) usrObj;
            deviceMd5 = getStringValue(usr, "did");
        }

        String[] ipResult = extractIpResult(msgData);
        Map<String, String> uaResult = extractUaResult(msgData);

        if (business == null || business.isEmpty()) {
            business = getStringValue(msgData, "business", "\\N");
        }

        RouterOutput output = new RouterOutput();
        String rawJson = message.getRawData();

        // 收集所有异步任务
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map<String, Object> dataItem : dataList) {
            String dt = getStringValue(dataItem, "dt");
            if (dt == null || dt.isEmpty()) {
                continue;
            }

            Object prObj = dataItem.get("pr");
            if (!(prObj instanceof Map)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> pr = (Map<String, Object>) prObj;

            // 异步路由处理
            CompletableFuture<Void> future = routeByDtAsync(output, dt, appId, platform, pr,
                    ip, ipResult, ua, uaResult, business, msgSt, deviceMd5, sdk, pl, rawJson);
            futures.add(future);
        }

        // 等待所有异步任务完成
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> output);
    }

    /**
     * 异步路由处理
     */
    private CompletableFuture<Void> routeByDtAsync(RouterOutput output, String dt,
                                                   Integer appId, Integer platform, Map<String, Object> pr,
                                                   String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                                   String business, Long msgSt, String deviceMd5,
                                                   String sdk, String pl, String rawJson) {

        switch (dt) {
            case "zgid":
                // UserTransfer 是同步的（无网络调用）
                UserRow userRow = userTransfer.transferFromMap(appId, platform, pr);
                if (userRow != null) {
                    output.addUserRow(userRow);
                    userCount.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);

            case "pl":
                // DeviceTransfer 是同步的（无网络调用）
                DeviceRow deviceRow = deviceTransfer.transferFromMap(appId, platform, pr, msgSt, deviceMd5);
                if (deviceRow != null) {
                    output.addDeviceRow(deviceRow);
                    deviceCount.incrementAndGet();
                }
                return CompletableFuture.completedFuture(null);

            case "usr":
                return userPropertyTransfer.transferFromMapAsync(appId, platform, pr)
                        .thenAccept(propRows -> {
                            for (UserPropertyRow propRow : propRows) {
                                output.addUserPropertyRow(propRow);
                                userPropertyCount.incrementAndGet();
                            }
                        });

            case "evt":
            case "vtl":
            case "mkt":
            case "ss":
            case "se":
            case "abp":
                return processEventAttrAsync(output, dt, appId, platform, pr, ip, ipResult, ua, uaResult,
                        business, sdk, pl, rawJson);

            default:
                return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * 异步处理事件属性
     */
    private CompletableFuture<Void> processEventAttrAsync(RouterOutput output, String dt,
                                                          Integer appId, Integer platform, Map<String, Object> pr,
                                                          String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                                          String business, String sdk, String pl, String rawJson) {

        String zgZgid = getStringValue(pr, "$zg_zgid");
        String zgEid = getStringValue(pr, "$zg_eid");
        String zgDid = getStringValue(pr, "$zg_did");
        Long ct = getLongValue(pr, "$ct", null);
        Integer tz = getIntValue(pr, "$tz", null);
        String eventName = getStringValue(pr, "$eid", "\\N");

        String realTime = formatTime(ct, tz);

        // 数据质量校验（同步）
        if (dataQualityEnabled && dataValidator != null) {
            DataQualityContext dqCtx = DataQualityContext.builder()
                    .appId(appId)
                    .platform(platform)
                    .eventName(eventName)
                    .eventId(zgEid)
                    .zgZgid(zgZgid)
                    .zgDid(zgDid)
                    .ct(ct)
                    .tz(tz != null ? String.valueOf(tz) : null)
                    .sdk(sdk)
                    .pl(pl)
                    .rawJson(rawJson)
                    .build();

            DataValidator.ValidationResult result = dataValidator.validate(dqCtx, realTime);
            if (result.isFailed()) {
                return CompletableFuture.completedFuture(null);
            }
        }

        return eventAttrTransfer.transferFromMapAsync(appId, platform, dt, pr, ip, ipResult, ua, uaResult, business)
                .thenAccept(eventRow -> {
                    if (eventRow != null) {
                        output.addEventAttrRow(eventRow);
                        eventAttrCount.incrementAndGet();
                    }
                });
    }

    private String[] extractIpResult(Map<String, Object> msgData) {
        return new String[]{
                getStringValue(msgData, "country", "\\N"),
                getStringValue(msgData, "province", "\\N"),
                getStringValue(msgData, "city", "\\N")
        };
    }

    private Map<String, String> extractUaResult(Map<String, Object> msgData) {
        Map<String, String> result = new HashMap<>(4);
        result.put("os", getStringValue(msgData, "os", null));
        result.put("os_version", getStringValue(msgData, "os_version", null));
        result.put("browser", getStringValue(msgData, "browser", null));
        result.put("browser_version", getStringValue(msgData, "browser_version", null));
        return result;
    }

    private String formatTime(Long ct, Integer tz) {
        if (ct == null || tz == null) {
            return "\\N";
        }
        try {
            if (Math.abs(tz) > 48 * 3600 * 1000) {
                return "\\N";
            }
            String result = DATE_FORMATTER.format(Instant.ofEpochMilli(ct));
            return result.length() > 19 ? "\\N" : result;
        } catch (Exception e) {
            return "\\N";
        }
    }

    private String getStringValue(Map<String, Object> map, String key) {
        return getStringValue(map, key, null);
    }

    private String getStringValue(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        return String.valueOf(value);
    }

    private Integer getIntValue(Map<String, Object> map, String key, Integer defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private Long getLongValue(Map<String, Object> map, String key, Long defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public void timeout(ZGMessage input, ResultFuture<RouterOutput> resultFuture) throws Exception {
        LOG.warn("处理超时: offset={}", input.getOffset());
        metrics.error();
        resultFuture.complete(Collections.singleton(RouterOutput.empty()));
    }

    @Override
    public void close() throws Exception {
        LOG.info("[DataRouterAsync] Metrics: in={}, out={}, error={}, skip={}",
                metrics.getIn(), metrics.getOut(), metrics.getError(), metrics.getSkip());
        LOG.info("[DataRouterAsync] Stats: user={}, device={}, userProp={}, eventAttr={}",
                userCount.get(), deviceCount.get(), userPropertyCount.get(), eventAttrCount.get());

        if (metrics != null) {
            metrics.shutdown();
        }
    }
}
