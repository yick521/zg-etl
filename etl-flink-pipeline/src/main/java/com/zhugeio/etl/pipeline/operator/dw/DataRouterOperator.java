package com.zhugeio.etl.pipeline.operator.dw;

import com.zhugeio.etl.common.cache.CacheConfig;
import com.zhugeio.etl.common.cache.ConfigCacheService;
import com.zhugeio.etl.common.metrics.OperatorMetrics;
import com.zhugeio.etl.common.model.DeviceRow;
import com.zhugeio.etl.common.model.EventAttrRow;
import com.zhugeio.etl.common.model.UserPropertyRow;
import com.zhugeio.etl.common.model.UserRow;
import com.zhugeio.etl.pipeline.dataquality.DataQualityContext;
import com.zhugeio.etl.pipeline.dataquality.DataQualityKafkaService;
import com.zhugeio.etl.pipeline.dataquality.DataValidator;
import com.zhugeio.etl.pipeline.entity.ZGMessage;
import com.zhugeio.etl.pipeline.service.BaiduKeywordService;
import com.zhugeio.etl.pipeline.transfer.DeviceTransfer;
import com.zhugeio.etl.pipeline.transfer.EventAttrTransfer;
import com.zhugeio.etl.pipeline.transfer.UserPropertyTransfer;
import com.zhugeio.etl.pipeline.transfer.UserTransfer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据路由异步算子
 * 
 *
 * 1. 继承 RichAsyncFunction 实现异步处理
 * 2. 输出统一包装类 RouterOutput，包含所有类型的输出
 * 3. 下游使用 ProcessFunction 解包并路由到侧输出
 *
 * ```
 */
public class DataRouterOperator extends RichAsyncFunction<ZGMessage, RouterOutput> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataRouterOperator.class);

    // 配置参数
    private final String kvrocksHost;
    private final int kvrocksPort;
    private final boolean kvrocksCluster;
    private final int localCacheSize;
    private final int localCacheExpireMinutes;
    private final String blackAppIdsStr;
    private final String whiteAppIdsStr;
    private final int expireSubDays;
    private final int expireAddDays;
    private final int eventAttrLengthLimit;
    private final boolean dataQualityEnabled;
    
    // 百度关键词配置
    private final String baiduUrl;
    private final String baiduId;
    private final String baiduKey;
    private final String redisHost;
    private final int redisPort;
    private final boolean redisCluster;
    private final int requestSocketTimeout;
    private final int requestConnectTimeout;

    // 运行时组件 (transient)
    private transient Set<String> blackAppIds;
    private transient Set<String> whiteAppIds;
    private transient ConfigCacheService configCacheService;
    private transient BaiduKeywordService keywordService;
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
    private transient AtomicLong errorCount;

    public DataRouterOperator(
            String kvrocksHost, int kvrocksPort, boolean kvrocksCluster,
            int localCacheSize, int localCacheExpireMinutes,
            String blackAppIdsStr, String whiteAppIdsStr,
            int expireSubDays, int expireAddDays, int eventAttrLengthLimit,
            boolean dataQualityEnabled,
            String baiduUrl, String baiduId, String baiduKey,
            String redisHost, int redisPort, boolean redisCluster,
            int requestSocketTimeout, int requestConnectTimeout) {
        
        this.kvrocksHost = kvrocksHost;
        this.kvrocksPort = kvrocksPort;
        this.kvrocksCluster = kvrocksCluster;
        this.localCacheSize = localCacheSize;
        this.localCacheExpireMinutes = localCacheExpireMinutes;
        this.blackAppIdsStr = blackAppIdsStr;
        this.whiteAppIdsStr = whiteAppIdsStr;
        this.expireSubDays = expireSubDays;
        this.expireAddDays = expireAddDays;
        this.eventAttrLengthLimit = eventAttrLengthLimit;
        this.dataQualityEnabled = dataQualityEnabled;
        this.baiduUrl = baiduUrl;
        this.baiduId = baiduId;
        this.baiduKey = baiduKey;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisCluster = redisCluster;
        this.requestSocketTimeout = requestSocketTimeout;
        this.requestConnectTimeout = requestConnectTimeout;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        metrics = OperatorMetrics.create(getRuntimeContext().getMetricGroup(), "data_router_async");

        // 初始化黑白名单
        blackAppIds = new HashSet<>(Arrays.asList(blackAppIdsStr.split(",")));
        whiteAppIds = new HashSet<>();
        if (whiteAppIdsStr != null && !whiteAppIdsStr.isEmpty()) {
            whiteAppIds.addAll(Arrays.asList(whiteAppIdsStr.split(",")));
        }

        // 初始化缓存服务
        configCacheService = new ConfigCacheService(
                CacheConfig.builder()
                        .kvrocksHost(kvrocksHost)
                        .kvrocksPort(kvrocksPort)
                        .kvrocksCluster(kvrocksCluster)
                        .appCacheSize(localCacheSize)
                        .appCacheExpireMinutes(localCacheExpireMinutes)
                        .build()
        );
        configCacheService.init();

        // 初始化百度关键词服务
        keywordService = new BaiduKeywordService(
                baiduUrl, baiduId, baiduKey,
                redisHost, redisPort, redisCluster,
                requestSocketTimeout, requestConnectTimeout
        );
        keywordService.init();

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

        // 初始化计数器
        userCount = new AtomicLong(0);
        deviceCount = new AtomicLong(0);
        userPropertyCount = new AtomicLong(0);
        eventAttrCount = new AtomicLong(0);
        errorCount = new AtomicLong(0);

        LOG.info("[DataRouterAsync-{}] 初始化完成", getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void asyncInvoke(ZGMessage message, ResultFuture<RouterOutput> resultFuture) throws Exception {
        metrics.in();

        CompletableFuture.supplyAsync(() -> {
            try {
                return processMessage(message);
            } catch (Exception e) {
                LOG.error("处理消息失败", e);
                errorCount.incrementAndGet();
                metrics.error();
                return RouterOutput.empty();
            }
        }).whenComplete((output, throwable) -> {
            if (throwable != null) {
                LOG.error("异步处理异常", throwable);
                errorCount.incrementAndGet();
                metrics.error();
                resultFuture.complete(Collections.singleton(RouterOutput.empty()));
            } else {
                metrics.out();
                resultFuture.complete(Collections.singleton(output));
            }
        });
    }

    /**
     * 处理消息 - 核心逻辑
     */
    private RouterOutput processMessage(ZGMessage message) {
        // 版本检查
        configCacheService.checkAndRefreshVersion();

        Map<String, Object> msgData = message.getData();
        if (msgData == null) {
            metrics.skip();
            return RouterOutput.empty();
        }

        Integer appId = message.getAppId();
        String business = message.getBusiness();

        if (appId == null || appId == 0) {
            metrics.skip();
            return RouterOutput.empty();
        }

        // 黑名单检查
        if (blackAppIds.contains(String.valueOf(appId))) {
            metrics.skip();
            return RouterOutput.empty();
        }

        // 提取基础信息
        Integer platform = getIntValue(msgData, "plat", 0);
        String ua = getStringValue(msgData, "ua");
        String ip = getStringValue(msgData, "ip");
        String sdk = getStringValue(msgData, "sdk");
        String pl = getStringValue(msgData, "pl");
        Long msgSt = getLongValue(msgData, "st", null);

        // 获取设备 MD5
        String deviceMd5 = null;
        Object usrObj = msgData.get("usr");
        if (usrObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> usr = (Map<String, Object>) usrObj;
            deviceMd5 = getStringValue(usr, "did");
        }

        // 提取富化数据
        String[] ipResult = extractIpResult(msgData);
        Map<String, String> uaResult = extractUaResult(msgData);
        if (business == null || business.isEmpty()) {
            business = getStringValue(msgData, "business", "\\N");
        }

        // 获取 data 数组
        Object dataObj = msgData.get("data");
        if (dataObj == null) {
            metrics.skip();
            return RouterOutput.empty();
        }

        List<?> dataList = (List<?>) dataObj;
        if (dataList.isEmpty()) {
            metrics.skip();
            return RouterOutput.empty();
        }

        // 预加载关键词
        Map<String, String> eqidKeywords = preloadKeywords(appId, dataList);

        // 构建输出
        RouterOutput output = new RouterOutput();
        String rawJson = message.getRawData();

        // 处理每个数据项
        for (Object item : dataList) {
            if (item == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> dataItem = (Map<String, Object>) item;

            String dt = getStringValue(dataItem, "dt");
            @SuppressWarnings("unchecked")
            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");

            if (dt == null || pr == null) {
                continue;
            }

            // 根据 dt 类型路由
            routeByDt(output, dt, appId, platform, pr, ip, ipResult, ua, uaResult,
                    business, eqidKeywords, msgSt, deviceMd5, sdk, pl, rawJson);
        }

        return output;
    }

    /**
     * 根据 dt 类型路由数据
     */
    private void routeByDt(RouterOutput output, String dt,
                          Integer appId, Integer platform, Map<String, Object> pr,
                          String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                          String business, Map<String, String> eqidKeywords,
                          Long msgSt, String deviceMd5, String sdk, String pl, String rawJson) {

        switch (dt) {
            case "zgid":
                UserRow userRow = userTransfer.transferFromMap(appId, platform, pr);
                if (userRow != null) {
                    output.addUserRow(userRow);
                    userCount.incrementAndGet();
                }
                break;

            case "pl":
                DeviceRow deviceRow = deviceTransfer.transferFromMap(appId, platform, pr, msgSt, deviceMd5);
                if (deviceRow != null) {
                    output.addDeviceRow(deviceRow);
                    deviceCount.incrementAndGet();
                }
                break;

            case "usr":
                List<UserPropertyRow> propRows = userPropertyTransfer.transferFromMap(appId, platform, pr);
                for (UserPropertyRow propRow : propRows) {
                    output.addUserPropertyRow(propRow);
                    userPropertyCount.incrementAndGet();
                }
                break;

            case "evt":
            case "vtl":
            case "mkt":
            case "ss":
            case "se":
            case "abp":
                processEventAttr(output, dt, appId, platform, pr, ip, ipResult, ua, uaResult,
                        business, eqidKeywords, sdk, pl, rawJson);
                break;

            default:
                break;
        }
    }

    /**
     * 处理事件属性
     */
    private void processEventAttr(RouterOutput output, String dt,
                                  Integer appId, Integer platform, Map<String, Object> pr,
                                  String ip, String[] ipResult, String ua, Map<String, String> uaResult,
                                  String business, Map<String, String> eqidKeywords,
                                  String sdk, String pl, String rawJson) {

        String zgZgid = getStringValue(pr, "$zg_zgid");
        String zgEid = getStringValue(pr, "$zg_eid");
        String zgDid = getStringValue(pr, "$zg_did");
        Long ct = getLongValue(pr, "$ct", null);
        String tz = getStringValue(pr, "$tz");
        String eventName = getStringValue(pr, "$eid", "\\N");

        String realTime = formatTime(ct, tz);

        // 数据质量校验
        if (dataQualityEnabled && dataValidator != null) {
            DataQualityContext dqCtx = DataQualityContext.builder()
                    .appId(appId)
                    .platform(platform)
                    .eventName(eventName)
                    .eventId(zgEid)
                    .zgZgid(zgZgid)
                    .zgDid(zgDid)
                    .ct(ct)
                    .tz(tz)
                    .sdk(sdk)
                    .pl(pl)
                    .rawJson(rawJson)
                    .build();

            DataValidator.ValidationResult result = dataValidator.validate(dqCtx, realTime);
            if (result.isFailed()) {
                return;
            }
        }

        // 转换
        EventAttrRow eventRow = eventAttrTransfer.transferFromMap(
                appId, platform, dt, pr, ip, ipResult, ua, uaResult, business, eqidKeywords);
        if (eventRow != null) {
            output.addEventAttrRow(eventRow);
            eventAttrCount.incrementAndGet();
        }
    }

    /**
     * 预加载百度关键词
     */
    private Map<String, String> preloadKeywords(Integer appId, List<?> dataList) {
        Map<String, String> result = new HashMap<>();

        if (!whiteAppIds.contains(String.valueOf(appId))) {
            return result;
        }

        Set<String> eqids = new HashSet<>();

        for (Object item : dataList) {
            if (item == null) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> dataItem = (Map<String, Object>) item;

            String dt = getStringValue(dataItem, "dt");
            if (dt == null || !keywordService.shouldExtractKeyword(dt)) {
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> pr = (Map<String, Object>) dataItem.get("pr");
            if (pr == null) {
                continue;
            }

            String utmTerm = getStringValue(pr, "$utm_term");
            if (utmTerm != null && !utmTerm.isEmpty() && !"\\N".equals(utmTerm)) {
                continue;
            }

            String ref = getStringValue(pr, "$ref");
            String eqid = keywordService.extractEqid(ref);
            if (eqid != null) {
                eqids.add(eqid);
                pr.put("$eqid", eqid);
            }
        }

        if (!eqids.isEmpty()) {
            try {
                result = keywordService.preloadKeywordsAsync(eqids).join();
            } catch (Exception e) {
                LOG.warn("预加载关键词失败", e);
            }
        }

        return result;
    }

    // ============ 工具方法 ============

    private String[] extractIpResult(Map<String, Object> msgData) {
        return new String[]{
                getStringValue(msgData, "country", "\\N"),
                getStringValue(msgData, "province", "\\N"),
                getStringValue(msgData, "city", "\\N")
        };
    }

    private Map<String, String> extractUaResult(Map<String, Object> msgData) {
        Map<String, String> result = new HashMap<>();
        result.put("os", getStringValue(msgData, "os", null));
        result.put("os_version", getStringValue(msgData, "os_version", null));
        result.put("browser", getStringValue(msgData, "browser", null));
        result.put("browser_version", getStringValue(msgData, "browser_version", null));
        return result;
    }

    private String formatTime(Long ct, String tz) {
        if (ct == null || tz == null || "\\N".equals(tz)) {
            return "\\N";
        }
        try {
            long tzOffset = Long.parseLong(tz);
            if (tzOffset > 48 * 3600 * 1000 || tzOffset < -48 * 3600 * 1000) {
                return "\\N";
            }
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ct);
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String result = sdf.format(cal.getTime());
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

        if (configCacheService != null) {
            configCacheService.close();
        }
        if (keywordService != null) {
            keywordService.close();
        }
        super.close();
    }
}
