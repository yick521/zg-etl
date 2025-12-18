package com.zhugeio.etl.common.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zhugeio.etl.common.client.kvrocks.KvrocksClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.zhugeio.etl.common.constants.CacheKeyConstants.*;

/**
 * 配置缓存服务
 * 
 * 使用 CacheKeyConstants 定义的 Key，保持与原 Scala FrontCache 完全一致
 * 
 * 功能:
 * 1. 两级缓存: L1 Caffeine + L2 KVRocks
 * 2. 版本检测: 自动感知配置变更
 * 3. 覆盖所有 ID 模块和 DW 模块的配置类缓存
 * 
 * ⚠️ 重要: 集群模式下 Key 需要带 Hash Tag {}
 * ⚠️ 不包含运行时 ID 映射 (deviceId, userId, zgid)，请使用 OneIdService
 */
public class ConfigCacheService implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCacheService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    /**
     * 表示"已查询但不存在"的特殊值，用于缓存负向结果
     */
    private static final Integer NOT_FOUND = -999999;

    private transient KvrocksClient kvrocksClient;
    
    /**
     * 是否集群模式 - 决定 Key 是否带花括号
     */
    

    // ===================== L1 本地缓存 =====================
    
    // App 相关
    private transient Cache<String, Integer> appKeyToIdCache;        // appKey → appId
    private transient Cache<Integer, Integer> appIdToCompanyIdCache; // appId → companyId
    private transient Cache<String, Integer> appSdkHasDataCache;     // {appId}_{platform} → hasData
    
    // 事件相关
    private transient Cache<String, Integer> eventIdCache;           // {appId}_{owner}_{eventName} → eventId
    private transient Cache<String, Integer> eventAttrIdCache;       // {appId}_{eventId}_{owner}_{attrName} → attrId
    private transient Cache<String, String> eventAttrColumnCache;    // {eventId}_{attrId} → columnName
    private transient Cache<String, String> eventAttrAliasCache;     // {appId}_{owner}_{eventName}_{attrName} → alias
    
    // 属性相关
    private transient Cache<String, Integer> userPropIdCache;        // {appId}_{owner}_{propName} → propId
    private transient Cache<String, String> userPropOriginalCache;   // {appId}_{owner}_{propId} → originalName
    private transient Cache<String, Integer> devicePropIdCache;      // {appId}_{owner}_{propName} → propId
    
    // 黑名单/禁用集合 (Set 模拟)
    private transient Cache<String, Boolean> blackEventIdCache;      // eventId → true
    private transient Cache<String, Boolean> blackUserPropCache;     // propId → true
    private transient Cache<String, Boolean> blackEventAttrIdCache;  // attrId → true
    private transient Cache<String, Boolean> createEventForbidCache; // appId → true
    private transient Cache<String, Boolean> autoCreateDisabledCache;// appId → true
    private transient Cache<String, Boolean> createAttrForbidCache;  // eventId → true
    private transient Cache<String, Boolean> uploadDataCache;        // appId → true
    
    // 平台关系
    private transient Cache<String, Boolean> eventPlatformCache;     // {eventId}_{platform} → true
    private transient Cache<String, Boolean> eventAttrPlatformCache; // {attrId}_{platform} → true
    private transient Cache<String, Boolean> devicePropPlatformCache;// {propId}_{platform} → true
    
    // 虚拟事件/属性
    private transient Cache<String, Boolean> virtualEventAppCache;       // appId → true
    private transient Cache<String, Boolean> virtualPropAppCache;        // appId → true
    private transient Cache<String, List<String>> virtualEventCache;     // {appId}_{owner}_{eventName} → configs
    private transient Cache<String, Set<String>> virtualEventAttrCache;  // {appId}_{virtualEventName}_{owner}_{eventName} → attrs
    private transient Cache<String, List<String>> virtualEventPropCache; // {appId}_eP_{eventName} → props
    private transient Cache<String, List<String>> virtualUserPropCache;  // appId → props
    private transient Cache<String, Boolean> eventVirtualAttrCache;      // attrId → true
    
    // 广告相关
    private transient Cache<String, Integer> advertisingAppCache;    // appKey → appId
    private transient Cache<String, String> adsLinkEventCache;       // {eventId}_{lid} → JSON
    private transient Cache<String, String> lidChannelEventCache;    // {lid}_{eventId} → channelEvent
    private transient Cache<String, Boolean> adFrequencyCache;       // {eventId}_{lid}_{zgId} → true
    
    // DW模块
    private transient Cache<String, String> baseCurrentCache;        // baseName → currentName
    private transient Cache<String, String> cdpConfigCache;          // appId → config
    private transient Cache<String, Boolean> businessCache;          // {companyId}_{identifier} → true
    private transient Cache<String, String> yearweekCache;           // day → yearWeek

    // 全量数据缓存
    private transient Cache<String, Map<String, String>> hashDataCache;

    // ===================== 版本控制 =====================
    private transient volatile long currentVersion = 0;
    private transient volatile long lastVersionCheckTime = 0;
    private static final long VERSION_CHECK_INTERVAL_MS = 5000;

    private final CacheConfig config;

    // ===================== 统计 =====================
    private transient final Map<String, AtomicLong> l1Hits = new ConcurrentHashMap<>();
    private transient final Map<String, AtomicLong> l2Hits = new ConcurrentHashMap<>();
    private transient final Map<String, AtomicLong> misses = new ConcurrentHashMap<>();

    public ConfigCacheService(CacheConfig config) {
        this.config = config;
    }

    // ===================== 初始化 =====================

    public void init() {
        kvrocksClient = new KvrocksClient(
                config.getKvrocksHost(),
                config.getKvrocksPort(),
                config.isKvrocksCluster()
        );
        kvrocksClient.init();

        if (!kvrocksClient.testConnection()) {
            throw new RuntimeException("KVRocks连接失败: " + config.getKvrocksHost());
        }

        initL1Caches();
        loadVersion();

        LOG.info("ConfigCacheService initialized, version={}", currentVersion);
    }

    private void initL1Caches() {
        // App (相对稳定，容量小)
        appKeyToIdCache = buildCache(10000, 10);
        appIdToCompanyIdCache = buildCache(10000, 10);
        appSdkHasDataCache = buildCache(50000, 10);
        
        // 事件 (量大，按热度淘汰)
        // 注意: 40万事件属性无法全部缓存，依赖LRU淘汰冷数据
        // 如果命中率低，考虑增大容量或缩短过期时间让热数据常驻
        eventIdCache = buildCache(200000, 30);           // 事件数一般几万
        eventAttrIdCache = buildCache(500000, 30);       // 事件属性可达40万+
        eventAttrColumnCache = buildCache(500000, 60);   // 同上
        eventAttrAliasCache = buildCache(100000, 30);
        
        // 属性
        userPropIdCache = buildCache(100000, 30);
        userPropOriginalCache = buildCache(100000, 30);
        devicePropIdCache = buildCache(50000, 30);
        
        // 黑名单
        blackEventIdCache = buildCache(10000, 30);
        blackUserPropCache = buildCache(10000, 30);
        blackEventAttrIdCache = buildCache(10000, 30);
        createEventForbidCache = buildCache(5000, 10);
        autoCreateDisabledCache = buildCache(5000, 10);
        createAttrForbidCache = buildCache(10000, 10);
        uploadDataCache = buildCache(10000, 10);
        
        // 平台
        eventPlatformCache = buildCache(100000, 30);
        eventAttrPlatformCache = buildCache(200000, 30);
        devicePropPlatformCache = buildCache(50000, 30);
        
        // 虚拟
        virtualEventAppCache = buildCache(5000, 5);
        virtualPropAppCache = buildCache(5000, 5);
        virtualEventCache = buildCache(10000, 5);
        virtualEventAttrCache = buildCache(10000, 5);
        virtualEventPropCache = buildCache(10000, 5);
        virtualUserPropCache = buildCache(5000, 5);
        eventVirtualAttrCache = buildCache(50000, 30);
        
        // 广告
        advertisingAppCache = buildCache(5000, 10);
        adsLinkEventCache = buildCache(10000, 10);
        lidChannelEventCache = buildCache(10000, 10);
        adFrequencyCache = buildCache(100000, 60);
        
        // DW
        baseCurrentCache = buildCache(1000, 60);
        cdpConfigCache = buildCache(5000, 60);
        businessCache = buildCache(10000, 30);
        yearweekCache = buildCache(1000, 1440);
        
        // 全量
        hashDataCache = buildCache(50, 5);
    }

    private <K, V> Cache<K, V> buildCache(int maxSize, int expireMinutes) {
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();
    }

    // ===================== 版本控制 =====================

    public void checkAndRefreshVersion() {
        long now = System.currentTimeMillis();
        if (now - lastVersionCheckTime < VERSION_CHECK_INTERVAL_MS) { return; }
        lastVersionCheckTime = now;

        try {
            String versionStr = kvrocksClient.asyncGet(SYNC_VERSION).get(1, TimeUnit.SECONDS);
            if (versionStr != null) {
                long newVersion = Long.parseLong(versionStr);
                if (newVersion > currentVersion) {
                    LOG.info("Cache version changed: {} -> {}", currentVersion, newVersion);
                    invalidateAllL1();
                    currentVersion = newVersion;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to check cache version", e);
        }
    }

    private void loadVersion() {
        try {
            String versionStr = kvrocksClient.asyncGet(SYNC_VERSION).get(5, TimeUnit.SECONDS);
            if (versionStr != null) { currentVersion = Long.parseLong(versionStr); }
        } catch (Exception e) {
            LOG.warn("Failed to load initial version", e);
        }
    }

    private void invalidateAllL1() {
        appKeyToIdCache.invalidateAll();
        appIdToCompanyIdCache.invalidateAll();
        appSdkHasDataCache.invalidateAll();
        eventIdCache.invalidateAll();
        eventAttrIdCache.invalidateAll();
        eventAttrColumnCache.invalidateAll();
        eventAttrAliasCache.invalidateAll();
        userPropIdCache.invalidateAll();
        userPropOriginalCache.invalidateAll();
        devicePropIdCache.invalidateAll();
        blackEventIdCache.invalidateAll();
        blackUserPropCache.invalidateAll();
        blackEventAttrIdCache.invalidateAll();
        createEventForbidCache.invalidateAll();
        autoCreateDisabledCache.invalidateAll();
        createAttrForbidCache.invalidateAll();
        uploadDataCache.invalidateAll();
        eventPlatformCache.invalidateAll();
        eventAttrPlatformCache.invalidateAll();
        devicePropPlatformCache.invalidateAll();
        virtualEventAppCache.invalidateAll();
        virtualPropAppCache.invalidateAll();
        virtualEventCache.invalidateAll();
        virtualEventAttrCache.invalidateAll();
        virtualEventPropCache.invalidateAll();
        virtualUserPropCache.invalidateAll();
        eventVirtualAttrCache.invalidateAll();
        advertisingAppCache.invalidateAll();
        adsLinkEventCache.invalidateAll();
        lidChannelEventCache.invalidateAll();
        adFrequencyCache.invalidateAll();
        baseCurrentCache.invalidateAll();
        cdpConfigCache.invalidateAll();
        businessCache.invalidateAll();
        yearweekCache.invalidateAll();
        hashDataCache.invalidateAll();
    }

    // ===================== App 相关 =====================

    /**
     * appKey → appId
     * Key: appKeyAppIdMap (Hash)
     */
    public CompletableFuture<Integer> getAppIdByAppKey(String appKey) {
        if (appKey == null) { return CompletableFuture.completedFuture(null); }
        
        Integer cached = appKeyToIdCache.getIfPresent(appKey);
        if (cached != null) { hit("appKey"); return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(APP_KEY_APP_ID_MAP, appKey)
                .thenApply(value -> {
                    if (value != null) {
                        Integer appId = Integer.parseInt(value);
                        appKeyToIdCache.put(appKey, appId);
                        l2Hit("appKey");
                        return appId;
                    }
                    miss("appKey");
                    return null;
                });
    }

    /**
     * appId → companyId
     * Key: cidByAidMap (Hash)
     */
    public CompletableFuture<Integer> getCompanyIdByAppId(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(null); }
        
        Integer cached = appIdToCompanyIdCache.getIfPresent(appId);
        if (cached != null) { hit("companyId"); return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(CID_BY_AID_MAP, String.valueOf(appId))
                .thenApply(value -> {
                    if (value != null) {
                        Integer cid = Integer.parseInt(value);
                        appIdToCompanyIdCache.put(appId, cid);
                        l2Hit("companyId");
                        return cid;
                    }
                    miss("companyId");
                    return null;
                });
    }

    /**
     * 检查应用平台是否有数据
     * Key: appIdSdkHasDataMap (Hash)
     */
    public CompletableFuture<Boolean> hasAppSdkData(Integer appId, Integer platform) {
        if (appId == null || platform == null) { return CompletableFuture.completedFuture(false); }
        
        String field = appId + "_" + platform;
        Integer cached = appSdkHasDataCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached == 1); }

        return kvrocksClient.asyncHGet(APP_ID_SDK_HAS_DATA_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        int hasData = Integer.parseInt(value);
                        appSdkHasDataCache.put(field, hasData);
                        return hasData == 1;
                    }
                    return false;
                });
    }

    /**
     * 检查应用是否已上传数据
     * Key: appIdUploadDataSet (Set)
     */
    public CompletableFuture<Boolean> hasUploadData(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(false); }
        
        String key = String.valueOf(appId);
        Boolean cached = uploadDataCache.getIfPresent(key);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(APP_ID_UPLOAD_DATA_SET, key)
                .thenApply(isMember -> {
                    uploadDataCache.put(key, isMember);
                    return isMember;
                });
    }

    // ===================== 事件相关 =====================

    /**
     * 获取事件ID
     * Key: appIdEventIdMap (Hash)
     * Field: ${appId}_${owner}_${eventName}
     * 
     * 返回 null 表示不存在
     */
    public CompletableFuture<Integer> getEventId(Integer appId, String owner, String eventName) {
        if (appId == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + owner + "_" + eventName;
        Integer cached = eventIdCache.getIfPresent(field);
        if (cached != null) { 
            hit("eventId"); 
            return CompletableFuture.completedFuture(cached == NOT_FOUND ? null : cached); 
        }

        return kvrocksClient.asyncHGet(APP_ID_EVENT_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer eventId = Integer.parseInt(value);
                        eventIdCache.put(field, eventId);
                        l2Hit("eventId");
                        return eventId;
                    }
                    eventIdCache.put(field, NOT_FOUND);
                    miss("eventId");
                    return null;
                });
    }

    /**
     * 获取事件属性ID
     * Key: appIdEventAttrIdMap (Hash)
     * Field: ${appId}_${eventId}_${owner}_${attrName(大写)}
     * 
     * 返回值:
     * - 正数: 属性ID
     * - NOT_FOUND (-1): 确认不存在 (会缓存这个结果)
     * - null: 查询异常
     */
    public CompletableFuture<Integer> getEventAttrId(Integer appId, Integer eventId, 
                                                      String owner, String attrName) {
        if (appId == null || eventId == null || owner == null || attrName == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + eventId + "_" + owner + "_" + attrName.toUpperCase();
        Integer cached = eventAttrIdCache.getIfPresent(field);
        if (cached != null) { 
            hit("eventAttrId"); 
            return CompletableFuture.completedFuture(cached == NOT_FOUND ? null : cached); 
        }

        return kvrocksClient.asyncHGet(APP_ID_EVENT_ATTR_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer attrId = Integer.parseInt(value);
                        eventAttrIdCache.put(field, attrId);
                        l2Hit("eventAttrId");
                        return attrId;
                    }
                    // 缓存"不存在"，避免重复查询
                    eventAttrIdCache.put(field, NOT_FOUND);
                    miss("eventAttrId");
                    return null;
                });
    }

    /**
     * 获取事件属性列名
     * Key: eventAttrColumnMap (Hash)
     * Field: ${eventId}_${attrId}
     */
    public CompletableFuture<String> getEventAttrColumn(String eventId, String attrId) {
        if (eventId == null || attrId == null) { return CompletableFuture.completedFuture(null); }
        
        String field = eventId + "_" + attrId;
        String cached = eventAttrColumnCache.getIfPresent(field);
        if (cached != null) { hit("eventAttrColumn"); return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(EVENT_ATTR_COLUMN_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        eventAttrColumnCache.put(field, value);
                        l2Hit("eventAttrColumn");
                    } else {
                        miss("eventAttrColumn");
                    }
                    return value;
                });
    }

    /**
     * 获取事件属性列索引 (从 cus1 解析为 1)
     */
    public CompletableFuture<Integer> getEventAttrColumnIndex(String eventId, String attrId) {
        return getEventAttrColumn(eventId, attrId)
                .thenApply(column -> {
                    if (column != null && column.startsWith("cus")) {
                        try {
                            return Integer.parseInt(column.substring(3));
                        } catch (NumberFormatException ignored) {}
                    }
                    return -1;
                });
    }

    /**
     * 获取事件属性别名
     * Key: eventAttrAliasMap (Hash)
     * Field: ${appId}_${owner}_${eventName}_${attrName}
     */
    public CompletableFuture<String> getEventAttrAlias(Integer appId, String owner, 
                                                        String eventName, String attrName) {
        if (appId == null || owner == null || eventName == null || attrName == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + owner + "_" + eventName + "_" + attrName;
        String cached = eventAttrAliasCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(EVENT_ATTR_ALIAS_MAP, field)
                .thenApply(value -> {
                    if (value != null) { eventAttrAliasCache.put(field, value); }
                    return value;
                });
    }

    // ===================== 属性相关 =====================

    /**
     * 获取用户属性ID
     * Key: appIdPropIdMap (Hash)
     * Field: ${appId}_${owner}_${name(大写)}
     */
    public CompletableFuture<Integer> getUserPropId(Integer appId, String owner, String propName) {
        if (appId == null || owner == null || propName == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + owner + "_" + propName.toUpperCase();
        Integer cached = userPropIdCache.getIfPresent(field);
        if (cached != null) { 
            hit("userPropId"); 
            return CompletableFuture.completedFuture(cached == NOT_FOUND ? null : cached); 
        }

        return kvrocksClient.asyncHGet(APP_ID_PROP_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer propId = Integer.parseInt(value);
                        userPropIdCache.put(field, propId);
                        l2Hit("userPropId");
                        return propId;
                    }
                    userPropIdCache.put(field, NOT_FOUND);
                    miss("userPropId");
                    return null;
                });
    }

    /**
     * 获取用户属性原始名称
     * Key: appIdPropIdOriginalMap (Hash)
     * Field: ${appId}_${owner}_${propId}
     */
    public CompletableFuture<String> getUserPropOriginalName(Integer appId, String owner, Integer propId) {
        if (appId == null || owner == null || propId == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + owner + "_" + propId;
        String cached = userPropOriginalCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(APP_ID_PROP_ID_ORIGINAL_MAP, field)
                .thenApply(value -> {
                    if (value != null) { userPropOriginalCache.put(field, value); }
                    return value;
                });
    }

    /**
     * 获取设备属性ID
     * Key: appIdDevicePropIdMap (Hash)
     * Field: ${appId}_${owner}_${propName}
     */
    public CompletableFuture<Integer> getDevicePropId(Integer appId, String owner, String propName) {
        if (appId == null || owner == null || propName == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        String field = appId + "_" + owner + "_" + propName;
        Integer cached = devicePropIdCache.getIfPresent(field);
        if (cached != null) { 
            return CompletableFuture.completedFuture(cached == NOT_FOUND ? null : cached); 
        }

        return kvrocksClient.asyncHGet(APP_ID_DEVICE_PROP_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer propId = Integer.parseInt(value);
                        devicePropIdCache.put(field, propId);
                        return propId;
                    }
                    devicePropIdCache.put(field, NOT_FOUND);
                    return null;
                });
    }

    // ===================== 黑名单相关 =====================

    /**
     * 检查事件是否在黑名单
     * Key: blackEventIdSet (Set)
     */
    public CompletableFuture<Boolean> isEventBlacklisted(Integer eventId) {
        if (eventId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(eventId);
        Boolean cached = blackEventIdCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(BLACK_EVENT_ID_SET, member)
                .thenApply(isMember -> {
                    blackEventIdCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查用户属性是否在黑名单
     * Key: blackUserPropSet (Set)
     */
    public CompletableFuture<Boolean> isUserPropBlacklisted(Integer propId) {
        if (propId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(propId);
        Boolean cached = blackUserPropCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(BLACK_USER_PROP_SET, member)
                .thenApply(isMember -> {
                    blackUserPropCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查事件属性是否在黑名单
     * Key: blackEventAttrIdSet (Set)
     */
    public CompletableFuture<Boolean> isEventAttrBlacklisted(Integer attrId) {
        if (attrId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(attrId);
        Boolean cached = blackEventAttrIdCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(BLACK_EVENT_ATTR_ID_SET, member)
                .thenApply(isMember -> {
                    blackEventAttrIdCache.put(member, isMember);
                    return isMember;
                });
    }

    // ===================== 配置开关 =====================

    /**
     * 检查应用是否禁止创建事件
     * Key: appIdCreateEventForbidSet (Set)
     */
    public CompletableFuture<Boolean> isCreateEventForbidden(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(appId);
        Boolean cached = createEventForbidCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(APP_ID_CREATE_EVENT_FORBID_SET, member)
                .thenApply(isMember -> {
                    createEventForbidCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查应用是否禁用自动创建
     * Key: appIdNoneAutoCreateSet (Set)
     */
    public CompletableFuture<Boolean> isAutoCreateDisabled(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(appId);
        Boolean cached = autoCreateDisabledCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(APP_ID_NONE_AUTO_CREATE_SET, member)
                .thenApply(isMember -> {
                    autoCreateDisabledCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查事件是否禁止创建属性
     * Key: eventIdCreateAttrForbiddenSet (Set)
     */
    public CompletableFuture<Boolean> isCreateAttrForbidden(Integer eventId) {
        if (eventId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(eventId);
        Boolean cached = createAttrForbidCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(EVENT_ID_CREATE_ATTR_FORBIDDEN_SET, member)
                .thenApply(isMember -> {
                    createAttrForbidCache.put(member, isMember);
                    return isMember;
                });
    }

    // ===================== 平台关系 =====================

    /**
     * 检查事件平台关系
     * Key: eventIdPlatform (Set)
     * Member: ${eventId}_${platform}
     */
    public CompletableFuture<Boolean> isEventPlatformValid(Integer eventId, Integer platform) {
        if (eventId == null || platform == null) { return CompletableFuture.completedFuture(true); }
        
        String member = eventId + "_" + platform;
        Boolean cached = eventPlatformCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(EVENT_ID_PLATFORM, member)
                .thenApply(isMember -> {
                    eventPlatformCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查事件属性平台关系
     * Key: eventAttrdPlatform (Set)
     * Member: ${attrId}_${platform}
     */
    public CompletableFuture<Boolean> isEventAttrPlatformValid(Integer attrId, Integer platform) {
        if (attrId == null || platform == null) { return CompletableFuture.completedFuture(true); }
        
        String member = attrId + "_" + platform;
        Boolean cached = eventAttrPlatformCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(EVENT_ATTR_PLATFORM, member)
                .thenApply(isMember -> {
                    eventAttrPlatformCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查设备属性平台关系
     * Key: devicePropPlatform (Set)
     * Member: ${propId}_${platform}
     */
    public CompletableFuture<Boolean> isDevicePropPlatformValid(Integer propId, Integer platform) {
        if (propId == null || platform == null) { return CompletableFuture.completedFuture(true); }
        
        String member = propId + "_" + platform;
        Boolean cached = devicePropPlatformCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(DEVICE_PROP_PLATFORM, member)
                .thenApply(isMember -> {
                    devicePropPlatformCache.put(member, isMember);
                    return isMember;
                });
    }

    // ===================== 虚拟事件/属性 =====================

    /**
     * 检查应用是否启用虚拟事件
     * Key: virtualEventAppidsSet (Set)
     */
    public CompletableFuture<Boolean> isVirtualEventEnabled(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(appId);
        Boolean cached = virtualEventAppCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(VIRTUAL_EVENT_APPIDS_SET, member)
                .thenApply(isMember -> {
                    virtualEventAppCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查应用是否启用虚拟属性
     * Key: virtualPropAppIdsSet (Set)
     */
    public CompletableFuture<Boolean> isVirtualPropEnabled(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(appId);
        Boolean cached = virtualPropAppCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(VIRTUAL_PROP_APP_IDS_SET, member)
                .thenApply(isMember -> {
                    virtualPropAppCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 检查属性是否为虚拟属性
     * Key: eventVirtualAttrIdsSet (Set)
     */
    public CompletableFuture<Boolean> isVirtualAttr(Integer attrId) {
        if (attrId == null) { return CompletableFuture.completedFuture(false); }
        
        String member = String.valueOf(attrId);
        Boolean cached = eventVirtualAttrCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(EVENT_VIRTUAL_ATTR_IDS_SET, member)
                .thenApply(isMember -> {
                    eventVirtualAttrCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 获取虚拟事件配置
     * Key: virtualEventMap (Hash)
     * Field: ${appId}_${owner}_${eventName}
     */
    public CompletableFuture<List<String>> getVirtualEventConfig(Integer appId, String owner, String eventName) {
        if (appId == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        
        String field = appId + "_" + owner + "_" + eventName;
        List<String> cached = virtualEventCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(VIRTUAL_EVENT_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            List<String> configs = OBJECT_MAPPER.readValue(value, 
                                    new TypeReference<List<String>>() {});
                            virtualEventCache.put(field, configs);
                            return configs;
                        } catch (Exception ignored) {}
                    }
                    return Collections.emptyList();
                });
    }

    /**
     * 获取虚拟事件属性
     * Key: virtualEventAttrMap (Hash)
     * Field: ${appId}_${virtualEventName}_${owner}_${eventName}
     */
    public CompletableFuture<Set<String>> getVirtualEventAttr(Integer appId, String virtualEventName,
                                                               String owner, String eventName) {
        if (appId == null || virtualEventName == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
        
        String field = appId + "_" + virtualEventName + "_" + owner + "_" + eventName;
        Set<String> cached = virtualEventAttrCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(VIRTUAL_EVENT_ATTR_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            Set<String> attrs = OBJECT_MAPPER.readValue(value, 
                                    new TypeReference<Set<String>>() {});
                            virtualEventAttrCache.put(field, attrs);
                            return attrs;
                        } catch (Exception ignored) {}
                    }
                    return Collections.emptySet();
                });
    }

    /**
     * 获取虚拟事件属性(按事件名)
     * Key: virtualEventPropMap (Hash)
     * Field: ${appId}_eP_${eventName}
     */
    public CompletableFuture<List<String>> getVirtualEventProp(Integer appId, String eventName) {
        if (appId == null || eventName == null) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        
        String field = appId + "_eP_" + eventName;
        List<String> cached = virtualEventPropCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(VIRTUAL_EVENT_PROP_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            List<String> props = OBJECT_MAPPER.readValue(value, 
                                    new TypeReference<List<String>>() {});
                            virtualEventPropCache.put(field, props);
                            return props;
                        } catch (Exception ignored) {}
                    }
                    return Collections.emptyList();
                });
    }

    /**
     * 获取虚拟用户属性
     * Key: virtualUserPropMap (Hash)
     * Field: ${appId}
     */
    public CompletableFuture<List<String>> getVirtualUserProp(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(Collections.emptyList()); }
        
        String field = String.valueOf(appId);
        List<String> cached = virtualUserPropCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(VIRTUAL_USER_PROP_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        try {
                            List<String> props = OBJECT_MAPPER.readValue(value, 
                                    new TypeReference<List<String>>() {});
                            virtualUserPropCache.put(field, props);
                            return props;
                        } catch (Exception ignored) {}
                    }
                    return Collections.emptyList();
                });
    }

    // ===================== 广告相关 =====================

    /**
     * 检查应用是否开启广告投放
     * Key: openAdvertisingFunctionAppMap (Hash)
     */
    public CompletableFuture<Integer> getAdvertisingAppId(String appKey) {
        if (appKey == null) { return CompletableFuture.completedFuture(null); }
        
        Integer cached = advertisingAppCache.getIfPresent(appKey);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(OPEN_ADVERTISING_FUNCTION_APP_MAP, appKey)
                .thenApply(value -> {
                    if (value != null) {
                        Integer appId = Integer.parseInt(value);
                        advertisingAppCache.put(appKey, appId);
                        return appId;
                    }
                    return null;
                });
    }

    /**
     * 获取广告链接事件配置
     * Key: adsLinkEventMap (Hash)
     * Field: ${eventId}_${lid}
     */
    public CompletableFuture<String> getAdsLinkEvent(Integer eventId, String lid) {
        if (eventId == null || lid == null) { return CompletableFuture.completedFuture(null); }
        
        String field = eventId + "_" + lid;
        String cached = adsLinkEventCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(ADS_LINK_EVENT_MAP, field)
                .thenApply(value -> {
                    if (value != null) { adsLinkEventCache.put(field, value); }
                    return value;
                });
    }

    /**
     * 获取链接渠道事件
     * Key: lidAndChannelEventMap (Hash)
     * Field: ${lid}_${eventId}
     */
    public CompletableFuture<String> getLidChannelEvent(String lid, Integer eventId) {
        if (lid == null || eventId == null) { return CompletableFuture.completedFuture(null); }
        
        String field = lid + "_" + eventId;
        String cached = lidChannelEventCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(LID_AND_CHANNEL_EVENT_MAP, field)
                .thenApply(value -> {
                    if (value != null) { lidChannelEventCache.put(field, value); }
                    return value;
                });
    }

    /**
     * 检查广告回传频次
     * Key: adFrequencySet (Set)
     * Member: ${eventId}_${lid}_${zgId}
     */
    public CompletableFuture<Boolean> hasAdFrequency(Integer eventId, String lid, Long zgId) {
        if (eventId == null || lid == null || zgId == null) {
            return CompletableFuture.completedFuture(false);
        }
        
        String member = eventId + "_" + lid + "_" + zgId;
        Boolean cached = adFrequencyCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(AD_FREQUENCY_SET, member)
                .thenApply(isMember -> {
                    adFrequencyCache.put(member, isMember);
                    return isMember;
                });
    }

    // ===================== DW模块 =====================

    /**
     * 获取 Kudu 表当前名称
     * Key: baseCurrentMap (Hash)
     */
    public CompletableFuture<String> getBaseCurrent(String baseName) {
        if (baseName == null) { return CompletableFuture.completedFuture(null); }
        
        String cached = baseCurrentCache.getIfPresent(baseName);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(BASE_CURRENT_MAP, baseName)
                .thenApply(value -> {
                    if (value != null) { baseCurrentCache.put(baseName, value); }
                    return value;
                });
    }

    /**
     * 获取 CDP 配置
     * Key: openCdpAppidMap (Hash)
     */
    public CompletableFuture<String> getCdpConfig(Integer appId) {
        if (appId == null) { return CompletableFuture.completedFuture(null); }
        
        String field = String.valueOf(appId);
        String cached = cdpConfigCache.getIfPresent(field);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(OPEN_CDP_APPID_MAP, field)
                .thenApply(value -> {
                    if (value != null) { cdpConfigCache.put(field, value); }
                    return value;
                });
    }

    /**
     * 检查 CDP 是否启用
     * Key: openCdpAppidMap (Hash)
     */
    public CompletableFuture<Boolean> isCdpEnabled(Integer appId) {
        return getCdpConfig(appId)
                .thenApply(config -> config != null && !config.isEmpty());
    }

    /**
     * 检查业务标识是否有效
     * Key: businessMap (Set)
     * Member: ${companyId}_${identifier}
     */
    public CompletableFuture<Boolean> isValidBusiness(Integer companyId, String identifier) {
        if (companyId == null || identifier == null || identifier.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        
        String member = companyId + "_" + identifier;
        Boolean cached = businessCache.getIfPresent(member);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncSIsMember(BUSINESS_MAP, member)
                .thenApply(isMember -> {
                    businessCache.put(member, isMember);
                    return isMember;
                });
    }

    /**
     * 获取年周
     * Key: yearweek (Hash)
     * Field: ${day}
     */
    public CompletableFuture<String> getYearWeek(String day) {
        if (day == null) { return CompletableFuture.completedFuture(null); }
        
        String cached = yearweekCache.getIfPresent(day);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGet(YEAR_WEEK, day)
                .thenApply(value -> {
                    if (value != null) { yearweekCache.put(day, value); }
                    return value;
                });
    }

    // ===================== 批量查询 =====================

    /**
     * 获取 Hash 全量数据
     * @param hashKey key，如 {appIdEventIdMap}
     */
    public CompletableFuture<Map<String, String>> getHashData(String hashKey) {
        Map<String, String> cached = hashDataCache.getIfPresent(hashKey);
        if (cached != null) { return CompletableFuture.completedFuture(cached); }

        return kvrocksClient.asyncHGetAll(hashKey)
                .thenApply(map -> {
                    if (map != null && !map.isEmpty()) {
                        hashDataCache.put(hashKey, map);
                        return map;
                    }
                    return Collections.emptyMap();
                });
    }

    // ===================== 统计 =====================

    private void hit(String name) { l1Hits.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet(); }
    private void l2Hit(String name) { l2Hits.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet(); }
    private void miss(String name) { misses.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet(); }

    // ===================== 存在性检查辅助类 =====================

    /**
     * 查询结果封装，明确区分"存在"、"不存在"、"查询失败"
     */
    public static class CacheResult<T> {
        private final T value;
        private final boolean exists;
        private final boolean success;

        private CacheResult(T value, boolean exists, boolean success) {
            this.value = value;
            this.exists = exists;
            this.success = success;
        }

        public static <T> CacheResult<T> found(T value) {
            return new CacheResult<>(value, true, true);
        }

        public static <T> CacheResult<T> notFound() {
            return new CacheResult<>(null, false, true);
        }

        public static <T> CacheResult<T> error() {
            return new CacheResult<>(null, false, false);
        }

        public T getValue() { return value; }
        public boolean exists() { return exists; }
        public boolean isSuccess() { return success; }
        public boolean notExists() { return success && !exists; }
    }

    /**
     * 获取事件ID (带存在性检查)
     * 
     * 使用示例:
     * ```java
     * CacheResult<Integer> result = configCache.getEventIdWithCheck(appId, owner, eventName).get();
     * if (result.exists()) {
     *     Integer eventId = result.getValue();
     * } else if (result.notExists()) {
     *     // 确认不存在，可以创建新事件
     * } else {
     *     // 查询失败
     * }
     * ```
     */
    public CompletableFuture<CacheResult<Integer>> getEventIdWithCheck(Integer appId, String owner, String eventName) {
        if (appId == null || owner == null || eventName == null) {
            return CompletableFuture.completedFuture(CacheResult.<Integer>error());
        }
        
        String field = appId + "_" + owner + "_" + eventName;
        Integer cached = eventIdCache.getIfPresent(field);
        if (cached != null) { 
            hit("eventId"); 
            if (cached.equals(NOT_FOUND)) {
                return CompletableFuture.completedFuture(CacheResult.<Integer>notFound());
            }
            return CompletableFuture.completedFuture(CacheResult.found(cached));
        }

        return kvrocksClient.asyncHGet(APP_ID_EVENT_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer eventId = Integer.parseInt(value);
                        eventIdCache.put(field, eventId);
                        l2Hit("eventId");
                        return CacheResult.found(eventId);
                    }
                    eventIdCache.put(field, NOT_FOUND);
                    miss("eventId");
                    return CacheResult.<Integer>notFound();
                })
                .exceptionally(ex -> {
                    LOG.error("Failed to get eventId: {}", field, ex);
                    return CacheResult.<Integer>error();
                });
    }

    /**
     * 获取事件属性ID (带存在性检查)
     */
    public CompletableFuture<CacheResult<Integer>> getEventAttrIdWithCheck(Integer appId, Integer eventId, 
                                                                            String owner, String attrName) {
        if (appId == null || eventId == null || owner == null || attrName == null) {
            return CompletableFuture.completedFuture(CacheResult.<Integer>error());
        }
        
        String field = appId + "_" + eventId + "_" + owner + "_" + attrName.toUpperCase();
        Integer cached = eventAttrIdCache.getIfPresent(field);
        if (cached != null) { 
            hit("eventAttrId"); 
            if (cached.equals(NOT_FOUND)) {
                return CompletableFuture.completedFuture(CacheResult.<Integer>notFound());
            }
            return CompletableFuture.completedFuture(CacheResult.found(cached));
        }

        return kvrocksClient.asyncHGet(APP_ID_EVENT_ATTR_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer attrId = Integer.parseInt(value);
                        eventAttrIdCache.put(field, attrId);
                        l2Hit("eventAttrId");
                        return CacheResult.found(attrId);
                    }
                    eventAttrIdCache.put(field, NOT_FOUND);
                    miss("eventAttrId");
                    return CacheResult.<Integer>notFound();
                })
                .exceptionally(ex -> {
                    LOG.error("Failed to get eventAttrId: {}", field, ex);
                    return CacheResult.<Integer>error();
                });
    }

    /**
     * 获取用户属性ID (带存在性检查)
     */
    public CompletableFuture<CacheResult<Integer>> getUserPropIdWithCheck(Integer appId, String owner, String propName) {
        if (appId == null || owner == null || propName == null) {
            return CompletableFuture.completedFuture(CacheResult.<Integer>error());
        }
        
        String field = appId + "_" + owner + "_" + propName.toUpperCase();
        Integer cached = userPropIdCache.getIfPresent(field);
        if (cached != null) { 
            hit("userPropId"); 
            if (cached.equals(NOT_FOUND)) {
                return CompletableFuture.completedFuture(CacheResult.<Integer>notFound());
            }
            return CompletableFuture.completedFuture(CacheResult.found(cached));
        }

        return kvrocksClient.asyncHGet(APP_ID_PROP_ID_MAP, field)
                .thenApply(value -> {
                    if (value != null) {
                        Integer propId = Integer.parseInt(value);
                        userPropIdCache.put(field, propId);
                        l2Hit("userPropId");
                        return CacheResult.found(propId);
                    }
                    userPropIdCache.put(field, NOT_FOUND);
                    miss("userPropId");
                    return CacheResult.<Integer>notFound();
                })
                .exceptionally(ex -> {
                    LOG.error("Failed to get userPropId: {}", field, ex);
                    return CacheResult.<Integer>error();
                });
    }

    public KvrocksClient getKvrocksClient() { return kvrocksClient; }
    public long getCurrentVersion() { return currentVersion; }

    public String getStats() {
        return String.format("ConfigCacheService: version=%d, L1=%s, L2=%s, Miss=%s",
                currentVersion, l1Hits, l2Hits, misses);
    }

    public void close() {
        LOG.info("Closing ConfigCacheService - {}", getStats());
        if (kvrocksClient != null) { kvrocksClient.shutdown(); }
    }
}
