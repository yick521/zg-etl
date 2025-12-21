package com.zhugeio.etl.common.constants;

/**
 * 缓存 Key 常量定义 (集群模式)
 * 
 * 所有 Key 都带 Hash Tag {}，保证：
 * 1. 同一个 Hash 的所有操作在同一 slot
 * 2. RENAME 原子替换时 {key} 和 {key}:temp 在同一 slot
 */
public final class CacheKeyConstants {

    private CacheKeyConstants() {}

    // ==========================================================
    // ID模块 - Hash类型
    // ==========================================================
    
    /** appKey -> appId */
    public static final String APP_KEY_APP_ID_MAP = "{appKeyAppIdMap}";
    
    /** appId_platform -> hasData */
    public static final String APP_ID_SDK_HAS_DATA_MAP = "{appIdSdkHasDataMap}";
    
    /** appId_owner_propName -> propId */
    public static final String APP_ID_PROP_ID_MAP = "{appIdPropIdMap}";
    
    /** appId_owner_propId -> originalName */
    public static final String APP_ID_PROP_ID_ORIGINAL_MAP = "{appIdPropIdOriginalMap}";
    
    /** appId_owner_eventName -> eventId */
    public static final String APP_ID_EVENT_ID_MAP = "{appIdEventIdMap}";
    
    /** appId_eventId_owner_attrName -> attrId */
    public static final String APP_ID_EVENT_ATTR_ID_MAP = "{appIdEventAttrIdMap}";
    
    /** appId_owner_propName -> propId */
    public static final String APP_ID_DEVICE_PROP_ID_MAP = "{appIdDevicePropIdMap}";

    // ==========================================================
    // ID模块 - Set类型
    // ==========================================================
    
    /** 禁止创建事件的 appId */
    public static final String APP_ID_CREATE_EVENT_FORBID_SET = "{appIdCreateEventForbidSet}";
    
    /** 已上传数据的 appId */
    public static final String APP_ID_UPLOAD_DATA_SET = "{appIdUploadDataSet}";
    
    /** 黑名单用户属性 propId */
    public static final String BLACK_USER_PROP_SET = "{blackUserPropSet}";
    
    /** 黑名单事件 eventId */
    public static final String BLACK_EVENT_ID_SET = "{blackEventIdSet}";
    
    /** 禁止自动创建的 appId */
    public static final String APP_ID_NONE_AUTO_CREATE_SET = "{appIdNoneAutoCreateSet}";
    
    /** 黑名单事件属性 attrId */
    public static final String BLACK_EVENT_ATTR_ID_SET = "{blackEventAttrIdSet}";
    
    /** 禁止创建属性的 eventId */
    public static final String EVENT_ID_CREATE_ATTR_FORBIDDEN_SET = "{eventIdCreateAttrForbiddenSet}";
    
    /** 事件-平台关系 eventId_platform */
    public static final String EVENT_ID_PLATFORM = "{eventIdPlatform}";
    
    /** 事件属性-平台关系 attrId_platform */
    public static final String EVENT_ATTR_PLATFORM = "{eventAttrPlatform}";
    
    /** 设备属性-平台关系 propId_platform */
    public static final String DEVICE_PROP_PLATFORM = "{devicePropPlatform}";
    
    /** 开启虚拟事件的 appId */
    public static final String VIRTUAL_EVENT_APPIDS_SET = "{virtualEventAppidsSet}";
    
    /** 开启虚拟属性的 appId */
    public static final String VIRTUAL_PROP_APP_IDS_SET = "{virtualPropAppIdsSet}";
    
    /** 虚拟属性 attrId */
    public static final String EVENT_VIRTUAL_ATTR_IDS_SET = "{eventVirtualAttrIdsSet}";

    // ==========================================================
    // 投放相关
    // ==========================================================
    
    /** 开启投放功能的 appKey -> appId */
    public static final String OPEN_ADVERTISING_FUNCTION_APP_MAP = "{openAdvertisingFunctionAppMap}";
    
    /** lid_eventId -> channelEvent */
    public static final String LID_AND_CHANNEL_EVENT_MAP = "{lidAndChannelEventMap}";
    
    /** appId -> s配置 */
    public static final String APP_ID_S_MAP = "{appIdSMap}";
    
    /** 广告频次去重 eventId_lid_zgId */
    public static final String AD_FREQUENCY_SET = "{adFrequencySet}";
    
    /** eventId_lid -> 广告链接配置 */
    public static final String ADS_LINK_EVENT_MAP = "{adsLinkEventMap}";

    // ==========================================================
    // 虚拟事件/属性相关
    // ==========================================================
    
    /** appId_owner_eventName -> 虚拟事件配置 JSON */
    public static final String VIRTUAL_EVENT_MAP = "{virtualEventMap}";
    
    /** appId_virtualEventName_owner_eventName -> 属性集合 */
    public static final String VIRTUAL_EVENT_ATTR_MAP = "{virtualEventAttrMap}";
    
    /** appId_owner_eventName_attrName -> alias */
    public static final String EVENT_ATTR_ALIAS_MAP = "{eventAttrAliasMap}";
    
    /** appId_eP_eventName -> 虚拟事件属性 */
    public static final String VIRTUAL_EVENT_PROP_MAP = "{virtualEventPropMap}";
    
    /** appId -> 虚拟用户属性 */
    public static final String VIRTUAL_USER_PROP_MAP = "{virtualUserPropMap}";

    // ==========================================================
    // DW模块 - Hash类型
    // ==========================================================
    
    /** eventId_attrId -> columnName (cus1-cus100) */
    public static final String EVENT_ATTR_COLUMN_MAP = "{eventAttrColumnMap}";
    
    /** baseName -> currentTableName */
    public static final String BASE_CURRENT_MAP = "{baseCurrentMap}";
    
    /** appId -> CDP配置 */
    public static final String OPEN_CDP_APPID_MAP = "{openCdpAppidMap}";
    
    /** day -> yearWeek */
    public static final String YEAR_WEEK = "{yearweek}";
    
    /** appId -> companyId */
    public static final String CID_BY_AID_MAP = "{cidByAidMap}";
    
    /** eqid -> keyword (百度关键词) */
    public static final String BAIDU_KEYWORD_MAP = "{baiduKeyword}";

    // ==========================================================
    // DW模块 - Set类型
    // ==========================================================
    
    /** companyId_identifier 业务标识 */
    public static final String BUSINESS_MAP = "{businessMap}";

    // ==========================================================
    // 同步元数据 (不需要花括号，不涉及 RENAME)
    // ==========================================================
    
    public static final String SYNC_VERSION = "sync:version";
    public static final String SYNC_TIMESTAMP = "sync:timestamp";
    public static final String SYNC_STATUS = "sync:status";

    // ==========================================================
    // 运行时 ID 映射 (Flink 算子写入，不需要花括号)
    // ==========================================================
    
    /** 设备ID映射: d:{appId} -> Hash(deviceMd5 -> deviceId) */
    public static final String DEVICE_ID_PREFIX = "d:";
    
    /** 用户ID映射: u:{appId} -> Hash(propertyValue -> userId) */
    public static final String USER_ID_PREFIX = "u:";
    
    /** userId->zgId映射: uz:{appId} -> Hash(userId -> zgId) */
    public static final String USER_ZGID_PREFIX = "uz:";
    
    /** zgId->userId映射: zu:{appId} -> Hash(zgId -> userId) */
    public static final String ZGID_USER_PREFIX = "zu:";
    
    /** deviceId->zgId映射: dz:{appId} -> Hash(deviceId -> zgId) */
    public static final String ZGID_DEVICE_PREFIX = "dz:";
    
    /** 搜索关键词: sk:{keyword} */
    public static final String SEARCH_KEYWORD_PREFIX = "sk:";
}
