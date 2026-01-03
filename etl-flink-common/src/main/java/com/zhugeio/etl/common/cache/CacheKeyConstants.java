package com.zhugeio.etl.common.cache;

/**
 * 缓存 Key 常量定义
 *
 */
public final class CacheKeyConstants {

    private CacheKeyConstants() {}


    /**
     * appKey → appId 映射
     * Field: ${appKey}
     * Value: ${appId}
     */
    public static final String APP_KEY_APP_ID_MAP = "appKeyAppIdMap";

    /**
     * app 平台数据状态
     * Field: ${appId}_${platform}
     * Value: ${hasData} (0/1)
     */
    public static final String APP_ID_SDK_HAS_DATA_MAP = "appIdSdkHasDataMap";

    /**
     * 用户属性 ID 映射
     * Field: ${appId}_${owner}_${name(大写)}
     * Value: ${propId}
     */
    public static final String APP_ID_PROP_ID_MAP = "appIdPropIdMap";

    /**
     * 用户属性原始名称映射
     * Field: ${appId}_${owner}_${propId}
     * Value: ${propName(原始大小写)}
     */
    public static final String APP_ID_PROP_ID_ORIGINAL_MAP = "appIdPropIdOriginalMap";

    /**
     * 事件 ID 映射
     * Field: ${appId}_${owner}_${eventName} #TODO:${appId}_${owner}_${eventName}_{platform} 同步那里也要加
     * Value: ${eventId}
     */
    public static final String APP_ID_EVENT_ID_MAP = "appIdEventIdMap";

    /**
     * 事件属性 ID 映射
     * Field: ${appId}_${eventId}_${owner}_${attrName(大写)}
     * Value: ${attrId}
     */
    public static final String APP_ID_EVENT_ATTR_ID_MAP = "appIdEventAttrIdMap";

    /**
     * 设备属性 ID 映射
     * Field: ${appId}_${owner}_${propName}
     * Value: ${propId}
     */
    public static final String APP_ID_DEVICE_PROP_ID_MAP = "appIdDevicePropIdMap";

    /**
     * 事件属性列名映射
     * Field: ${eventId}_${attrId}
     * Value: ${columnName} (cus1, cus2, ...)
     */
    public static final String EVENT_ATTR_COLUMN_MAP = "eventAttrColumnMap";

    /**
     * appId → companyId 映射
     * Field: ${appId}
     * Value: ${companyId}
     */
    public static final String CID_BY_AID_MAP = "cidByAidMap";

    /**
     * 开启 CDP 的应用配置
     * Field: ${appId}
     * Value: ${appConfig}
     */
    public static final String OPEN_CDP_APPID_MAP = "openCdpAppidMap";

    /**
     * 事件属性别名映射
     * Field: ${appId}_${owner}_${eventName}_${attrName}
     * Value: ${aliasName}
     */
    public static final String EVENT_ATTR_ALIAS_MAP = "eventAttrAliasMap";

    // ==========================================================
    // 虚拟事件/属性相关 Hash
    // ==========================================================

    /**
     * 虚拟事件映射
     * Field: ${appId}_${owner}_${eventName}
     * Value: JSON Array
     */
    public static final String VIRTUAL_EVENT_MAP = "virtualEventMap";

    /**
     * 虚拟事件属性映射
     * Field: ${appId}_${virtualEventName}_${owner}_${eventName}
     * Value: JSON Set (属性名集合)
     */
    public static final String VIRTUAL_EVENT_ATTR_MAP = "virtualEventAttrMap";

    /**
     * 虚拟事件属性 (事件级虚拟属性)
     * Field: ${appId}_eP_${eventName}
     * Value: JSON Array
     */
    public static final String VIRTUAL_EVENT_PROP_MAP = "virtualEventPropMap";

    /**
     * 虚拟用户属性
     * Field: ${appId}
     * Value: JSON Array
     */
    public static final String VIRTUAL_USER_PROP_MAP = "virtualUserPropMap";

    // ==========================================================
    // Set 类型
    // ==========================================================

    /**
     * 禁止创建事件的应用 ID 集合
     * Member: ${appId}
     */
    public static final String APP_ID_CREATE_EVENT_FORBID_SET = "appIdCreateEventForbidSet";

    /**
     * 已上传数据的应用 ID 集合
     * Member: ${appId}
     */
    public static final String APP_ID_UPLOAD_DATA_SET = "appIdUploadDataSet";

    /**
     * 黑名单用户属性 ID 集合
     * Member: ${propId}
     */
    public static final String BLACK_USER_PROP_SET = "blackUserPropSet";

    /**
     * 黑名单事件 ID 集合
     * Member: ${eventId}
     */
    public static final String BLACK_EVENT_ID_SET = "blackEventIdSet";

    /**
     * 非自动创建应用 ID 集合 (auto_event=0)
     * Member: ${appId}
     */
    public static final String APP_ID_NONE_AUTO_CREATE_SET = "appIdNoneAutoCreateSet";

    /**
     * 黑名单事件属性 ID 集合
     * Member: ${attrId}
     */
    public static final String BLACK_EVENT_ATTR_ID_SET = "blackEventAttrIdSet";

    /**
     * 禁止创建属性的事件 ID 集合
     * Member: ${eventId}
     */
    public static final String EVENT_ID_CREATE_ATTR_FORBIDDEN_SET = "eventIdCreateAttrForbiddenSet";

    /**
     * 事件平台映射集合
     * Member: ${eventId}_${platform}
     */
    public static final String EVENT_ID_PLATFORM = "eventIdPlatform";

    /**
     * 事件属性平台映射集合
     * Member: ${attrId}_${platform}
     */
    public static final String EVENT_ATTR_PLATFORM = "eventAttrdPlatform";

    /**
     * 设备属性平台映射集合
     * Member: ${propId}_${platform}
     */
    public static final String DEVICE_PROP_PLATFORM = "devicePropPlatform";

    /**
     * 虚拟事件应用 ID 集合
     * Member: ${appId}
     */
    public static final String VIRTUAL_EVENT_APPIDS_SET = "virtualEventAppidsSet";

    /**
     * 虚拟属性应用 ID 集合
     * Member: ${appId}
     */
    public static final String VIRTUAL_PROP_APP_IDS_SET = "virtualPropAppIdsSet";

    /**
     * 虚拟事件属性 ID 集合
     * Member: ${attrId}
     */
    public static final String EVENT_VIRTUAL_ATTR_IDS_SET = "eventVirtualAttrIdsSet";

    /**
     * 业务标识集合
     * Member: ${companyId}_${identifier}
     */
    public static final String BUSINESS_MAP = "businessMap";


    /**
     * appId 到 ip黑名单 ua黑名单的映射
     * Hash Key:ipBlackMap
     * Field: ${appId}
     * Value: ${ip黑名单字符串}
     * value 格式为 ["a","b"]
     */
    public static final String IP_BLACK_MAP = "ipBlackMap";

    /**
     * appId 到 ip黑名单 ua黑名单的映射
     * Hash Key:ipBlackMap
     * Field: ${appId}
     * Value: ${ua黑名单字符串}
     * value 格式为 ["a","b"]
     */
    public static final String UA_BLACK_MAP = "uaBlackMap";

    // ==========================================================
    // 同步元数据
    // ==========================================================

    /**
     * 同步版本号
     */
    public static final String SYNC_VERSION = "sync:version";

    /**
     * 同步时间戳
     */
    public static final String SYNC_TIMESTAMP = "sync:timestamp";

    /**
     * 同步状态
     */
    public static final String SYNC_STATUS = "sync:status";

    // ==========================================================
    // DW 模块
    // ==========================================================

    /**
     * Kudu 表当前名称映射
     * Field: ${baseName}
     * Value: ${currentName}
     */
    public static final String BASE_CURRENT_MAP = "baseCurrentMap";

    /**
     * 年周映射
     * Field: ${day}
     * Value: ${yearWeek}
     */
    public static final String YEAR_WEEK = "yearweek";

    // ==========================================================
    // 工具方法
    // ==========================================================

    /**
     * 构建事件 ID 查询的 Field
     */
    public static String eventIdField(Integer appId, String owner, String eventName) {
        return appId + "_" + owner + "_" + eventName;
    }

    /**
     * 构建事件属性 ID 查询的 Field
     * 注意: attrName 需要转大写
     */
    public static String eventAttrIdField(Integer appId, Integer eventId, String owner, String attrName) {
        return appId + "_" + eventId + "_" + owner + "_" + attrName.toUpperCase();
    }

    /**
     * 构建事件属性列名查询的 Field (Integer版本)
     */
    public static String eventAttrColumnField(Integer eventId, Integer attrId) {
        return eventId + "_" + attrId;
    }

    /**
     * 构建事件属性列名查询的 Field (String版本，支持雪花ID)
     */
    public static String eventAttrColumnField(String eventId, String attrId) {
        return eventId + "_" + attrId;
    }

    /**
     * 构建事件属性列名查询的 Field (Long版本，支持雪花ID)
     */
    public static String eventAttrColumnField(Long eventId, Long attrId) {
        return eventId + "_" + attrId;
    }

    /**
     * 构建用户属性 ID 查询的 Field
     * 注意: propName 需要转大写
     */
    public static String userPropIdField(Integer appId, String owner, String propName) {
        return appId + "_" + owner + "_" + propName.toUpperCase();
    }

    /**
     * 构建用户属性原始名称查询的 Field
     */
    public static String userPropOriginalField(Integer appId, String owner, Integer propId) {
        return appId + "_" + owner + "_" + propId;
    }

    /**
     * 构建设备属性 ID 查询的 Field
     */
    public static String devicePropIdField(Integer appId, String owner, String propName) {
        return appId + "_" + owner + "_" + propName;
    }

    /**
     * 构建 SDK 有数据查询的 Field
     */
    public static String sdkHasDataField(Integer appId, Integer platform) {
        return appId + "_" + platform;
    }

    /**
     * 构建事件平台 Set Member
     */
    public static String eventPlatformMember(Integer eventId, Integer platform) {
        return eventId + "_" + platform;
    }

    /**
     * 构建事件属性平台 Set Member
     */
    public static String eventAttrPlatformMember(Integer attrId, Integer platform) {
        return attrId + "_" + platform;
    }

    /**
     * 构建设备属性平台 Set Member
     */
    public static String devicePropPlatformMember(Integer propId, Integer platform) {
        return propId + "_" + platform;
    }

    /**
     * 构建虚拟事件查询的 Field
     */
    public static String virtualEventField(Integer appId, String owner, String eventName) {
        return appId + "_" + owner + "_" + eventName;
    }

    /**
     * 构建虚拟事件属性查询的 Field
     */
    public static String virtualEventAttrField(Integer appId, String virtualEventName,
                                               String owner, String eventName) {
        return appId + "_" + virtualEventName + "_" + owner + "_" + eventName;
    }

    /**
     * 构建虚拟事件属性 (事件级) 查询的 Field
     */
    public static String virtualEventPropField(Integer appId, String eventName) {
        return appId + "_eP_" + eventName;
    }

    /**
     * 构建事件属性别名查询的 Field
     */
    public static String eventAttrAliasField(Integer appId, String owner,
                                             String eventName, String attrName) {
        return appId + "_" + owner + "_" + eventName + "_" + attrName;
    }

}