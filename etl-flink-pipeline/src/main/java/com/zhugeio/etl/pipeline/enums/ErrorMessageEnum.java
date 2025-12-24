package com.zhugeio.etl.pipeline.enums;

import lombok.Getter;

import java.io.Serializable;

@Getter
public enum ErrorMessageEnum implements Serializable {

    // ========== 原有枚举 (保持不变) ==========
    IP_BLOCK(1010001, "ip为黑名单", "Ip被配置为黑名单，详见mysql表data_access_filter"),
    UA_BLOCK(1010002, "ua为黑名单", "user-agent被配置为黑名单，详见mysql表data_access_filter"),
    IP_NONE(1010003, "Ip字段获取异常", "Ip字段不存在或不是字符串类型"),
    NOW_NONE(1010004, "Now字段获取异常", "Now字段不存在或不是数值类型"),
    BASIC_SCHEMA_FORMAT_NOT_MATCH(1020002, "转换后的json与basicSchema标准不符", "json中的要字段不存在或json结构有误，详见basicSchema文档"),
    HEADER_NONE(1010005, "Header字段解析异常", "Header字段不存在或不是字符串类型"),
    AK_NONE(1010006, "ak在应用管理中不存在或已被删除", "ak在应用管理中不存在或已被删除"),
    JSON_FORMAT_ERROR(1020001, "json解析异常", "校验pay_statisv2主题的数据是否符合json格式"),
    DID_NONE(1020004, "did获取异常", "did值为空或不存在"),
    EID_NONE(1020005, "$eid获取异常", "$eid值为空或不存在"),
    MKT_SEND_ZG_ID_NONE(1020006, "触达事件获取zg_id异常", "did按'-'切割出的第2个数据非数值型"),
    EVENT_NAME_LENGTH_LIMIT(1020007, "事件名称长度超过限制", "事件名称长度超过限制128个字符"),
    MKT_EVENT_NOT_SPECIFIED(1020008, "触达事件未被指定", "owner= 'mkt'表明是触达事件，该事件在mysql表etl_sdk_config中sdk_key='mktEvents'条件下不存在"),
    BUILTIN_EVENT_NOT_SPECIFIED(1020009, "内置事件未被指定", "owner= 'abp'表明是内置事件，该事件在mysql表etl_sdk_config中sdk_key='abpEvents'条件下不存在"),
    EVENT_NUMBER_LIMIT(1020010, "事件个数超限制或自定义事件已禁用自动创建", "该应用在用的事件id个数超过mysql表company_app设置的值event_sum或者auto_event字段被设置为0"),
    EVENT_ATTR_ID_ERROR(1020011, "事件属性id生成失败", "应用中事件的属性个数超过mysql表company_app表设置的值attr_sum 或者auto_event字段被设置为0"),
    EVENT_TYPE_ERROR(1020012, "事件类型dt错误", "事件类型dt错误"),
    EVENT_NAME_INVALID(1020013, "事件名不合法", "事件名包含了除数字、字母、下划线、中划线、汉字、$以外字符"),
    EVENT_ATTR_INVALID(1020014, "事件属性不合法", "事件属性包含了除数字、字母、下划线、中划线、汉字、$以外字符"),
    VIRTUAL_ATTR_FIELD(1020015, "虚拟属性处理失败", "虚拟属性处理失败"),
    NONE_ERROR(0000000, "无异常", "无异常，供代码处理使用"),
    ZG_ZGID_NONE(1030001, "入库核心字段$zg_zgid缺失", "入库核心字段$zg_zgid缺失"),
    ZG_EID_NONE(1030002, "入库核心字段$zg_eid缺失", "入库核心字段$zg_eid缺失"),
    ZG_DID_NONE(1030003, "入库核心字段$zg_did缺失", "入库核心字段$zg_did缺失"),
    CT_TZ_NONE(1030004, "入库核心字段$ct或$tz缺失", "入库核心字段$ct或$tz缺失"),
    EVENT_TIME_EXCEEDS_RANGE(1030005, "事件时间不在配置时间区间内", "dw模块默认配置只入库事件事件在近7天的事件，早于该时间的数据不再入库"),

    // ========== 新增枚举值 (用于算子) ==========
    
    // 事件相关 (102010x)
    EVENT_NOT_FOUND(1020101, "事件ID获取失败", "事件在KVRocks中不存在且创建失败"),
    EVENT_BLACK(1020102, "事件在黑名单中", "该事件已被添加到黑名单，不再处理"),
    EVENT_CREATE_DISABLED(1020103, "事件自动创建被禁用", "该应用已禁用自动创建新事件"),
    EVENT_COUNT_EXCEED(1020104, "事件数量超过限制", "该应用的事件数量已达到上限"),
    
    // 事件属性相关 (102011x)
    EVENT_ATTR_NOT_FOUND(1020111, "事件属性ID获取失败", "事件属性在KVRocks中不存在且创建失败"),
    EVENT_ATTR_BLACK(1020112, "事件属性在黑名单中", "该事件属性已被添加到黑名单"),
    EVENT_ATTR_CREATE_DISABLED(1020113, "事件属性自动创建被禁用", "该事件已禁用自动创建新属性"),
    EVENT_ATTR_COUNT_EXCEED(1020114, "事件属性数量超过限制", "单个事件的属性数量已达到上限"),
    
    // 用户属性相关 (102012x)
    USER_PROP_NOT_FOUND(1020121, "用户属性ID获取失败", "用户属性在KVRocks中不存在且创建失败"),
    USER_PROP_BLACK(1020122, "用户属性在黑名单中", "该用户属性已被添加到黑名单"),
    USER_PROP_CREATE_DISABLED(1020123, "用户属性自动创建被禁用", "该应用已禁用自动创建新用户属性"),
    USER_PROP_COUNT_EXCEED(1020124, "用户属性数量超过限制", "该应用的用户属性数量已达到上限"),
    
    // ID映射相关 (102013x)
    DEVICE_ID_MAPPING_FAILED(1020131, "设备ID映射失败", "设备ID在KVRocks中映射失败"),
    USER_ID_MAPPING_FAILED(1020132, "用户ID映射失败", "用户ID在KVRocks中映射失败"),
    ZGID_MAPPING_FAILED(1020133, "诸葛ID映射失败", "诸葛ID在KVRocks中映射失败"),
    
    // 通用
    UNKNOWN_ERROR(9999999, "未知错误", "未知错误");

    private final int errorCode;
    private final String errorMessage;
    private final String errorDescribe;

    ErrorMessageEnum(int errorCode, String errorMessage, String errorDescribe) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.errorDescribe = errorDescribe;
    }

    /**
     * 获取错误码 (兼容 getCode() 调用)
     */
    public int getCode() {
        return errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getErrorDescribe() {
        return errorDescribe;
    }

    /**
     * 根据错误码获取枚举
     */
    public static ErrorMessageEnum getByCode(int code) {
        for (ErrorMessageEnum e : values()) {
            if (e.getErrorCode() == code) {
                return e;
            }
        }
        return UNKNOWN_ERROR;
    }

    @Override
    public String toString() {
        return "ErrorMessageEnum{" +
                "errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
