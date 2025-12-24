package com.zhugeio.etl.pipeline.enums;

import lombok.Getter;

/**
 * @author ningjh
 * @name ArchiveType
 * @date 2025/12/24
 * @description 映射类型枚举
 */
public enum ArchiveType {

    DEVICE(1,"DEVICE","设备MD5 → zgDeviceId"),
    USER(2,"USER","用户id → zgUserId"),
    DEVICE_ZGID(3,"DEVICE_ZGID","zgDeviceId → zgId"),
    USER_ZGID(4,"USER_ZGID","zgUserId → zgId"),
    ZGID_USER(5,"ZGID_USER","zgId → zgUserId (反向映射)"),
    UNKNOWN(-1,"UNKNOWN","未知的类型");

    @Getter
    private final int code;
    private final String name;
    private final String describe;

    ArchiveType(int code, String name, String describe) {
        this.code = code;
        this.name = name;
        this.describe = describe;
    }

    /**
     * 根据错误码获取枚举
     */
    public static ArchiveType getByCode(int code) {
        for (ArchiveType e : values()) {
            if (e.getCode() == code) {
                return e;
            }
        }
        return UNKNOWN;
    }

    @Override
    public String toString() {
        return "ArchiveType2{" +
                "code=" + code +
                ", name='" + name + '\'' +
                ", describe='" + describe + '\'' +
                '}';
    }
}
