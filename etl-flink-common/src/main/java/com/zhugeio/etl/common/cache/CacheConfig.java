package com.zhugeio.etl.common.cache;

import java.io.Serializable;

/**
 * 缓存配置类
 */
public class CacheConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // KVRocks 连接配置
    private String kvrocksHost = "localhost";
    private int kvrocksPort = 6379;
    private boolean kvrocksCluster = false;
    private int kvrocksTimeout = 60;

    // 缓存大小配置 (建议值)
    private int appCacheSize = 10000;
    private int eventCacheSize = 100000;
    private int eventAttrCacheSize = 200000;
    private int propCacheSize = 100000;
    private int blacklistCacheSize = 10000;
    private int virtualCacheSize = 10000;

    // 缓存过期时间 (分钟)
    private int appCacheExpireMinutes = 10;
    private int eventCacheExpireMinutes = 30;
    private int eventAttrCacheExpireMinutes = 30;
    private int propCacheExpireMinutes = 30;
    private int blacklistCacheExpireMinutes = 30;
    private int virtualCacheExpireMinutes = 5;

    private CacheConfig() {}

    // ===================== Getters =====================

    public String getKvrocksHost() { return kvrocksHost; }
    public int getKvrocksPort() { return kvrocksPort; }
    public boolean isKvrocksCluster() { return kvrocksCluster; }
    public int getKvrocksTimeout() { return kvrocksTimeout; }

    public int getAppCacheSize() { return appCacheSize; }
    public int getEventCacheSize() { return eventCacheSize; }
    public int getEventAttrCacheSize() { return eventAttrCacheSize; }
    public int getPropCacheSize() { return propCacheSize; }
    public int getBlacklistCacheSize() { return blacklistCacheSize; }
    public int getVirtualCacheSize() { return virtualCacheSize; }

    public int getAppCacheExpireMinutes() { return appCacheExpireMinutes; }
    public int getEventCacheExpireMinutes() { return eventCacheExpireMinutes; }
    public int getEventAttrCacheExpireMinutes() { return eventAttrCacheExpireMinutes; }
    public int getPropCacheExpireMinutes() { return propCacheExpireMinutes; }
    public int getBlacklistCacheExpireMinutes() { return blacklistCacheExpireMinutes; }
    public int getVirtualCacheExpireMinutes() { return virtualCacheExpireMinutes; }

    // ===================== Builder =====================

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final CacheConfig config = new CacheConfig();

        public Builder kvrocksHost(String host) {
            config.kvrocksHost = host;
            return this;
        }

        public Builder kvrocksPort(int port) {
            config.kvrocksPort = port;
            return this;
        }

        public Builder kvrocksCluster(boolean cluster) {
            config.kvrocksCluster = cluster;
            return this;
        }

        public Builder kvrocksTimeout(int timeout) {
            config.kvrocksTimeout = timeout;
            return this;
        }

        public Builder appCacheSize(int size) {
            config.appCacheSize = size;
            return this;
        }

        public Builder eventCacheSize(int size) {
            config.eventCacheSize = size;
            return this;
        }

        public Builder eventAttrCacheSize(int size) {
            config.eventAttrCacheSize = size;
            return this;
        }

        public Builder propCacheSize(int size) {
            config.propCacheSize = size;
            return this;
        }

        public Builder blacklistCacheSize(int size) {
            config.blacklistCacheSize = size;
            return this;
        }

        public Builder virtualCacheSize(int size) {
            config.virtualCacheSize = size;
            return this;
        }

        public Builder appCacheExpireMinutes(int minutes) {
            config.appCacheExpireMinutes = minutes;
            return this;
        }

        public Builder eventCacheExpireMinutes(int minutes) {
            config.eventCacheExpireMinutes = minutes;
            return this;
        }

        public Builder eventAttrCacheExpireMinutes(int minutes) {
            config.eventAttrCacheExpireMinutes = minutes;
            return this;
        }

        public Builder propCacheExpireMinutes(int minutes) {
            config.propCacheExpireMinutes = minutes;
            return this;
        }

        public Builder blacklistCacheExpireMinutes(int minutes) {
            config.blacklistCacheExpireMinutes = minutes;
            return this;
        }

        public Builder virtualCacheExpireMinutes(int minutes) {
            config.virtualCacheExpireMinutes = minutes;
            return this;
        }

        public CacheConfig build() {
            return config;
        }
    }

    @Override
    public String toString() {
        return "CacheConfig{" +
                "kvrocksHost='" + kvrocksHost + '\'' +
                ", kvrocksPort=" + kvrocksPort +
                ", kvrocksCluster=" + kvrocksCluster +
                '}';
    }
}
