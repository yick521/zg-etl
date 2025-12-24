package com.zhugeio.etl.common.cache;

import java.io.Serializable;

/**
 * 缓存配置类
 */
public class CacheConfig implements Serializable {

    private static final long serialVersionUID = 2L;

    // KVRocks 连接配置
    private String kvrocksHost = "localhost";
    private int kvrocksPort = 6379;
    private boolean kvrocksCluster = false;
    private int kvrocksTimeout = 60;
    private String kvrocksPassword = null;

    private CacheConfig() {}

    // Getters
    public String getKvrocksHost() { return kvrocksHost; }
    public int getKvrocksPort() { return kvrocksPort; }
    public boolean isKvrocksCluster() { return kvrocksCluster; }
    public int getKvrocksTimeout() { return kvrocksTimeout; }
    public String getKvrocksPassword() { return kvrocksPassword; }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final CacheConfig config = new CacheConfig();

        public Builder kvrocksHost(String host) { config.kvrocksHost = host; return this; }
        public Builder kvrocksPort(int port) { config.kvrocksPort = port; return this; }
        public Builder kvrocksCluster(boolean cluster) { config.kvrocksCluster = cluster; return this; }
        public Builder kvrocksTimeout(int timeout) { config.kvrocksTimeout = timeout; return this; }
        public Builder kvrocksPassword(String password) { config.kvrocksPassword = password; return this; }
        public CacheConfig build() { return config; }
    }

    @Override
    public String toString() {
        return "CacheConfig{host='" + kvrocksHost + "', port=" + kvrocksPort + ", cluster=" + kvrocksCluster + "}";
    }
}
