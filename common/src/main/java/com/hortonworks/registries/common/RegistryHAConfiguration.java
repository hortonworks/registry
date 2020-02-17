package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class RegistryHAConfiguration {

    @JsonProperty("peer.list.refresh.interval.ms")
    private long peerListRefreshIntervalMs;

    @JsonProperty("peer.entry.expiry.time.ms")
    private long peerEntryExpiryTimeMs;

    @JsonProperty("sslConfig")
    private Map<String, String> sslConfig;

    public long getPeerListRefreshIntervalMs() {
        return peerListRefreshIntervalMs;
    }

    public long getPeerEntryExpiryTimeMs() {
        return peerEntryExpiryTimeMs;
    }

    public Map<String, String> getSslConfig() {
        return sslConfig;
    }

    @Override
    public String toString() {
        return "RegistryHAConfiguration{" +
                "peerListRefreshIntervalMs=" + peerListRefreshIntervalMs +
                ", peerEntryExpiryTimeMs=" + peerEntryExpiryTimeMs +
                ", sslConfig=" + sslConfig +
                '}';
    }
}
