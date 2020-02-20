/**
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class HAConfiguration {

    @JsonProperty("heartbeat.interval.ms")
    private long heartbeatIntervalMs;

    @JsonProperty("peer.list.expiry.time.ms")
    private long peerListExpiryTimeMs;

    @JsonProperty("cache.invalidation.retry.policy")
    private Map<String, ?> cacheInvalidationRetryPolicy;

    @JsonProperty("cache.invalidation.thread.count")
    private int cacheInvalidationThreadCount;

    @JsonProperty("sslConfig")
    private Map<String, String> sslConfig;

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public long getPeerListExpiryTimeMs() {
        return peerListExpiryTimeMs;
    }

    public Map<String, ?> getCacheInvalidationRetryPolicy() {
        return cacheInvalidationRetryPolicy;
    }

    public int getCacheInvalidationThreadCount() {
        return cacheInvalidationThreadCount;
    }

    public Map<String, String> getSslConfig() {
        return sslConfig;
    }

    @Override
    public String toString() {
        return "HAConfiguration{" +
                "heartbeatIntervalMs=" + heartbeatIntervalMs +
                ", peerListExpiryTimeMs=" + peerListExpiryTimeMs +
                ", cacheInvalidationRetryPolicy=" + cacheInvalidationRetryPolicy +
                ", cacheInvalidationThreadCount=" + cacheInvalidationThreadCount +
                ", sslConfig=" + sslConfig +
                '}';
    }
}
