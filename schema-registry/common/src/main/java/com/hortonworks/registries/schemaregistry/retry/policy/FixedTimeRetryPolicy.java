/*
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

package com.hortonworks.registries.schemaregistry.retry.policy;

import com.google.common.annotations.VisibleForTesting;

import java.util.Map;

public class FixedTimeRetryPolicy extends RetryPolicy {

    private static final long DEFAULT_SLEEP_TIME_MS = 5000L;
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final long DEFAULT_MAX_SLEEP_TIME_MS = 90_000L;

    public FixedTimeRetryPolicy() {

    }

    public FixedTimeRetryPolicy(Long sleepTimeMs, Integer maxRetries, Long maxSleepTimeMs) {
        super(sleepTimeMs, maxRetries, maxSleepTimeMs);
    }

    @Override
    public void init(Map<String, Object> properties) {
        this.sleepTimeMs = Long.valueOf(properties.getOrDefault(SLEEP_TIME_MS, DEFAULT_SLEEP_TIME_MS).toString());
        this.maxRetries = Integer.valueOf(properties.getOrDefault(MAX_RETRIES, DEFAULT_MAX_RETRIES).toString());
        this.maxSleepTimeMs = Long.valueOf(properties.getOrDefault(MAX_SLEEP_TIME_MS, DEFAULT_MAX_SLEEP_TIME_MS).toString());
    }

    @Override
    @VisibleForTesting
    long sleepTime(int iteration, long timeElapsed) {
        return sleepTimeMs;
    }

    @Override
    public String toString() {
        return "FixedTimeRetryPolicy{" +
                "sleepTimeMs=" + sleepTimeMs +
                ", maxRetries=" + maxRetries +
                ", maxSleepTimeMs=" + maxSleepTimeMs +
                '}';
    }
}
