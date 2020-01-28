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

public class ExponentialBackoffRetryPolicy extends RetryPolicy {

    public static final String EXPONENT = "exponent";

    private static final long DEFAULT_SLEEP_TIME_MS = 1000L;
    private static final float DEFAULT_EXPONENT = 2;
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final long DEFAULT_MAX_SLEEP_TIME_MS = 90_000L;

    private Float exponent;

    public ExponentialBackoffRetryPolicy() {

    }

    public ExponentialBackoffRetryPolicy(Long sleepTimeMs, Float exponent, Integer maxRetries, Long maxSleepTimeMs) {
        super(sleepTimeMs, maxRetries, maxSleepTimeMs);
        this.exponent = exponent;
    }

    @Override
    public void init(Map<String, Object> properties) {
        this.sleepTimeMs = (Long) properties.getOrDefault(SLEEP_TIME_MS, DEFAULT_SLEEP_TIME_MS);
        this.maxRetries = (Integer) properties.getOrDefault(MAX_RETRIES, DEFAULT_MAX_RETRIES);
        this.maxSleepTimeMs = (Long) properties.getOrDefault(MAX_SLEEP_TIME_MS, DEFAULT_MAX_SLEEP_TIME_MS);
        this.exponent = (Float) properties.getOrDefault(EXPONENT, DEFAULT_EXPONENT);
    }

    @Override
    @VisibleForTesting
    long sleepTime(int iteration, long timeElapsed) {
        return (long) (this.sleepTimeMs * Math.pow(exponent, iteration));
    }
}
