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
import com.hortonworks.registries.schemaregistry.retry.block.RetryableBlock;

import java.util.Map;

/**
 *   FixedTimeBackoffPolicy allows a user to retry an instance of {@link RetryableBlock}
 *   after a fixed time. FixedTimeBackoffPolicy is both time bound and attempt bound, meaning you can declare max attempts
 *   and timeout with in which reattempt would be triggered for an instance of {@link RetryableBlock}
 */

public class FixedTimeBackoffPolicy extends BackoffPolicy {

    private static final long DEFAULT_SLEEP_TIME_MS = 1000L;
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final long DEFAULT_TIMEOUT_MS = 60_000L;

    public FixedTimeBackoffPolicy() {

    }

    public FixedTimeBackoffPolicy(Long sleepTimeMs, Integer maxAttempts, Long maxSleepTimeMs) {
        super(sleepTimeMs, maxAttempts, maxSleepTimeMs);
    }

    @Override
    public void init(Map<String, Object> properties) {
        this.sleepTimeMs = Long.valueOf(properties.getOrDefault(SLEEP_TIME_MS, DEFAULT_SLEEP_TIME_MS).toString());
        this.maxAttempts = Integer.valueOf(properties.getOrDefault(MAX_ATTEMPTS, DEFAULT_MAX_RETRIES).toString());
        this.timeoutMs = Long.valueOf(properties.getOrDefault(TIMEOUT_MS, DEFAULT_TIMEOUT_MS).toString());
    }

    @Override
    @VisibleForTesting
    long sleepTime(int attemptNumber, long timeElapsed) {
        return sleepTimeMs;
    }

    @Override
    public String toString() {
        return "FixedTimeBackoffPolicy{" +
                "sleepTimeMs=" + sleepTimeMs +
                ", maxAttempts=" + maxAttempts +
                ", maxSleepTimeMs=" + timeoutMs +
                '}';
    }
}
