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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class ExponentialBackoffRetryPolicy extends RetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffRetryPolicy.class);

    public static final String BASE_SLEEP_TIME_MS = "baseSleepTimeMs";
    public static final String MULTIPLIER = "multiplier";
    public static final String MAX_RETRIES = "maxRetries";
    public static final String MAX_SLEEP_TIME_MS = "maxSleepTimeMs";

    private static final long DEFAULT_BASE_SLEEP_TIME_MS = 1000L;
    private static final float DEFAULT_MULTIPLIER = 2;
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final long DEFAULT_MAX_SLEEP_TIME_MS = 90_000L;

    private Long baseSleepTimeMs;
    private Float multiplier;
    private Integer maxRetries;
    private Long maxSleepTimeMs;

    public ExponentialBackoffRetryPolicy() {

    }

    @Override
    public void init(Map<String, Object> properties) {
        this.baseSleepTimeMs = (Long) properties.getOrDefault(BASE_SLEEP_TIME_MS, DEFAULT_BASE_SLEEP_TIME_MS);
        this.maxRetries = (Integer) properties.getOrDefault(MAX_RETRIES, DEFAULT_MAX_RETRIES);
        this.maxSleepTimeMs = (Long) properties.getOrDefault(MAX_SLEEP_TIME_MS, DEFAULT_MAX_SLEEP_TIME_MS);
        this.multiplier = (Float) properties.getOrDefault(MULTIPLIER, DEFAULT_MULTIPLIER);
    }

    @Override
    public boolean mayBeSleep(int iteration, long timeElapsed) {
        if (iteration > maxRetries) {
            return false;
        }

        long sleepMs = (long) (this.baseSleepTimeMs * Math.pow(multiplier, iteration));
        if (sleepMs + timeElapsed > this.maxSleepTimeMs) {
            return false;
        }

        sleep(sleepMs);

        return true;
    }
}
