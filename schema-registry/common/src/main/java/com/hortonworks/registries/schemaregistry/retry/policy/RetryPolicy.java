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

import com.hortonworks.registries.schemaregistry.retry.exception.RetryPolicyException;

import java.util.Map;

public abstract class RetryPolicy {

    protected Long sleepTimeMs;
    protected Integer maxRetries;
    protected Long maxSleepTimeMs;

    public static final String SLEEP_TIME_MS = "sleepTimeMs";
    public static final String MAX_RETRIES = "maxRetries";
    public static final String MAX_SLEEP_TIME_MS = "maxSleepTimeMs";

    protected RetryPolicy() {

    }

    protected RetryPolicy(Long sleepTimeMs, Integer maxRetries, Long maxSleepTimeMs) {
        this.sleepTimeMs = sleepTimeMs;
        this.maxRetries = maxRetries;
        this.maxSleepTimeMs = maxSleepTimeMs;
    }

    public abstract void init(Map<String, Object> properties);

    public boolean mayBeSleep(int iteration, long timeElapsed) {
        if (iteration > maxRetries) {
            return false;
        }

        long sleepTime = sleepTime(iteration, timeElapsed);

        if (sleepTime + timeElapsed > this.maxSleepTimeMs) {
            return false;
        }

        sleep(sleepTime);

        return true;
    }

    abstract long sleepTime(int iteration, long timeElapsed);

    protected void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new RetryPolicyException(e);
        }
    }

}
