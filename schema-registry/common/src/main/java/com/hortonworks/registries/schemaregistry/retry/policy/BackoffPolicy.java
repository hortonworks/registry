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

import java.util.Map;

public abstract class BackoffPolicy {

    protected Long sleepTimeMs;
    protected Integer maxAttempts;
    protected Long timeoutMs;

    public static final String SLEEP_TIME_MS = "sleepTimeMs";
    public static final String MAX_ATTEMPTS = "maxAttempts";
    public static final String TIMEOUT_MS = "timeoutMs";

    protected BackoffPolicy() {

    }

    protected BackoffPolicy(Long sleepTimeMs, Integer maxAttempts, Long timeoutMs) {
        this.sleepTimeMs = sleepTimeMs;
        this.maxAttempts = maxAttempts;
        this.timeoutMs = timeoutMs;
    }

    public abstract void init(Map<String, Object> properties);

    public boolean mayBeSleep(int attemptNumber, long timeElapsed) {
        if (attemptNumber >= maxAttempts) {
            return false;
        }

        long sleepTime = sleepTime(attemptNumber, timeElapsed);

        if (sleepTime + timeElapsed > this.timeoutMs) {
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
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
