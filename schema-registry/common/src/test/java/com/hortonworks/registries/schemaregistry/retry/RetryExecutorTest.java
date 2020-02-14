/*
 * Copyright 2016-2019 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.registries.schemaregistry.retry;

import com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.FixedTimeBackoffPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.BackoffPolicy;
import com.hortonworks.registries.util.CustomParameterizedRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(CustomParameterizedRunner.class)
public class RetryExecutorTest {

    enum RetryPolicyType {
        FIXED,
        EXPONENTIAL_BACKOFF;
    }

    private static RetryPolicyType retryPolicyType;

    @CustomParameterizedRunner.Parameters
    public static Iterable<RetryPolicyType> profiles() {
        return Arrays.asList(RetryPolicyType.FIXED, RetryPolicyType.EXPONENTIAL_BACKOFF);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(RetryPolicyType retryPolicyTypeFromParameter) throws Exception {
        retryPolicyType = retryPolicyTypeFromParameter;
    }

    public RetryExecutorTest(RetryPolicyType retryPolicyType) {
    }

    @Test
    public void testForMaxIteration() {
        AtomicInteger iteration = new AtomicInteger(0);

        createRetryExecutor(100, 3, 100000).execute((() -> {
            iteration.incrementAndGet();
            if (iteration.get() < 2) {
                throw new RuntimeException();
            } else {
                return null;
            }
        }));

        Assert.assertEquals(2, iteration.get());
    }

    @Test
    public void testForMaxTime() {
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong stopTime = new AtomicLong(0);

        AtomicInteger iteration = new AtomicInteger(0);

        createRetryExecutor(100, 10000, 2000).execute(() -> {
            stopTime.set(System.currentTimeMillis());
            if ((stopTime.get() - startTime.get()) < 1000) {
                iteration.incrementAndGet();
                throw new RuntimeException();
            } else {
                return null;
            }
        });

        Assert.assertTrue((stopTime.get() - startTime.get() < 2000) && iteration.get() > 1);
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionMaxIteration() {
        createRetryExecutor(100, 1, 60_000).execute(() -> {
            throw new RuntimeException();
        });
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionForMaxSleep() {
        createRetryExecutor(100, 1000, 300).execute(() -> {
            throw new RuntimeException();
        });
    }

    private RetryExecutor createRetryExecutor(long sleepMs, int iteration, long maxSleepMs) {
        BackoffPolicy backoffPolicy;
        Map<String, Object> props = new HashMap<>();
        switch (retryPolicyType) {
            case FIXED:
                props.put(BackoffPolicy.TIMEOUT_MS, maxSleepMs);
                props.put(BackoffPolicy.SLEEP_TIME_MS, sleepMs);
                props.put(BackoffPolicy.MAX_ATTEMPTS, iteration);
                backoffPolicy = new FixedTimeBackoffPolicy();
                break;
            case EXPONENTIAL_BACKOFF:
                props.put(BackoffPolicy.TIMEOUT_MS, maxSleepMs);
                props.put(BackoffPolicy.SLEEP_TIME_MS, sleepMs);
                props.put(BackoffPolicy.MAX_ATTEMPTS, iteration);
                backoffPolicy = new ExponentialBackoffPolicy();
                break;
            default:
                throw new RuntimeException("Invalid retry policy type : " + retryPolicyType);
        }

        backoffPolicy.init(props);

        RetryExecutor.Builder builder = new RetryExecutor.Builder();
        return builder.backoffPolicy(backoffPolicy).build();
    }
}
