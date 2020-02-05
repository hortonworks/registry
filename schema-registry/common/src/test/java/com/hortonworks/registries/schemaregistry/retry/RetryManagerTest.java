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

import com.hortonworks.registries.schemaregistry.retry.exception.RetryableException;
import com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffRetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.FixedTimeRetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
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
public class RetryManagerTest {

    enum RetryPolicyType {
        FIXED,
        EXPONENTIAL_BACKOFF;
    }

    private RetryManager retryManager = new RetryManager();
    private static RetryPolicyType retryPolicyType;

    @CustomParameterizedRunner.Parameters
    public static Iterable<RetryPolicyType> profiles() {
        return Arrays.asList(RetryPolicyType.FIXED, RetryPolicyType.EXPONENTIAL_BACKOFF);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(RetryPolicyType retryPolicyTypeFromParameter) throws Exception {
        retryPolicyType = retryPolicyTypeFromParameter;
    }

    public RetryManagerTest(RetryPolicyType retryPolicyType) {
    }

    @Test
    public void testForMaxIteration() {
        AtomicInteger iteration = new AtomicInteger(0);

        retryManager.execute(getRetryContextBuilder(100, 3, 100000).build(() -> {
            iteration.incrementAndGet();
            if (iteration.get() < 2) {
                throw new RetryableException(new RuntimeException());
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

        retryManager.execute(getRetryContextBuilder(100, 10000, 2000).build(() -> {
            stopTime.set(System.currentTimeMillis());
            if ((stopTime.get() - startTime.get()) < 1000) {
                iteration.incrementAndGet();
                throw new RetryableException(new RuntimeException());
            } else {
                return null;
            }
        }));

        Assert.assertTrue((stopTime.get() - startTime.get() < 2000) && iteration.get() > 1);
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionMaxIteration() {
        retryManager.execute(getRetryContextBuilder(100, 1, 60_000).build(() -> {
            throw new RetryableException(new RuntimeException());
        }));
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionForMaxSleep() {
        retryManager.execute(getRetryContextBuilder(100, 1000, 300).build(() -> {
            throw new RetryableException(new RuntimeException());
        }));
    }

    private RetryContext.Builder<Void> getRetryContextBuilder(long baseSleepMs, int iteration, long maxSleepMs) {
        RetryPolicy retryPolicy;
        Map<String, Object> props = new HashMap<>();
        switch (retryPolicyType) {
            case FIXED:
                props.put(RetryPolicy.MAX_SLEEP_TIME_MS, maxSleepMs);
                props.put(RetryPolicy.SLEEP_TIME_MS, baseSleepMs);
                props.put(RetryPolicy.MAX_RETRIES, iteration);
                retryPolicy = new FixedTimeRetryPolicy();
                break;
            case EXPONENTIAL_BACKOFF:
                props.put(RetryPolicy.MAX_SLEEP_TIME_MS, maxSleepMs);
                props.put(RetryPolicy.SLEEP_TIME_MS, baseSleepMs);
                props.put(ExponentialBackoffRetryPolicy.EXPONENT, 2F);
                props.put(RetryPolicy.MAX_RETRIES, iteration);
                retryPolicy = new ExponentialBackoffRetryPolicy();
                break;
            default:
                throw new RuntimeException("Invalid retry policy type : " + retryPolicyType);
        }

        retryPolicy.init(props);

        RetryContext.Builder<Void> builder = new RetryContext.Builder<Void>();
        return builder.policy(retryPolicy);
    }
}
