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

import com.hortonworks.registries.schemaregistry.retry.exception.RetriableException;
import com.hortonworks.registries.schemaregistry.retry.exception.RetryManagerException;
import com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffRetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ExponentialBackoffRetryPolicyTest {

    private RetryManager retryManager = new RetryManager();

    @Test
    public void testForMaxIteration() {
        AtomicInteger iteration = new AtomicInteger(0);

        retryManager.execute(getRetryContextBuilder(100, 3, 100000).build(() -> {
            iteration.incrementAndGet();
            if (iteration.get() < 2) {
                throw new RetriableException("");
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

        retryManager.execute(getRetryContextBuilder(100, 10000, 60000).build(() -> {
            stopTime.set(System.currentTimeMillis());
            if (stopTime.get() - startTime.get() < 2000) {
                iteration.incrementAndGet();
                System.out.println("Time elapsed : " + (stopTime.get() - startTime.get()));
                throw new RetriableException("");
            } else {
                return null;
            }
        }));

        Assert.assertTrue((stopTime.get() - startTime.get() < 2000) && iteration.get() > 1);
    }

    @Test(expected = RetryManagerException.class)
    public void testExceptionMaxIteration() {
        retryManager.execute(getRetryContextBuilder(100, 1, 60_000).build(() -> {
            throw new RetriableException("");
        }));
    }

    @Test(expected = RetryManagerException.class)
    public void testExceptionForMaxSleep() {
        retryManager.execute(getRetryContextBuilder(100, 1000, 300).build(() -> {
            throw new RetriableException("");
        }));
    }

    private RetryContext.Builder<Void> getRetryContextBuilder(long baseSleepMs, int iteration, long maxSleepMs) {
        Map<String, Object> props = new HashMap<>();
        props.put(ExponentialBackoffRetryPolicy.MAX_SLEEP_TIME_MS, maxSleepMs);
        props.put(ExponentialBackoffRetryPolicy.BASE_SLEEP_TIME_MS, baseSleepMs);
        props.put(ExponentialBackoffRetryPolicy.MULTIPLIER, 2F);
        props.put(ExponentialBackoffRetryPolicy.MAX_RETRIES, iteration);

        RetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy();
        retryPolicy.init(props);

        RetryContext.Builder<Void> builder = new RetryContext.Builder<Void>();
        return builder.policy(retryPolicy);
    }
}
