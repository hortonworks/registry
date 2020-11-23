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
import com.hortonworks.registries.schemaregistry.util.CustomParameterizedRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testRetries() {
        AtomicInteger attempt = new AtomicInteger(1);

        createRetryExecutor(100, 3, 100000).execute((() -> {
            attempt.incrementAndGet();
            if (attempt.get() < 2) {
                throw new RuntimeException();
            } else {
                return null;
            }
        }));

        Assert.assertEquals(2, attempt.get());
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionOnExceedingMaxAttempts() {
        createRetryExecutor(100, 1, 60_000).execute(() -> {
            throw new RuntimeException();
        });
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionOnExceedingMaxTimeout() {
        createRetryExecutor(100, 1000, 300).execute(() -> {
            throw new RuntimeException();
        });
    }

    private RetryExecutor createRetryExecutor(long sleepTimeMs, int maxAttempts, long timeoutMs) {
        BackoffPolicy backoffPolicy;
        Map<String, Object> props = new HashMap<>();
        props.put(BackoffPolicy.TIMEOUT_MS, timeoutMs);
        props.put(BackoffPolicy.SLEEP_TIME_MS, sleepTimeMs);
        props.put(BackoffPolicy.MAX_ATTEMPTS, maxAttempts);
        switch (retryPolicyType) {
            case FIXED:
                backoffPolicy = new FixedTimeBackoffPolicy();
                break;
            case EXPONENTIAL_BACKOFF:
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
