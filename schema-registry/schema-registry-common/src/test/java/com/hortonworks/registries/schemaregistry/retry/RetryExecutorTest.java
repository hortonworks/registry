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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class RetryExecutorTest {

    enum RetryPolicyType {
        FIXED,
        EXPONENTIAL_BACKOFF;
    }

    private static RetryPolicyType retryPolicyType;

    public static Stream<RetryPolicyType> profiles() {
        return Stream.of(RetryPolicyType.FIXED, RetryPolicyType.EXPONENTIAL_BACKOFF);
    }

    public void beforeParam(RetryPolicyType retryPolicyTypeFromParameter) throws Exception {
        retryPolicyType = retryPolicyTypeFromParameter;
    }
    
    @ParameterizedTest
    @MethodSource("profiles")
    public void testRetries(RetryPolicyType profile) throws Exception {
        beforeParam(profile);
        AtomicInteger attempt = new AtomicInteger(1);

        createRetryExecutor(100, 3, 100000).execute((() -> {
            attempt.incrementAndGet();
            if (attempt.get() < 2) {
                throw new RuntimeException();
            } else {
                return null;
            }
        }));

        Assertions.assertEquals(2, attempt.get());
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void testExceptionOnExceedingMaxAttempts(RetryPolicyType profile) throws Exception {
        beforeParam(profile);
        Assertions.assertThrows(RuntimeException.class, () -> createRetryExecutor(100, 1, 60_000).execute(() -> {
            throw new RuntimeException();
        }));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void testExceptionOnExceedingMaxTimeout(RetryPolicyType profile) throws Exception {
        beforeParam(profile);
        Assertions.assertThrows(RuntimeException.class, () -> createRetryExecutor(100, 1000, 300).execute(() -> {
            throw new RuntimeException();
        }));
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
