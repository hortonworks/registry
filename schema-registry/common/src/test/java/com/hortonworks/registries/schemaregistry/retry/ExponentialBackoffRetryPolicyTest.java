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

import com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffRetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.FixedTimeRetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExponentialBackoffRetryPolicyTest {

    private RetryManager retryManager = new RetryManager();

    @Test
    public void

    private RetryContext.Builder<Void> getRetryContextBuilder(long baseSleepMs, float multiplier, int iteration, long maxSleepMs) {
        Map<String, Object> props = new HashMap<>();
        props.put(ExponentialBackoffRetryPolicy.MAX_RETRIES, maxSleepMs);
        props.put(ExponentialBackoffRetryPolicy.BASE_SLEEP_TIME_MS, baseSleepMs);
        props.put(ExponentialBackoffRetryPolicy.MAX_RETRIES, iteration);
        props.put(ExponentialBackoffRetryPolicy.MULTIPLIER, multiplier);

        RetryPolicy retryPolicy = new ExponentialBackoffRetryPolicy();
        retryPolicy.init(props);

        RetryContext.Builder<Void> builder = new RetryContext.Builder<Void>();
        return builder.policy(retryPolicy);
    }

}
