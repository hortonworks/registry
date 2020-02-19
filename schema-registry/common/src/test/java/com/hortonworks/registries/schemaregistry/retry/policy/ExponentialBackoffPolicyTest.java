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

package com.hortonworks.registries.schemaregistry.retry.policy;

import org.junit.Assert;
import org.junit.Test;

public class ExponentialBackoffPolicyTest {

    @Test
    public void testSleepInterval() {
        long sleepMs = 100L;
        ExponentialBackoffPolicy exponentialBackoffRetryPolicy = new ExponentialBackoffPolicy(sleepMs, 10, 10000L);
        long[] sleepInterval = {100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 102400};
        for (int i = 1; i <= 10; i++) {
            Assert.assertEquals(sleepInterval[i - 1], exponentialBackoffRetryPolicy.sleepTime(i, 1000));
        }
    }
}
