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

/**
 * NOOPBackoffPolicy essentially works as a no retry policy. This is used when
 * no retry is expected instead retry should be handled at the application level.
 */

public class NOOPBackoffPolicy extends BackoffPolicy {

    public NOOPBackoffPolicy() {
        super(0L,1,0L);
    }

    @Override
    public void init(Map<String, Object> properties) {

    }

    @Override
    long sleepTime(int attemptNumber, long timeElapsed) {
        return 0;
    }

    @Override
    public String toString() {
        return "NOOPBackoffPolicy{" +
                "sleepTimeMs=" + sleepTimeMs +
                ", maxAttempts=" + maxAttempts +
                ", timeoutMs=" + timeoutMs +
                '}';
    }
}
