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

public class NOOPRetryPolicy extends RetryPolicy {

    public NOOPRetryPolicy() {
        super(0L,0,0L);
    }

    @Override
    public void init(Map<String, Object> properties) {

    }

    @Override
    long sleepTime(int iteration, long timeElapsed) {
        return 0;
    }
}
