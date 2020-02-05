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

package com.hortonworks.registries.schemaregistry.retry;

import com.hortonworks.registries.schemaregistry.retry.exception.RetryableException;
import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.request.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryManager {

    private static final Logger LOG = LoggerFactory.getLogger(RetryManager.class);

    public <T> T execute(RetryContext<T> retryContext) {

        Request<T> request = retryContext.request();
        RetryPolicy policy = retryContext.policy();
        RetryableException retryableException = null;

        System.out.println("Policy : "+policy);

        int iteration = 0;
        long startTime = System.currentTimeMillis();

        do {
            try {
                return request.run();
            } catch (Exception e) {
                if (!(e instanceof RetryableException)) {
                    throw e;
                } else {
                    retryableException = (RetryableException) e;
                }
            }
        } while (policy.mayBeSleep(++iteration, System.currentTimeMillis() - startTime));

        LOG.debug("Reached the limit of retries for the request after iteration : " +
                iteration + " and elapsed time : " + (System.currentTimeMillis() - startTime));

        throw retryableException.getUnderlyingException();
    }
}
