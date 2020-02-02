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

import com.hortonworks.registries.schemaregistry.retry.exception.RetriableException;
import com.hortonworks.registries.schemaregistry.retry.exception.RetryManagerException;
import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.request.RequestWithReturnType;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class RetryManager {

    private ExecutorService executorService;

    public RetryManager() {

    }

    public RetryManager(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public <T> T execute(RetryContextWithReturnType<T> retryContextWithReturnType) {

        if (executorService != null) {
            try {
                Future<?> ret = executorService.submit(() -> {
                    executable(retryContextWithReturnType);
                });
                return (T) ret.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RetryManagerException(e);
            }
        } else {
            return executable(retryContextWithReturnType);
        }
    }

    private <T> T executable(RetryContextWithReturnType<T> retryContextWithReturnType) {
        RequestWithReturnType<T> requestWithReturnType = retryContextWithReturnType.request();
        RetryPolicy policy = retryContextWithReturnType.policy();

        int iteration = 0;
        long startTime = System.currentTimeMillis();

        do {
            try {
                return requestWithReturnType.run();
            } catch (Exception e) {
                if (!(e instanceof RetriableException)) {
                    throw e;
                }
            }
        } while (policy.mayBeSleep(++iteration, System.currentTimeMillis() - startTime));

        throw new RetryManagerException("Reached the limit of retries for the requestWithReturnType after iteration : " +
                iteration + " and elapsed time : " + (System.currentTimeMillis() - startTime));
    }
}
