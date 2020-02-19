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

import com.hortonworks.registries.schemaregistry.retry.policy.BackoffPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.NOOPBackoffPolicy;
import com.hortonworks.registries.schemaregistry.retry.block.RetryableBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 *   RetryExecutor encapsulates the necessary context to attempt retry on an instance of
 *   {@link RetryableBlock}. This context includes {@link RetryExecutor#backoffPolicy} which can be one of
 *   {@link com.hortonworks.registries.schemaregistry.retry.policy.ExponentialBackoffPolicy},
 *   {@link com.hortonworks.registries.schemaregistry.retry.policy.FixedTimeBackoffPolicy} or
 *   {@link com.hortonworks.registries.schemaregistry.retry.policy.NOOPBackoffPolicy} which tells
 *   RetryExecutor how retries should be attempted and these retries are attempted when an instance
 *   of {@link RetryExecutor#exceptionClass} is thrown from {@link RetryableBlock}
 */

public class RetryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(RetryExecutor.class);

    private BackoffPolicy backoffPolicy;

    private Class<? extends RuntimeException> exceptionClass;

    private RetryExecutor(BackoffPolicy backoffPolicy, Class<? extends RuntimeException> exceptionClass) {
        this.backoffPolicy = backoffPolicy;
        this.exceptionClass = exceptionClass;
    }

    public <T> T execute(RetryableBlock<T> retryableBlock) {
        RuntimeException exception = null;

        int attemptNumber = 1;
        long startTime = System.currentTimeMillis();

        do {
            try {
                LOG.debug("Executing the retryable block with attempt number : {} and elapsed time : {} ms",
                        attemptNumber, (System.currentTimeMillis() - startTime));
                return retryableBlock.run();
            } catch (Exception e) {
                if (!(e.getClass().equals(exceptionClass))) {
                    throw e;
                } else {
                    exception = (RuntimeException) e;
                }
            }
        } while (backoffPolicy.mayBeSleep(attemptNumber++, System.currentTimeMillis() - startTime));

        LOG.debug("Giving up on executing the retryable block after attempt number : {} and elapsed time : {} ms",
                attemptNumber, (System.currentTimeMillis() - startTime));

        throw exception;
    }

    public static class Builder {

        private BackoffPolicy policy = new NOOPBackoffPolicy();

        private Class<? extends RuntimeException> exceptionClass = RuntimeException.class;

        public Builder backoffPolicy(BackoffPolicy policy) {
            this.policy = policy;
            return this;
        }

        public Builder retryOnException(Class<? extends RuntimeException> e) {
            this.exceptionClass = e;
            return this;
        }

        public RetryExecutor build() {
            return new RetryExecutor(policy, exceptionClass);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetryExecutor that = (RetryExecutor) o;
        return Objects.equals(backoffPolicy, that.backoffPolicy) &&
                Objects.equals(exceptionClass, that.exceptionClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backoffPolicy, exceptionClass);
    }

    @Override
    public String toString() {
        return "RetryExecutor{" +
                "backoffPolicy=" + backoffPolicy +
                ", exceptionClass=" + exceptionClass +
                '}';
    }
}
