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

import com.hortonworks.registries.schemaregistry.retry.policy.NOOPRetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.request.Request;

import java.util.Objects;

public class RetryContext<T> {

    private RetryPolicy policy = new NOOPRetryPolicy();

    private Request<T> request;

    private Class<? extends RuntimeException> exceptionClass = RuntimeException.class;

    public static class Builder<T> {

        private RetryContext<T> retryContext = new RetryContext<T>();

        public Builder<T> policy(RetryPolicy policy) {
            retryContext.policy = policy;
            return this;
        }

        public Builder<T> retryOnException(Class<? extends RuntimeException> e) {
            retryContext.exceptionClass = e;
            return this;
        }

        public RetryContext<T> build(Request<T> request) {
            retryContext.request = request;
            return retryContext;
        }
    }

    public Class<? extends RuntimeException> retryOnException() {
        return exceptionClass;
    }

    public RetryPolicy policy() {
        return policy;
    }

    public Request<T> request() {
        return request;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetryContext<?> that = (RetryContext<?>) o;
        return Objects.equals(policy, that.policy) &&
                Objects.equals(request, that.request) &&
                Objects.equals(exceptionClass, that.exceptionClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, request, exceptionClass);
    }

    @Override
    public String toString() {
        return "RetryContext{" +
                "policy=" + policy +
                ", request=" + request +
                ", exceptionClass=" + exceptionClass +
                '}';
    }
}
