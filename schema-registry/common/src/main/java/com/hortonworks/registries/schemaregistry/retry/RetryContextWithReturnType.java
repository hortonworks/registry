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

import com.hortonworks.registries.schemaregistry.retry.policy.RetryPolicy;
import com.hortonworks.registries.schemaregistry.retry.request.RequestWithReturnType;

import java.util.Objects;

public class RetryContextWithReturnType<T> {

    private RetryPolicy policy;

    private RequestWithReturnType<T> request;

    public static class Builder<T> {

        private RetryContextWithReturnType<T> retryContextWithReturnType = new RetryContextWithReturnType<T>();

        public Builder<T> policy(RetryPolicy policy) {
            retryContextWithReturnType.policy = policy;
            return this;
        }

        public RetryContextWithReturnType<T> build(RequestWithReturnType<T> request) {
            retryContextWithReturnType.request = request;
            return retryContextWithReturnType;
        }
    }

    public RetryPolicy policy() {
        return policy;
    }

    public RequestWithReturnType<T> request() {
        return request;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetryContextWithReturnType<?> that = (RetryContextWithReturnType<?>) o;
        return Objects.equals(policy, that.policy) &&
                Objects.equals(request, that.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, request);
    }

    @Override
    public String toString() {
        return "RetryContextWithReturnType{" +
                "policy=" + policy +
                ", request=" + request +
                '}';
    }
}
