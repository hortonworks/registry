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

package com.hortonworks.registries.schemaregistry.retry.block;

/**
 * This class encapsulates the block of code that needs to be executed with RetryExecutor
 * with retries
 *
 * @param <T> Return type of the request
 */

@FunctionalInterface
public interface RetryableBlock<T, E1 extends Exception, E2 extends Exception> {
    T run() throws E1, E2;
}