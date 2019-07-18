/*
 * Copyright 2017 Hortonworks.
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
package com.hortonworks.registries.schemaregistry.exceptions;

import com.hortonworks.registries.schemaregistry.serde.RetryableException;

/**
 * This is thrown when an exception is caused due to the Schema Registry, 
 * and the exception is deemed retry-able, e.g. cause being IOException.
 */
public class RegistryRetryableException extends RegistryException implements RetryableException {

   public RegistryRetryableException() {
   }

   public RegistryRetryableException(String message) {
      super(message);
   }

   public RegistryRetryableException(String message, Throwable cause) {
      super(message, cause);
   }

   public RegistryRetryableException(Throwable cause) {
      super(cause);
   }

   public RegistryRetryableException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
}
