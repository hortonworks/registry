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
package com.hortonworks.registries.schemaregistry.serdes.avro.exceptions;

import com.hortonworks.registries.schemaregistry.serde.SerDesException;

/**
 * This is thrown when an exception is caused due to Avro to binary (or vice versa) conversion.
 * This should be the default, unless the caught exception is retryable,
 * where {@link com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException}
 * should be thrown instead.
 */
public class AvroException extends SerDesException {

   public AvroException() {
   }

   public AvroException(String message) {
      super(message);
   }

   public AvroException(String message, Throwable cause) {
      super(message, cause);
   }

   public AvroException(Throwable cause) {
      super(cause);
   }

   public AvroException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
   
}
