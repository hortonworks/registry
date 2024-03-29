/**
 * Copyright 2016-2022 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.storage.exception;

public class OffsetRangeReachedException extends StorageException {
  public OffsetRangeReachedException(Throwable cause) {
    super(cause);
  }

  public OffsetRangeReachedException(String message, Throwable cause) {
    super(message, cause);
  }

  public OffsetRangeReachedException(String message) {
    super(message);
  }
}
