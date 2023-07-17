/**
 * Copyright 2016-2023 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.schemaregistry;

/**
 * Describes how to perform schema version deduplication when adding a new schema version.
 */
public enum SchemaDeduplicationMode {
  /**
   * Effective equality check on schemas, based on schema fingerprint.
   * I.e. schema versions having the same structure (regardless of formatting or non-functional differences)
   * will be deduplicated.
   */
  CANONICAL_CHECK,
  /**
   * Additionally to CANONICAL_CHECK (fingerprint), also check full schema text equality.
   * I.e. schema versions with the exact same schema text will be deduplicated.
   */
  TEXT_CHECK,
  /**
   * No extra checks. I.e. schema versions will not be deduplicated.
   */
  NO_CHECK
}
