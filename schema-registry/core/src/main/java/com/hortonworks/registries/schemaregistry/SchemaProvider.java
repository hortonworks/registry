/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry;

import java.util.Collection;
import java.util.List;

/**
 * Different types of Schema providers such as AVRO, Protobuf etc.
 */
public interface SchemaProvider {

    /**
     * @return type of this provider. This should be unique among all the registered providers.
     */
    String getType();

    boolean isCompatible(String toSchema, String existingSchema, SchemaCompatibility compatibility);

    boolean isCompatible(String toSchemaText, Collection<String> existingSchemaTexts, SchemaCompatibility existingSchemaCompatibility);

    /**
     * @param schemaText textual representation of schema
     * @return fingerprint of canonicalized form of the given schema.
     */
    byte[] getFingerprint(String schemaText) throws InvalidSchemaException;

    /**
     * TODO why is this called rootSchema? Is this same as schemaText above?
     * @param rootSchema
     * @return all the fields in the given schema
     */
    List<SchemaFieldInfo> generateFields(String rootSchema);
}
