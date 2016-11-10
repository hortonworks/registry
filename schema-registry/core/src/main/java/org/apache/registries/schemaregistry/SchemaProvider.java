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
package org.apache.registries.schemaregistry;

import org.apache.registries.schemaregistry.errors.InvalidSchemaException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Different types of Schema providers such as AVRO, Protobuf etc.
 */
public interface SchemaProvider {

    /**
     * Initializes with the given {@code config}.
     * @param config
     */
    public void init(Map<String, Object> config);

    /**
     * @return Name of this provider
     */
    String getName();

    /**
     * @return descirption about this provider.
     */
    String getDescription();

    /**
     * @return Fully qualified class name of the default serializer.
     */
    String getDefaultSerializerClassName();

    /**
     * @return Fully qualified class name of the default serializer.
     */
    String getDefaultDeserializerClassName();

    /**
     * @return type of this provider. This should be unique among all the registered providers.
     */
    String getType();

    /**
     * Returns true if the given {@code schemaText} is compatible with the given {@code existingSchema} according to {@code existingSchemaCompatibility}
     *
     * @param toSchema
     * @param existingSchema
     * @param compatibility
     */
    boolean isCompatible(String toSchema, String existingSchema, SchemaCompatibility compatibility);

    /**
     * Returns true if the given {@code schemaText} is compatible with all the given {@code existingSchemaTexts} according to {@code existingSchemaCompatibility}
     *
     * @param toSchemaText
     * @param existingSchemaTexts
     * @param existingSchemaCompatibility
     */
    boolean isCompatible(String toSchemaText, Collection<String> existingSchemaTexts, SchemaCompatibility existingSchemaCompatibility);

    /**
     * @param schemaText textual representation of schema
     * @return fingerprint of canonicalized form of the given schema.
     */
    byte[] getFingerprint(String schemaText) throws InvalidSchemaException;

    /**
     * Returns all the fields in the given {@code schemaText} by traversing the whole schema including nested/complex types.
     *
     * @param schemaText
     * @return all the fields in the given {@code schemaText}
     */
    List<SchemaFieldInfo> generateFields(String schemaText);
}
