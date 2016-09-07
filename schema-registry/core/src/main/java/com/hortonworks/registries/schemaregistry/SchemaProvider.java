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
 *
 */
public interface SchemaProvider {
    public SchemaProvider.Compatibility DEFAULT_COMPATIBILITY = SchemaProvider.Compatibility.BACKWARD;

    enum Compatibility {
        NONE,
        BACKWARD,
        FORWARD,
        BOTH
    }

    /**
     * Type of this provider. This should be unique among all the registered providers.
     *
     * @return
     */
    String getType();

    boolean isCompatible(String toSchema, String existingSchema, Compatibility compatibility);

    boolean isCompatible(String toSchemaText, Collection<String> existingSchemaTexts, Compatibility existingSchemaCompatibility);

    /**
     * Returns fingerprint of canonicalized form of the given schema.
     * @param schemaText
     * @return
     */
    byte[] getFingerprint(String schemaText);

    /**
     * Returns all the fields in the given schema
     * @param rootSchema
     * @return
     */
    List<SchemaFieldInfo> generateFields(String rootSchema);

}
