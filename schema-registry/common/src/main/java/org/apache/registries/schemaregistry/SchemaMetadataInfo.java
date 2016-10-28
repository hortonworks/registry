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

import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * This class encapsulates metadata about a schema which includes group, name, type, description and compatibility.
 * There can be only one instance with the same name.
 * New versions of the schema can be registered for the given {@link SchemaMetadataInfo} by giving {@link SchemaVersion} instances.
 */
public final class SchemaMetadataInfo implements Serializable {

    /**
     * metadata about the schema
     */
    private SchemaMetadata schemaMetadata;

    private Long id;

    private Long timestamp;

    @SuppressWarnings("unused")
    private SchemaMetadataInfo() { /* Private constructor for Jackson JSON mapping */}

    /**
     * @param schemaMetadata
     */
    public SchemaMetadataInfo(SchemaMetadata schemaMetadata) {
        this(schemaMetadata, null, null);
    }

    SchemaMetadataInfo(SchemaMetadata schemaMetadata,
                       Long id,
                       Long timestamp) {
        Preconditions.checkNotNull(schemaMetadata, "schemaMetadata can not be null");
        this.schemaMetadata = schemaMetadata;
        this.id = id;
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }

}
