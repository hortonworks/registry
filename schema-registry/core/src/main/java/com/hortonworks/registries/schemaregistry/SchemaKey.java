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

import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * This class contains the schema metadata. This key can be used to find any version
 * of a schema in the schema repository.
 */
public class SchemaKey implements Serializable {
    private final SchemaMetadataKey schemaMetadataKey;
    private final Integer version;

    public SchemaKey(SchemaMetadataKey schemaMetadataKey, Integer version) {
        Preconditions.checkNotNull(schemaMetadataKey, "schemaMetadataKey can not be null");
        Preconditions.checkNotNull(version, "version can not be null");
        this.schemaMetadataKey = schemaMetadataKey;
        this.version = version;
    }

    public SchemaMetadataKey getSchemaMetadataKey() {
        return schemaMetadataKey;
    }

    public Integer getVersion() { return version; }
}
