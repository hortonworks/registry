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
 * This class contains the schema metadata. This key can be used to find a given version
 * of a schema in the schema repository.
 */
public class SchemaVersionKey implements Serializable {
    private SchemaKey schemaKey;
    private Integer version;

    /** Private contructor for Jackson JSON mapping */
    @SuppressWarnings("unused")
    private SchemaVersionKey() { }

    public SchemaVersionKey(SchemaKey schemaKey, Integer version) {
        Preconditions.checkNotNull(schemaKey, "schemaMetadataKey can not be null");
        Preconditions.checkNotNull(version, "version can not be null");
        this.schemaKey = schemaKey;
        this.version = version;
    }

    public SchemaKey getSchemaKey() {
        return schemaKey;
    }

    public Integer getVersion() { return version; }
}
