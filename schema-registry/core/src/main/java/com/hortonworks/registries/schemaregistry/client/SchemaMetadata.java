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
package com.hortonworks.registries.schemaregistry.client;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaProvider;

import java.io.Serializable;

/**
 * This class encapsulates information of an evolving schema including group, name, type, description and compatibility.
 * <p>
 * There can be only one instance with same (type, name, group) fields.
 * <p>
 * This can be used to register a schema when client does not know about the existing registered schema information.
 */
public final class SchemaMetadata implements Serializable {

    /**
     * Unique key representation for this evolving schema.
     */
    private SchemaMetadataKey schemaMetadataKey;

    /**
     * Description about the schema metadata.
     */
    private String description;

    /**
     * Compatibility to be supported for all versions of this evolving schema.
     */
    private SchemaProvider.Compatibility compatibility = SchemaProvider.DEFAULT_COMPATIBILITY;

    private SchemaMetadata() {
    }

    public SchemaMetadata(SchemaMetadataKey schemaMetadataKey, String description, SchemaProvider.Compatibility compatibility) {
        this.schemaMetadataKey = schemaMetadataKey;
        Preconditions.checkNotNull(schemaMetadataKey, "schemaMetadataKey can not be null");

        this.compatibility = compatibility;
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }

    public SchemaMetadataStorable schemaMetadataStorable() {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setType(schemaMetadataKey.getType());
        schemaMetadataStorable.setGroup(schemaMetadataKey.getGroup());
        schemaMetadataStorable.setName(schemaMetadataKey.getName());
        schemaMetadataStorable.setDescription(description);
        schemaMetadataStorable.setCompatibility(compatibility);

        return schemaMetadataStorable;
    }

    public SchemaMetadataKey getSchemaMetadataKey() {
        return schemaMetadataKey;
    }

}
