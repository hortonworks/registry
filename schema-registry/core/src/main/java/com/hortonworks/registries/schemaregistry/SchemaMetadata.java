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

    private Long id;

    /**
     * Description about the schema metadata.
     */
    private String description;

    /**
     * Compatibility to be supported for all versions of this evolving schema.
     */
    private SchemaProvider.Compatibility compatibility = SchemaProvider.DEFAULT_COMPATIBILITY;

    private Long timestamp;

    private SchemaMetadata() {
    }

    public SchemaMetadata(SchemaMetadataKey schemaMetadataKey,
                          String description,
                          SchemaProvider.Compatibility compatibility) {
        this(schemaMetadataKey, null, description, null, compatibility);
    }
    public SchemaMetadata(SchemaMetadataKey schemaMetadataKey,
                          Long id,
                          String description,
                          Long timestamp,
                          SchemaProvider.Compatibility compatibility) {
        this.schemaMetadataKey = schemaMetadataKey;
        this.id = id;
        this.timestamp = timestamp;
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

    public Long getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SchemaMetadataStorable toSchemaMetadataStorable() {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setId(id);
        schemaMetadataStorable.setType(schemaMetadataKey.getType());
        schemaMetadataStorable.setGroup(schemaMetadataKey.getGroup());
        schemaMetadataStorable.setName(schemaMetadataKey.getName());
        schemaMetadataStorable.setDescription(description);
        schemaMetadataStorable.setCompatibility(compatibility);
        schemaMetadataStorable.setTimestamp(timestamp);

        return schemaMetadataStorable;
    }

    public static SchemaMetadata fromSchemaMetadataStorable(SchemaMetadataStorable schemaMetadataStorable) {
        SchemaMetadataKey schemaMetadataKey =
                new SchemaMetadataKey(schemaMetadataStorable.getType(),
                        schemaMetadataStorable.getGroup(),
                        schemaMetadataStorable.getName());

        return  new SchemaMetadata(schemaMetadataKey,
                                    schemaMetadataStorable.getId(),
                                    schemaMetadataStorable.getDescription(),
                                    schemaMetadataStorable.getTimestamp(),
                                    schemaMetadataStorable.getCompatibility());
    }

    public SchemaMetadataKey getSchemaMetadataKey() {
        return schemaMetadataKey;
    }

}
