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
 * This class encapsulates information of different versions of a schema which includes group, name, type, description and compatibility.
 * There can be only one instance with same (type, name, group) fields.
 * New versions of the schema can be registered for the given {@link SchemaInfo} by giving {@link SchemaVersion} instances.
 */
public final class SchemaInfo implements Serializable {

    /**
     * Unique key to identify a schema
     */
    private final SchemaKey schemaKey;

    private final Long id;

    /**
     * Description about the schema metadata.
     */
    private final String description;

    /**
     * Compatibility to be supported for all versions of this evolving schema.
     */
    private final SchemaProvider.Compatibility compatibility;

    private final Long timestamp;

    public SchemaInfo(SchemaKey schemaKey,
                      String description,
                      SchemaProvider.Compatibility compatibility) {
        this(schemaKey, null, description, null, compatibility);
    }

    SchemaInfo(SchemaKey schemaKey,
               Long id,
               String description,
               Long timestamp,
               SchemaProvider.Compatibility compatibility) {
        this.schemaKey = schemaKey;
        this.id = id;
        this.timestamp = timestamp;
        Preconditions.checkNotNull(schemaKey, "schemaMetadataKey can not be null");

        this.compatibility = compatibility != null ? compatibility : SchemaProvider.DEFAULT_COMPATIBILITY;
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

    public SchemaInfoStorable toSchemaInfoStorable() {
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable();
        schemaInfoStorable.setId(id);
        schemaInfoStorable.setType(schemaKey.getType());
        schemaInfoStorable.setSchemaGroup(schemaKey.getSchemaGroup());
        schemaInfoStorable.setName(schemaKey.getName());
        schemaInfoStorable.setDescription(description);
        schemaInfoStorable.setCompatibility(compatibility);
        schemaInfoStorable.setTimestamp(timestamp);

        return schemaInfoStorable;
    }

    public static SchemaInfo fromSchemaInfoStorable(SchemaInfoStorable schemaInfoStorable) {
        SchemaKey schemaKey =
                new SchemaKey(schemaInfoStorable.getType(),
                        schemaInfoStorable.getSchemaGroup(),
                        schemaInfoStorable.getName());

        return new SchemaInfo(schemaKey,
                schemaInfoStorable.getId(),
                schemaInfoStorable.getDescription(),
                schemaInfoStorable.getTimestamp(),
                schemaInfoStorable.getCompatibility());
    }

    public SchemaKey getSchemaKey() {
        return schemaKey;
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "schemaMetadataKey=" + schemaKey +
                ", id=" + id +
                ", description='" + description + '\'' +
                ", compatibility=" + compatibility +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaInfo that = (SchemaInfo) o;

        if (schemaKey != null ? !schemaKey.equals(that.schemaKey) : that.schemaKey != null)
            return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (compatibility != that.compatibility) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;

    }

    @Override
    public int hashCode() {
        int result = schemaKey != null ? schemaKey.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
