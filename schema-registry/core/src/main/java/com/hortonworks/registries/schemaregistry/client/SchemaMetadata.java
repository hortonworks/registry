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
import com.hortonworks.registries.schemaregistry.SchemaInfoStorable;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaProvider;

import java.io.Serializable;

/**
 * This class encapsulates information of an evolving schema including group, name, type, description and compatibility.
 *
 * There can be only one instance with same (name, group, type) fields.
 *
 * This can be used to register a schema when client does not know about the existing registered schema information.
 */
public final class SchemaMetadata implements Serializable {

    private static final SchemaProvider.Compatibility DEFAULT_COMPATIBILITY = SchemaProvider.Compatibility.BACKWARD;

    /**
     * name of this schema.
     * Follows unique constraint of (name, group, type).
     */
    private String name;

    /**
     * Data source group to which this schema belongs to. For ex: kafka, hive etc
     * This can be used in querying schemas belonging to a specific data source group.
     */
    private String group;

    /**
     * type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc
     */
    private String type;

    /**
     * Description about the schema.
     */
    private String description;

    /**
     * Textual representation of the schema for the initial version which is passed along with this SchemaMetadata.
     */
    private String schemaText;

    /**
     * Compatibility to be supported for all versions of this evolving schema.
     */
    private SchemaProvider.Compatibility compatibility;

    private SchemaMetadata() {
    }

    protected SchemaMetadata(String group, String name, String type, String description, SchemaProvider.Compatibility compatibility, String schemaText) {
        Preconditions.checkNotNull(group, "group can not be null");
        Preconditions.checkNotNull(name, "namespace can not be null");
        Preconditions.checkNotNull(type, "type can not be null");
        Preconditions.checkNotNull(compatibility, "compatibility can not be null");

        this.group = group;
        this.name = name;
        this.type = type;
        this.description = description;
        this.compatibility = compatibility;
        this.schemaText = schemaText;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }

    public SchemaMetadataStorable schemaMetadataStorable() {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setGroup(group);
        schemaMetadataStorable.setName(name);
        schemaMetadataStorable.setType(type);
        schemaMetadataStorable.setDescription(description);

        return schemaMetadataStorable;
    }

    public SchemaInfoStorable schemaInfoStorable() {
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable();
        schemaInfoStorable.setSchemaText(schemaText);

        return schemaInfoStorable;
    }

    public String getGroup() {
        return group;
    }

    public static final class Builder {

        private String group;

        private String namespace;

        private String type;

        private String description;

        private String schemaText;

        private SchemaProvider.Compatibility compatibility = DEFAULT_COMPATIBILITY;

        public Builder() {
        }

        public Builder(SchemaMetadata schemaMetadata) {
            group = schemaMetadata.getGroup();
            namespace = schemaMetadata.getName();
            type = schemaMetadata.getType();
            description = schemaMetadata.getDescription();
            schemaText = schemaMetadata.getSchemaText();
            compatibility = schemaMetadata.getCompatibility();
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder schemaText(String schemaText) {
            this.schemaText = schemaText;
            return this;
        }

        public Builder compatibility(SchemaProvider.Compatibility compatibility) {
            this.compatibility = compatibility;
            return this;
        }

        public SchemaMetadata build() {
            Preconditions.checkNotNull(group, "group can not be null");
            Preconditions.checkNotNull(namespace, "namespace can not be null");
            Preconditions.checkNotNull(type, "type can not be null");

            return new SchemaMetadata(group, namespace, type, description, compatibility, schemaText);
        }

    }

}
