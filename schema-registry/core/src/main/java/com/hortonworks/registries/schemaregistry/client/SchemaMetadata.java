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
 * This class encapsulates schema information including name, type, description, schemaText and compatibility. This can
 * be used when client does not know about the existing registered schema information.
 *
 */
public class SchemaMetadata implements Serializable {

    // name for schema which is part of schema metadata
    private String name;

    // type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc
    private String type;

    // description of the schema which is given in schema metadata
    private String description;

    // textual representation of the schema which is given in SchemaInfo
    private String schemaText;

    // Compatibility of the schema for a given version which is given in SchemaInfo
    private SchemaProvider.Compatibility compatibility;

    private SchemaMetadata() {
    }

    public SchemaMetadata(String name, String type, String description, SchemaProvider.Compatibility compatibility) {
        this(name, type, description, compatibility, null);
    }


    public SchemaMetadata(String name, String type, String description, SchemaProvider.Compatibility compatibility, String schemaText) {
        Preconditions.checkNotNull(name, "name can not be null");
        Preconditions.checkNotNull(type, "type can not be null");
        Preconditions.checkNotNull(description, "description can not be null");
        Preconditions.checkNotNull(compatibility, "compatinility can not be null");

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

}
