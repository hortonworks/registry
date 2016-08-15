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

import java.io.Serializable;

/**
 *
 */
public final class SchemaInfo implements Serializable {

    // schema metadata id
    private Long id;

    // name for schema which is part of schema metadata
    private String name;

    // type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc
    private String type;

    // description of the schema which is given in schema metadata
    private String description;

    // version of the schema which is given in SchemaInfo
    private Integer version;

    // textual representation of the schema which is given in SchemaInfo
    private String schemaText;

    // timestamp of the schema which is given in SchemaInfo
    private Long timestamp;

    // Compatibility of the schema for a given version which is given in SchemaInfo
    private SchemaProvider.Compatibility compatibility;

    private SchemaInfo() {
    }

    public SchemaInfo(Long id, String name, String type, String description, Integer version, String schemaText,
                      Long timestamp, SchemaProvider.Compatibility compatibility) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.description = description;
        this.version = version;
        this.schemaText = schemaText;
        this.timestamp = timestamp;
        this.compatibility = compatibility;
    }

    public SchemaInfo(SchemaMetadataStorable schemaMetadataStorable, SchemaInfoStorable schemaInfoStorable) {
        id = schemaMetadataStorable.getId();
        name = schemaMetadataStorable.getName();
        type = schemaMetadataStorable.getType();
        description = schemaMetadataStorable.getDescription();
        version = schemaInfoStorable.getVersion();
        schemaText = schemaInfoStorable.getSchemaText();
        timestamp = schemaInfoStorable.getTimestamp();
        compatibility = schemaMetadataStorable.getCompatibility();
    }

    public Long getId() {
        return id;
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

    public Integer getVersion() {
        return version;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }
    
    @Override
    public String toString() {
        return "SchemaDto{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", description='" + description + '\'' +
                ", version=" + version +
                ", schemaText='" + schemaText + '\'' +
                ", timestamp=" + timestamp +
                ", compatibility=" + compatibility +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaInfo schemaInfo = (SchemaInfo) o;

        if (id != null ? !id.equals(schemaInfo.id) : schemaInfo.id != null) return false;
        if (name != null ? !name.equals(schemaInfo.name) : schemaInfo.name != null) return false;
        if (type != null ? !type.equals(schemaInfo.type) : schemaInfo.type != null) return false;
        if (description != null ? !description.equals(schemaInfo.description) : schemaInfo.description != null)
            return false;
        if (version != null ? !version.equals(schemaInfo.version) : schemaInfo.version != null) return false;
        if (schemaText != null ? !schemaText.equals(schemaInfo.schemaText) : schemaInfo.schemaText != null) return false;
        if (timestamp != null ? !timestamp.equals(schemaInfo.timestamp) : schemaInfo.timestamp != null) return false;
        return compatibility == schemaInfo.compatibility;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        return result;
    }
}
