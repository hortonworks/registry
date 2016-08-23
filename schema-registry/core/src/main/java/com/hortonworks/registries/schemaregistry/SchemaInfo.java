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

    /**
     * description of this schema instance
     */
    private String description;

    /**
     * version of the schema which is given in SchemaInfo
     */
    private Integer version;

    /**
     * textual representation of the schema which is given in SchemaInfo
     */
    private String schemaText;

    /**
     * timestamp of the schema which is given in SchemaInfo
     */
    private Long timestamp;

    /**
     * Compatibility of the schema for a given version which is given in SchemaInfo
     */
    private SchemaProvider.Compatibility compatibility;

    private SchemaInfo() {
    }

    public SchemaInfo(SchemaMetadataStorable schemaMetadataStorable, SchemaInfoStorable schemaInfoStorable) {
        description = schemaInfoStorable.getDescription();
        version = schemaInfoStorable.getVersion();
        schemaText = schemaInfoStorable.getSchemaText();
        timestamp = schemaInfoStorable.getTimestamp();
        compatibility = schemaMetadataStorable.getCompatibility();
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaInfo that = (SchemaInfo) o;

        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (schemaText != null ? !schemaText.equals(that.schemaText) : that.schemaText != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        return compatibility == that.compatibility;

    }

    @Override
    public int hashCode() {
        int result = (description != null ? description.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaInfo{" +
                ", description='" + description + '\'' +
                ", version=" + version +
                ", schemaText='" + schemaText + '\'' +
                ", timestamp=" + timestamp +
                ", compatibility=" + compatibility +
                '}';
    }
}
