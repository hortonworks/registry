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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaVersionStorable extends AbstractStorable {
    public static final String NAME_SPACE = "schema_version_info";
    public static final String ID = "id";
    public static final String SCHEMA_METADATA_ID = "schemaMetadataId";
    public static final String DESCRIPTION = "description";
    public static final String NAME = "name";
    public static final String SCHEMA_TEXT = "schemaText";
    public static final String VERSION = "version";
    public static final String TIMESTAMP = "timestamp";
    public static final String FINGERPRINT = "fingerprint";

    /**
     * Unique ID generated for this component.
     */
    private Long id;

    /**
     * Id of the {@link SchemaMetadataStorable} instance.
     */
    private Long schemaMetadataId;

    /**
     * unique name of the schema from {@link SchemaMetadataStorable} instance.
     */
    private String name;

    /**
     * Description about this schema instance
     */
    private String description;

    /**
     * Textual representation of the schema
     */
    private String schemaText;

    /**
     * Current version of the schema. (id, version) pair is unique constraint.
     */
    private Integer version;

    /**
     * Time at which this schema was created/updated.
     */
    private Long timestamp;

    /**
     * Fingerprint of the schema.
     */
    private String fingerprint;

    public SchemaVersionStorable() {
    }

    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(new Schema.Field(ID, Schema.Type.LONG), id);
        return new PrimaryKey(values);
    }

    @Override
    @JsonIgnore
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(ID, Schema.Type.LONG),
                Schema.Field.of(SCHEMA_METADATA_ID, Schema.Type.LONG),
                Schema.Field.of(SCHEMA_TEXT, Schema.Type.STRING),
                Schema.Field.of(NAME, Schema.Type.STRING),
                Schema.Field.optional(DESCRIPTION, Schema.Type.STRING),
                Schema.Field.of(VERSION, Schema.Type.INTEGER),
                Schema.Field.of(TIMESTAMP, Schema.Type.LONG),
                Schema.Field.of(FINGERPRINT, Schema.Type.STRING)
        );
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getSchemaMetadataId() {
        return schemaMetadataId;
    }

    public void setSchemaMetadataId(Long schemaMetadataId) {
        this.schemaMetadataId = schemaMetadataId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SchemaVersionInfo toSchemaVersionInfo() {
        return new SchemaVersionInfo(version, schemaText, timestamp, description);
    }

    @Override
    public String toString() {
        return "SchemaVersionStorable{" +
                "id=" + id +
                ", schemaMetadataId=" + schemaMetadataId +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", schemaText='" + schemaText + '\'' +
                ", version=" + version +
                ", timestamp=" + timestamp +
                ", fingerprint='" + fingerprint + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionStorable that = (SchemaVersionStorable) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (schemaMetadataId != null ? !schemaMetadataId.equals(that.schemaMetadataId) : that.schemaMetadataId != null)
            return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (schemaText != null ? !schemaText.equals(that.schemaText) : that.schemaText != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        return fingerprint != null ? fingerprint.equals(that.fingerprint) : that.fingerprint == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (schemaMetadataId != null ? schemaMetadataId.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (fingerprint != null ? fingerprint.hashCode() : 0);
        return result;
    }
}
