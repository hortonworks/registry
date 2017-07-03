/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.AbstractVersionedStorable;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaVersionStorable extends AbstractVersionedStorable {
    
    public static final String NAME_SPACE = "schema_version_info";
    public static final String ID = "id";
    
    // Maintaining this for backward compatibility as users may nto want to recreate DB schemas with the changes.
    public static final String SCHEMA_METADATA_ID = "schemaMetadataId";

    public static final String NAME = "name";
    public static final String SCHEMA_TEXT = "schemaText";
    public static final String TIMESTAMP = "timestamp";
    public static final String FINGERPRINT = "fingerprint";
    public static final String DISABLED = "disabled";


    public static final Schema.Field ID_FIELD = Schema.Field.of(ID, Schema.Type.LONG);

    public static final Schema SCHEMA = Schema.of(
        ID_FIELD,
        Schema.Field.of(SCHEMA_METADATA_ID, Schema.Type.LONG),
        Schema.Field.of(SCHEMA_TEXT, Schema.Type.STRING),
        Schema.Field.of(NAME, Schema.Type.STRING),
        Schema.Field.optional(DESCRIPTION, Schema.Type.STRING),
        Schema.Field.of(VERSION, Schema.Type.INTEGER),
        Schema.Field.of(TIMESTAMP, Schema.Type.LONG),
        Schema.Field.of(FINGERPRINT, Schema.Type.STRING),
        Schema.Field.of(DISABLED, Schema.Type.BOOLEAN)
    );
    

    /**
     * Unique ID generated for this component.
     */
    private Long id;

    /**
     * Id of the {@link SchemaMetadataStorable} instance.
     *
     * Maintaining this for backward compatibility as users may nto want to recreate DB schemas with the changes.
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

    /**
     * If the schema is disabled.
     */
    private Boolean disabled = Boolean.FALSE;

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
        return getPrimaryKey(id);
    }

    @Override
    @JsonIgnore
    public Schema getSchema() {
        return SCHEMA;
    }
    
    @JsonIgnore
    public static PrimaryKey getPrimaryKey(Long id) {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(SCHEMA.getField(ID), id);
        return new PrimaryKey(values);
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
        return getRootEntityId();
    }

    public void setSchemaMetadataId(Long schemaMetadataId) {
        this.rootEntityId = schemaMetadataId;
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

    public Boolean getDisabled() {
        return disabled;
    }

    public void setDisabled(Boolean disabled) {
        this.disabled = disabled;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SchemaVersionInfo toSchemaVersionInfo() {
        return new SchemaVersionInfo(id, name, version, schemaText, timestamp, disabled, description);
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
                ", disabled=" + disabled +
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
        if (disabled != null ? !disabled.equals(that.disabled) : that.disabled != null) return false;
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
        result = 31 * result + (disabled != null ? disabled.hashCode() : 0);
        result = 31 * result + (fingerprint != null ? fingerprint.hashCode() : 0);
        return result;
    }
}
