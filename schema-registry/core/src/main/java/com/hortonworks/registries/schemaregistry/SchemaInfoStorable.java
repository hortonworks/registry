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
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaInfoStorable extends AbstractStorable {
    public static final String NAME_SPACE = "schema_instance_info";
    public static final String ID = "id";
    public static final String SCHEMA_METADATA_ID = "schemaMetadataId";
    public static final String DESCRIPTION = "description";
    public static final String SCHEMA_TEXT = "schemaText";
    public static final String VERSION = "version";
    public static final String TIMESTAMP = "timestamp";
    public static final String FINGERPRINT = "fingerprint";

    /**
     * Unique ID generated for this component.
     */
    private Long id;

    /**
     * Id of the SchemaMetadata instance.
     */
    private Long schemaMetadataId;

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

    public SchemaInfoStorable() {
    }

    public SchemaInfoStorable(SchemaInfoStorable givenSchemaInfoStorable) {
        id = givenSchemaInfoStorable.id;
        schemaMetadataId = givenSchemaInfoStorable.schemaMetadataId;
        version = givenSchemaInfoStorable.version;
        schemaText = givenSchemaInfoStorable.schemaText;
        timestamp = givenSchemaInfoStorable.timestamp;
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
                Schema.Field.of(DESCRIPTION, Schema.Type.STRING),
                Schema.Field.of(VERSION, Schema.Type.LONG),
                Schema.Field.of(TIMESTAMP, Schema.Type.LONG),
                // schema does not really support array with given types!!!, it simply assumes that as list
                Schema.Field.of(FINGERPRINT, Schema.Type.ARRAY)
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
}
