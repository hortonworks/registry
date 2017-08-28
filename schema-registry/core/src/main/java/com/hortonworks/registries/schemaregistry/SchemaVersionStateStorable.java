/*
 * Copyright 2016 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaVersionStateStorable extends AbstractStorable {
    public static final String NAME_SPACE = "schema_version_state";

    public static final String SCHEMA_VERSION_ID = "schemaVersionId";
    public static final String STATE = "stateId";
    public static final String SEQUENCE = "sequence";
    public static final String TIMESTAMP = "timestamp";
    public static final String DETAILS = "details";
    public static final String ID = "id";

    private static final Schema.Field ID_FIELD = Schema.Field.of(ID, Schema.Type.LONG);
    private static final Schema.Field SCHEMA_VERSION_ID_FIELD = Schema.Field.of(SCHEMA_VERSION_ID, Schema.Type.LONG);
    private static final Schema.Field SEQUENCE_FIELD = Schema.Field.of(SEQUENCE, Schema.Type.INTEGER);
    private static final Schema.Field STATE_FIELD = Schema.Field.of(STATE, Schema.Type.BYTE);
    private static final Schema.Field TIMESTAMP_FIELD = Schema.Field.of(TIMESTAMP, Schema.Type.LONG);
    private static final Schema.Field DETAILS_FIELD = Schema.Field.of(DETAILS, Schema.Type.STRING);

    public static final Schema SCHEMA = Schema.of(ID_FIELD,
                                                  SCHEMA_VERSION_ID_FIELD,
                                                  SEQUENCE_FIELD,
                                                  STATE_FIELD,
                                                  TIMESTAMP_FIELD,
                                                  DETAILS_FIELD);

    // PK (schemaVersionId, stateId, sequence)
    private Long schemaVersionId;
    private Byte stateId;
    private Integer sequence;
    private Long timestamp;
    private String details;

    private long id;

    public SchemaVersionStateStorable() {
    }

    public Long getSchemaVersionId() {
        return schemaVersionId;
    }

    public Byte getStateId() {
        return stateId;
    }

    public Integer getSequence() {
        return sequence;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getDetails() {
        return details;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public void setSchemaVersionId(Long schemaVersionId) {
        this.schemaVersionId = schemaVersionId;
    }

    public void setStateId(Byte stateId) {
        this.stateId = stateId;
    }

    public void setSequence(Integer sequence) {
        this.sequence = sequence;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    @Override
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fields = new HashMap<Schema.Field, Object>(){{
        put(SCHEMA_VERSION_ID_FIELD, schemaVersionId);
        put(STATE_FIELD, stateId);
        put(SEQUENCE_FIELD, sequence);
        }};
        return new PrimaryKey(fields);
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
}
