/**
 * Copyright 2016-2019 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.schemaregistry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import io.swagger.annotations.ApiModelProperty;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * This class represents details about versioned instance of a schema which includes description and schema text.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaVersion implements Serializable {
    private static final long serialVersionUID = 1664618495690787804L;

    private String description;
    private String schemaText;
    private Byte initialState;
    private byte [] stateDetails;

    @SuppressWarnings("unused")
    private SchemaVersion() {
        /* Private constructor for Jackson JSON mapping */
        this(null, null, null);
    }

    public SchemaVersion(String schemaText, String description) {
        this(schemaText, description, null);
    }

    public SchemaVersion(String schemaText, String description, Byte initialState) {
        this(schemaText, description, initialState, null);
    }

    public SchemaVersion(String schemaText, String description, Byte initialState, byte [] stateDetails) {
        this.description = description;
        this.schemaText = schemaText;
        this.initialState = initialState != null ? initialState : SchemaVersionLifecycleStates.ENABLED.getId();
        this.stateDetails = stateDetails;
    }
    
    public SchemaVersion(SchemaVersionInfo schemaVersionInfo) {
        this.description = schemaVersionInfo.getDescription();
        this.schemaText = schemaVersionInfo.getSchemaText();
        this.initialState = schemaVersionInfo.getStateId();
    }

    public void setState(Byte state) {
        this.initialState = state;
    }

    public void setStateDetails(byte [] abstractStateDetails) {
        this.stateDetails = abstractStateDetails;
    }

    @ApiModelProperty(required = true, value = "Description must not be null")
    @Nonnull
    public String getDescription() {
        return description;
    }
    
    @ApiModelProperty(required = true, value = "Schema text must not be null")
    @Nonnull
    public String getSchemaText() {
        return schemaText;
    }

    @ApiModelProperty(example = "5")
    public Byte getInitialState() {
        return initialState;
    }

    @ApiModelProperty(example = "null")
    public byte [] getStateDetails() {
        return stateDetails;
    }

    @Override
    public String toString() {
        return "SchemaVersion{" +
                "description='" + description + '\'' +
                ", schemaText='" + schemaText + '\'' +
                ", initialState='" + initialState + '\'' +
                ", details=" + stateDetails +
                '}';
    }
}
