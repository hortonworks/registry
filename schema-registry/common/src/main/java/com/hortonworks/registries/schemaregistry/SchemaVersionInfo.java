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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;

import java.io.Serializable;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SchemaVersionInfo implements Serializable {

    private static final long serialVersionUID = -669711262227194948L;

    /**
     * global unique id of this schema instance
     */
    private Long id;

    /**
     * schema metadata id
     */
    private Long schemaMetadataId;

    /**
     * name of this schema instance
     */
    private String name;
    
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
     * current stateId of this version.
     */
    private Byte stateId;


    /**
     *   If the schema version is merged from a schema branch, this is will corresponding merge message
     *   which includes the branch name from it was merged from and the schema version id
     */
    private String details;

    @SuppressWarnings("unused")
    private SchemaVersionInfo() { /* Private constructor for Jackson JSON mapping */ }

    public SchemaVersionInfo(Long id,
                             String name,
                             Integer version,
                             String schemaText,
                             Long timestamp,
                             String description) {
        this(id, name, version, null, schemaText, timestamp, description, null);
    }

    public SchemaVersionInfo(Long id,
                             String name,
                             Integer version,
                             Long schemaMetadataId,
                             String schemaText,
                             Long timestamp,
                             String description,
                             Byte stateId) {
        this.id = id;
        this.name = name;
        this.schemaMetadataId = schemaMetadataId;
        this.description = description;
        this.version = version;
        this.schemaText = schemaText;
        this.timestamp = timestamp;
        this.stateId = stateId == null ? SchemaVersionLifecycleStates.ENABLED.getId() : stateId;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
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

    public Byte getStateId() {
        return stateId;
    }

    public Long getSchemaMetadataId() {
        return schemaMetadataId;
    }

    public String getDetails() { return this.details;}

    public void setDetails(String details) { this.details = details;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionInfo that = (SchemaVersionInfo) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (schemaMetadataId != null ? !schemaMetadataId.equals(that.schemaMetadataId) : that.schemaMetadataId != null)
            return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (schemaText != null ? !schemaText.equals(that.schemaText) : that.schemaText != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (details != null ? !details.equals(that.details) : that.details != null) return false;
        return stateId != null ? stateId.equals(that.stateId) : that.stateId == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (schemaMetadataId != null ? schemaMetadataId.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (stateId != null ? stateId.hashCode() : 0);
        result = 31 * result + (details != null ? details.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaVersionInfo{" +
                "id=" + id +
                ", schemaMetadataId=" + schemaMetadataId +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", version=" + version +
                ", schemaText='" + schemaText + '\'' +
                ", timestamp=" + timestamp +
                ", stateId=" + stateId +
                ", details='" + details + '\'' +
                '}';
    }
}
