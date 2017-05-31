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

import java.io.Serializable;

/**
 *
 */
public final class SchemaVersionInfo implements Serializable {

    /**
     * global unique id of this schema instance
     */
    private Long id;

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
    

    @SuppressWarnings("unused")
    private SchemaVersionInfo() { /* Private constructor for Jackson JSON mapping */ }

    public SchemaVersionInfo(Long id, String name, Integer version, String schemaText, Long timestamp, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.version = version;
        this.schemaText = schemaText;
        this.timestamp = timestamp;
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

    @Override
    public String toString() {
        return "SchemaVersionInfo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", version=" + version +
                ", schemaText='" + schemaText + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionInfo that = (SchemaVersionInfo) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (schemaText != null ? !schemaText.equals(that.schemaText) : that.schemaText != null) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
