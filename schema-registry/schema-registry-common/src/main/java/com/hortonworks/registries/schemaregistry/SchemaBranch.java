/**
 * Copyright 2017-2019 Cloudera, Inc.
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
 **/

package com.hortonworks.registries.schemaregistry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.swagger.annotations.ApiModelProperty;

import javax.annotation.Nonnull;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaBranch implements Serializable {

    public static final String MASTER_BRANCH = "MASTER";
    public static final String MASTER_BRANCH_DESC = "'MASTER' branch for schema metadata '%s'";

    private static final long serialVersionUID = -5159269803911927338L;

    private Long id;
    private String name;
    private String schemaMetadataName;
    private String description;
    private Long timestamp;

    private SchemaBranch() {

    }

    public SchemaBranch(String name, String schemaMetadataName) {
        this(name, schemaMetadataName, null, null);
    }

    public SchemaBranch(String name, String schemaMetadataName, String description, Long timestamp) {
       this(null, name, schemaMetadataName, description, timestamp);
    }

    public SchemaBranch(Long id, String name, String schemaMetadataName, String description, Long timestamp) {
        this.id = id;
        this.name = name;
        this.schemaMetadataName = schemaMetadataName;
        this.description = description;
        this.timestamp = timestamp;
    }

    public Long getId() {
        return this.id;
    }

    @ApiModelProperty(required = true, value = "Name must not be null")
    @Nonnull
    public String getName() {
        return this.name;
    }

    public String getSchemaMetadataName() { 
        return this.schemaMetadataName;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public String getDescription() { 
        return this.description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchemaBranch schemaFieldInfo = (SchemaBranch) o;

        if (name != null ? !name.equals(schemaFieldInfo.name) : schemaFieldInfo.name != null) {
            return false;
        }
        if (schemaMetadataName != null ? 
                !schemaMetadataName.equals(schemaFieldInfo.schemaMetadataName) : schemaFieldInfo.schemaMetadataName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (schemaMetadataName != null ? schemaMetadataName.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaBranch {" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", schemaMetadataName='" + schemaMetadataName + '\'' +
                ", description='" + description + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
