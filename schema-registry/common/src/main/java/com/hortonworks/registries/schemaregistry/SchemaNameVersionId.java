/*
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
 */
package com.hortonworks.registries.schemaregistry;

import com.google.common.base.Preconditions;

/**
 * This class represents schema, its name, version and unique id.
 */
public class SchemaNameVersionId {

    private String name;
    
    private Integer version;
    
    private Long id;

    private String schemaText;


    public SchemaNameVersionId(String name, Integer version, Long id, String schemaText) {
        Preconditions.checkNotNull(name, "name can not be null");
        Preconditions.checkNotNull(version, "version can not be null");
        Preconditions.checkNotNull(id, "id can not be null");
        Preconditions.checkNotNull(schemaText, "schemaText can not be null");
        this.name = name;
        this.version = version;
        this.id = id;
        this.schemaText = schemaText;
    }

    public String getName() {
        return name;
    }

    public Integer getVersion() {
        return version;
    }

    public Long getId() {
        return id;
    }

    public String getSchemaText() {
        return schemaText;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchemaNameVersionId that = (SchemaNameVersionId) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (version != null ? !version.equals(that.version) : that.version != null) {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        return schemaText != null ? schemaText.equals(that.schemaText) : that.schemaText == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaNameVersionId{" +
               "name='" + name + '\'' +
               ", version=" + version +
               ", id=" + id +
               ", schemaText='" + schemaText + '\'' +
               '}';
    }
}
