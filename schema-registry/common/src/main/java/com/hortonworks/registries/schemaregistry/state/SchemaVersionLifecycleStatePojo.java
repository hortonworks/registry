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
package com.hortonworks.registries.schemaregistry.state;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 * This class is used for serialization/deserialiation of {@link SchemaVersionLifecycleState} instances with json format.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SchemaVersionLifecycleStatePojo implements SchemaVersionLifecycleState, Serializable {
    private static final long serialVersionUID = -9100601483425067672L;

    private Byte id;
    private String name;
    private String description;

    private SchemaVersionLifecycleStatePojo() {
    }

    public SchemaVersionLifecycleStatePojo(SchemaVersionLifecycleState schemaVersionLifecycleState) {
        this.id = schemaVersionLifecycleState.getId();
        this.name = schemaVersionLifecycleState.getName();
        this.description = schemaVersionLifecycleState.getDescription();
    }

    public Byte getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaVersionLifecycleStatePojo that = (SchemaVersionLifecycleStatePojo) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return description != null ? description.equals(that.description) : that.description == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaVersionLifecycleStatePojo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
