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

import java.util.List;

/**
 *
 */
public abstract class AbstractInbuiltSchemaLifecycleState implements InbuiltSchemaVersionLifecycleState {
    private final String name;
    private final byte id;
    private final String description;
    private final List<SchemaVersionLifecycleState> nextStates;

    protected AbstractInbuiltSchemaLifecycleState(String name,
                                                  byte id,
                                                  String description,
                                                  List<SchemaVersionLifecycleState> nextStates) {
        this.name = name;
        this.id = id;
        this.description = description;
        this.nextStates = nextStates;
    }

    @Override
    public Byte id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public List<SchemaVersionLifecycleState> nextStates() {
        return nextStates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractInbuiltSchemaLifecycleState that = (AbstractInbuiltSchemaLifecycleState) o;

        if (id != that.id) return false;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (int) id;
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", id=" + id +
                ", description='" + description + '\'' +
                '}';
    }
}
