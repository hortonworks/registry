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
package com.hortonworks.registries.schemaregistry.state;

import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaLifeCycleContext {
    private SchemaLifeCycleState state;
    private SchemaVersionKey schemaVersionKey;
    private ISchemaRegistry schemaRegistry;
    private Map<String, Object> props = new HashMap<>();

    public SchemaLifeCycleContext(SchemaVersionKey schemaVersionKey, ISchemaRegistry schemaRegistry) {
        this.schemaVersionKey = schemaVersionKey;
        this.schemaRegistry = schemaRegistry;
    }

    public void setState(SchemaLifeCycleState state) {
        this.state = state;
    }

    public SchemaVersionKey getSchemaVersionKey() {
        return schemaVersionKey;
    }

    public ISchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }

    public Object getProperty(String key) {
        return props.get(key);
    }

    public Object setProperty(String key, String value) {
        return props.put(key, value);
    }

    public void startReview() throws SchemaLifeCycleException {
        state.startReview(this);
    }

    public void enable() throws SchemaLifeCycleException {
        state.enable(this);
    }

    public void disable() throws SchemaLifeCycleException {
        state.disable(this);
    }

    public void archive() throws SchemaLifeCycleException {
        state.archive(this);
    }

    public void delete() throws SchemaLifeCycleException {
        state.delete(this);
    }

}
