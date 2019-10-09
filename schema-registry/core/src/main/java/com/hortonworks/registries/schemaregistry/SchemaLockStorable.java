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

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SchemaLockStorable extends AbstractStorable {

    private static String NAMESPACE = "schema_lock";
    private static String NAME = "name";

    private String name;

    private Long timestamp;

    public static final Schema.Field NAME_FIELD = Schema.Field.of(NAME, Schema.Type.STRING);


    public SchemaLockStorable() {

    }

    public SchemaLockStorable(String name) {
        this.name = name;
    }

    public SchemaLockStorable(String name, Long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    public SchemaLockStorable(String namespace, String name) {
        this.name = namespace + "-" + name;
    }

    public SchemaLockStorable(String namespace, String name, Long timestamp) {
        this.name = namespace + "-" + name;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getNameSpace() {
        return NAMESPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(NAME_FIELD, name);
        return new PrimaryKey(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaLockStorable)) {
            return false;
        }
        SchemaLockStorable that = (SchemaLockStorable) o;
        return Objects.equals(getName(), that.getName()) &&
                Objects.equals(getTimestamp(), that.getTimestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getTimestamp());
    }

    @Override
    public String toString() {
        return "SchemaLockStorable{" +
                "name='" + name + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
