/*
 * Copyright 2016-2021 Cloudera, Inc.
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

package com.hortonworks.registries.storage.impl.jdbc.sequences;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class NamespaceSequenceStorable extends AbstractStorable {

    public static final String NAMESPACE = "namespace_sequence";
    private static final String NAMESPACE_FIELD_NAME = "namespace";
    private static final String NEXT_ID_FIELD_NAME = "nextId";

    private String namespace;
    private long nextId;

    public static final Schema.Field NAMESPACE_FIELD = Schema.Field.of(NAMESPACE_FIELD_NAME, Schema.Type.STRING);
    public static final Schema.Field NEXT_ID_FIELD = Schema.Field.of(NEXT_ID_FIELD_NAME, Schema.Type.LONG);

    public static final Schema SCHEMA = Schema.of(NAMESPACE_FIELD, NEXT_ID_FIELD);

    public NamespaceSequenceStorable() {
    }

    public NamespaceSequenceStorable(String namespace, long nextId) {
        this.namespace = namespace;
        this.nextId = nextId;
    }

    public NamespaceSequenceStorable(String namespace) {
        this(namespace, 0);
    }

    public static StorableKey getStorableKeyForNamespace(String nameSpace) {
        return new NamespaceSequenceStorable(nameSpace).getStorableKey();
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public long getNextId() {
        return nextId;
    }

    public void setNextId(long nextId) {
        this.nextId = nextId;
    }

    @Override
    public String getNameSpace() {
        return NAMESPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(NAMESPACE_FIELD, namespace);
        return new PrimaryKey(values);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> values = super.toMap();
        values.put(NAMESPACE_FIELD_NAME, namespace);
        values.put(NEXT_ID_FIELD_NAME, nextId);
        return values;
    }

    @Override
    public Storable fromMap(Map<String, Object> map) {
        namespace = getIgnoreCase(map, NAMESPACE_FIELD_NAME);
        nextId = getIgnoreCase(map, NEXT_ID_FIELD_NAME);
        return this;
    }

    @SuppressWarnings("unchecked")
    private <T> T getIgnoreCase(Map<String, Object> map, String fieldName) {
        Optional<Object> value = map.entrySet()
                .stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(fieldName))
                .map(Map.Entry::getValue)
                .findFirst();
        return (T) value.orElse(null);
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }

    /**
     * @return incremented sequence copied over (this.nextId is not changed)
     */
    public NamespaceSequenceStorable increment() {
        return new NamespaceSequenceStorable(namespace, nextId + 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NamespaceSequenceStorable)) {
            return false;
        }
        NamespaceSequenceStorable that = (NamespaceSequenceStorable) o;
        return Objects.equals(getNamespace(), that.getNamespace()) &&
                Objects.equals(getNextId(), that.getNextId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNamespace(), getNextId());
    }

    @Override
    public String toString() {
        return "SchemaLockStorable{" +
                "namespace='" + namespace + '\'' +
                ", nextId=" + nextId +
                '}';
    }
}
