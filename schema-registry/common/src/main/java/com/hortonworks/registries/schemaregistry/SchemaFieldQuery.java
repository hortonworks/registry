/**
 * Copyright 2016-2019 Cloudera, Inc.
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class SchemaFieldQuery {
    private final String name;
    private final String namespace;
    private final String type;

    public SchemaFieldQuery(String name, String namespace, String type) {
        this.name = name;
        this.namespace = namespace;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getType() {
        return type;
    }

    public Map<String, String> toQueryMap() {
        Map<String, String> queryMap = new HashMap<>();
        if(name != null) {
            queryMap.put(SchemaFieldInfo.NAME, name);
        }
        if(namespace != null) {
            queryMap.put(SchemaFieldInfo.FIELD_NAMESPACE, namespace);
        }
        if(type != null) {
            queryMap.put(SchemaFieldInfo.TYPE, type);
        }

        return Collections.unmodifiableMap(queryMap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaFieldQuery that = (SchemaFieldQuery) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, namespace, type);
    }

    @Override
    public String toString() {
        return "SchemaFieldQuery{" +
                "name='" + name + '\'' +
                ", namespace='" + namespace + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    public static class Builder {
        private String name;
        private String namespace;
        private String type;

        public Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public SchemaFieldQuery build() {
            return new SchemaFieldQuery(name, namespace, type);
        }
    }
}
