/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.Serializable;

/**
 * This class contains the schema metadata. This key can be used to find any version
 * of a schema in the schema repository.
 */
public class SchemaKey implements Serializable {
    public static final int NO_VERSION = Integer.MIN_VALUE;

    private Long id;
    private Integer version;
    private String namespace;
    private String group;

    private SchemaKey() {
    }

    public SchemaKey(String group, String namespace, Long id) {
        this(group, namespace, id, NO_VERSION);
    }

    public SchemaKey(String group, String namespace, Long id, Integer version) {
        Preconditions.checkNotNull(group, "group can not be null");
        Preconditions.checkNotNull(namespace, "namespace can not be null");
        Preconditions.checkNotNull(id, "id can not be null");
        Preconditions.checkNotNull(version, "version can not be null");

        this.namespace = namespace;
        this.group = group;
        this.id = id;
        this.version = version;
    }

    public Long getId() {
        return id;
    }

    public Integer getVersion() {
        return version;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaKey schemaKey = (SchemaKey) o;

        if (id != null ? !id.equals(schemaKey.id) : schemaKey.id != null) return false;
        if (version != null ? !version.equals(schemaKey.version) : schemaKey.version != null) return false;
        if (namespace != null ? !namespace.equals(schemaKey.namespace) : schemaKey.namespace != null) return false;
        return group != null ? group.equals(schemaKey.group) : schemaKey.group == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaKey{" +
                "id=" + id +
                ", version=" + version +
                ", namespace='" + namespace + '\'' +
                ", group='" + group + '\'' +
                '}';
    }
}
