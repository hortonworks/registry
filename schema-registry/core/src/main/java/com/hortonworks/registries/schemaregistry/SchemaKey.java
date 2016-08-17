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
 * This class contains the schema metadata id and version of a schema instance. This key can be used to find any version
 * of a schema in the schema repository.
 */
public class SchemaKey implements Serializable {
    public static final int NO_VERSION = Integer.MIN_VALUE;

    private Long id;
    private Integer version;

    private SchemaKey() {
    }

    public SchemaKey(Long id) {
        this(id, NO_VERSION);
    }

    public SchemaKey(Long id, Integer version) {
        Preconditions.checkNotNull(id, "id can not be null");
        Preconditions.checkNotNull(version, "version can not be null");
        this.id = id;
        this.version = version;
    }

    public Long getId() {
        return id;
    }

    public Integer getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaKey schemaKey = (SchemaKey) o;

        if (id != null ? !id.equals(schemaKey.id) : schemaKey.id != null) return false;
        return version != null ? version.equals(schemaKey.version) : schemaKey.version == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaKey{" +
                "id=" + id +
                ", version=" + version +
                '}';
    }
}
