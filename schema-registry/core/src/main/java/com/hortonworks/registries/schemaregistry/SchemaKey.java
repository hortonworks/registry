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
 * This class contains the schema information. This key (name, group, type) is used
 * to find a schema in the schema repository.
 */
public class SchemaKey implements Serializable {

    /** Type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc */
    private String type;

    /**
     * Schema group to which this schema belongs to. For ex: kafka, hive etc
     * This can be used in querying schemas belonging to a specific schema group.
     */
    private String schemaGroup;

    /** Name of this schema. Follows unique constraint of (name, group, type). */
    private String name;

    /** Private constructor for Jackson JSON mapping */
    @SuppressWarnings("unused")
    private SchemaKey() {
    }

    public SchemaKey(String type, String schemaGroup, String name) {
        Preconditions.checkNotNull(type, "type can not be null");
        Preconditions.checkNotNull(schemaGroup, "schemaGroup can not be null");
        Preconditions.checkNotNull(name, "name can not be null");

        this.type = type;
        this.name = name;
        this.schemaGroup = schemaGroup;
    }

    /**
     * @return unique name of schema with in a group and type.
     */
    public String getName() {
        return name;
    }

    /**
     * @return group of the schema. For ex: Kafka, Hive.
     */
    public String getSchemaGroup() {
        return schemaGroup;
    }

    /**
     * @return type of the schema. For ex: AVRO, JSON
     */
    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaKey schemaKey = (SchemaKey) o;

        if (name != null ? !name.equals(schemaKey.name) : schemaKey.name != null) return false;
        if (schemaGroup != null ? !schemaGroup.equals(schemaKey.schemaGroup) : schemaKey.schemaGroup != null) return false;
        return type != null ? type.equals(schemaKey.type) : schemaKey.type == null;

    }

    @Override
    public int hashCode() {
        int result = (name != null ? name.hashCode() : 0);
        result = 31 * result + (schemaGroup != null ? schemaGroup.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaMetadataKey{" +
                "type='" + type + '\'' +
                ", schemaGroup='" + schemaGroup + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
