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
 * This class contains the schema metadata information. This key can be used to find any schema metadata in
 * the schema repository.
 */
public class SchemaMetadataKey implements Serializable {

    /**
     * type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc
     */
    private String type;

    /**
     * Data source group to which this schema belongs to. For ex: kafka, hive etc
     * This can be used in querying schemas belonging to a specific data source group.
     */
    private String group;

    /**
     * name of this schema.
     * Follows unique constraint of (name, group, type).
     */
    private String name;

    private SchemaMetadataKey() {
    }

    public SchemaMetadataKey(String type, String group, String name) {
        Preconditions.checkNotNull(type, "type can not be null");
        Preconditions.checkNotNull(group, "group can not be null");
        Preconditions.checkNotNull(name, "namespace can not be null");

        this.type = type;
        this.name = name;
        this.group = group;
    }

    /**
     * Unique name of schema with in a group.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Group of the schema. For ex: Kafka, Hive.
     *
     * @return
     */
    public String getGroup() {
        return group;
    }

    /**
     * Returns type of the schema. For ex: AVRO, JSON
     *
     * @return
     */
    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaMetadataKey schemaKey = (SchemaMetadataKey) o;

        if (name != null ? !name.equals(schemaKey.name) : schemaKey.name != null) return false;
        if (group != null ? !group.equals(schemaKey.group) : schemaKey.group != null) return false;
        return type != null ? type.equals(schemaKey.type) : schemaKey.type == null;

    }

    @Override
    public int hashCode() {
        int result = (name != null ? name.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaKey{" +
                " name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

}
