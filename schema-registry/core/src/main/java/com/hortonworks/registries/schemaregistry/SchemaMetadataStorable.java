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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaMetadataStorable extends AbstractStorable {
    public static final String NAME_SPACE = "schema_metadata_info";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String GROUP = "group";
    public static final String COMPATIBILITY = "compatibility";
    public static final String TYPE = "type";
    public static final String TIMESTAMP = "timestamp";

    public static final Schema.Field NAME_FIELD = Schema.Field.of(NAME, Schema.Type.STRING);
    public static final Schema.Field GROUP_FIELD = Schema.Field.of(GROUP, Schema.Type.STRING);
    public static final Schema.Field TYPE_FIELD = Schema.Field.of(TYPE, Schema.Type.STRING);

    /**
     * Unique ID generated for this component.
     */
    private Long id;

    /**
     * Schema type which can be avro, protobuf or json etc. User can have a unique constraint with (type, name) values.
     */
    private String type;


    /**
     * Data source group for which this schema metadata belongs to.
     */
    private String group;

    /**
     * Given name of the schema.
     * Unique constraint with (name, group, type)
     */
    private String name;

    /**
     * Description of the schema.
     */
    private String description;

    /**
     * Time at which this schema was created/updated.
     */
    private Long timestamp;

    /**
     * Compatibility of this schema instance
     */
    private SchemaProvider.Compatibility compatibility = SchemaProvider.DEFAULT_COMPATIBILITY;

    public SchemaMetadataStorable() {
    }

    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fieldValues = new HashMap<Schema.Field, Object>() {{
            put(NAME_FIELD, name);
            put(GROUP_FIELD, group);
            put(TYPE_FIELD, type);
        }};
        return new PrimaryKey(fieldValues);
    }

    @Override
    @JsonIgnore
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(ID, Schema.Type.LONG),
                NAME_FIELD,
                GROUP_FIELD,
                TYPE_FIELD,
                Schema.Field.of(COMPATIBILITY, Schema.Type.STRING),
                Schema.Field.of(TIMESTAMP, Schema.Type.LONG)
        );
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> values = super.toMap();
        values.put(COMPATIBILITY, compatibility.name());
        return values;
    }

    @Override
    public Storable fromMap(Map<String, Object> map) {
        String compatibilityName = (String) map.remove(COMPATIBILITY);
        compatibility = SchemaProvider.Compatibility.valueOf(compatibilityName);
        super.fromMap(map);
        return this;
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }

    public void setCompatibility(SchemaProvider.Compatibility compatibility) {
        this.compatibility = compatibility;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaMetadataStorable that = (SchemaMetadataStorable) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (group != null ? !group.equals(that.group) : that.group != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return compatibility == that.compatibility;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (group != null ? group.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SchemaMetadataStorable{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", description='" + description + '\'' +
                ", timestamp=" + timestamp +
                ", type='" + type + '\'' +
                ", compatibility=" + compatibility +
                '}';
    }

    public static class Builder {
        private Long id;
        private String type;
        private String group;
        private String name;
        private String description;
        private Long timestamp;

        public Builder(SchemaMetadataKey schemaMetadataKey) {
        }
    }

}
