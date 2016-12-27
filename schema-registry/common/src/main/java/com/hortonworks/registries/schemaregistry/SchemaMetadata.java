/**
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
 **/
package com.hortonworks.registries.schemaregistry;

import com.google.common.base.Preconditions;

import java.io.Serializable;


/**
 * This class is about metadata of a schema which includes name, type, schemaGroup, description and compatibility.
 */
public class SchemaMetadata implements Serializable {

    /**
     * Type of a schema, which can be AVRO, JSON, PROTOBUF etc
     */
    private String type;

    /**
     * Schema group to which this schema belongs to. For ex: kafka, hive etc
     * This can be used in querying schemas belonging to a specific schema group.
     */
    private String schemaGroup;

    /**
     * Unique name of this schema.
     */
    private String name;


    /**
     * Description about the schema metadata.
     */
    private String description;

    /**
     * Compatibility to be supported for all versions of this evolving schema.
     */
    private SchemaCompatibility compatibility;

    /**
     * Private constructor for Jackson JSON mapping
     */
    @SuppressWarnings("unused")
    private SchemaMetadata() {
    }

    private SchemaMetadata(String name, String type, String schemaGroup, String description, SchemaCompatibility compatibility) {
        Preconditions.checkNotNull(name, "name can not be null");
        Preconditions.checkNotNull(type, "type can not be null");

        this.name = name.trim();
        this.type = type.trim();
        this.schemaGroup = schemaGroup;
        this.description = description;
        this.compatibility = (compatibility != null) ? compatibility : SchemaCompatibility.DEFAULT_COMPATIBILITY;
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

    /**
     * @return description about the schema
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return compatibility supported by thei schema
     */
    public SchemaCompatibility getCompatibility() {
        return compatibility;
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "type='" + type + '\'' +
                ", schemaGroup='" + schemaGroup + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", compatibility=" + compatibility +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaMetadata that = (SchemaMetadata) o;

        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (schemaGroup != null ? !schemaGroup.equals(that.schemaGroup) : that.schemaGroup != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        return compatibility == that.compatibility;

    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (schemaGroup != null ? schemaGroup.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        return result;
    }

    public static class Builder {

        private final String name;
        private String type;
        private String schemaGroup;
        private String description;
        private SchemaCompatibility compatibility;

        /**
         * @param name Unique name for all versions of a schema
         */
        public Builder(String name) {
            Preconditions.checkNotNull(name, "name can not be null");
            this.name = name;
        }

        public Builder(SchemaMetadata schemaMetadata) {
            name = schemaMetadata.getName();
            type = schemaMetadata.getType();
            schemaGroup = schemaMetadata.getSchemaGroup();
            description = schemaMetadata.getDescription();
            compatibility = schemaMetadata.getCompatibility();
        }

        /**
         * @param schemaGroup schema group to which this schema belongs to. For ex: kafka, hive etc
         *                    This can be used in querying schemas belonging to a specific schema group.
         */
        public Builder schemaGroup(String schemaGroup) {
            this.schemaGroup = schemaGroup;
            return this;
        }

        /**
         * @param type Type of a schema, which can be AVRO, JSON, PROTOBUF etc
         */
        public Builder type(String type) {
            Preconditions.checkNotNull(type, "type can not be null");
            this.type = type;
            return this;
        }

        /**
         * @param description Description about the schema metadata.
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * @param compatibility Compatibility to be supported for all versions of this evolving schema.
         */
        public Builder compatibility(SchemaCompatibility compatibility) {
            this.compatibility = compatibility;
            return this;
        }

        public SchemaMetadata build() {
            return new SchemaMetadata(name, type, schemaGroup, description, compatibility);
        }
    }
}
