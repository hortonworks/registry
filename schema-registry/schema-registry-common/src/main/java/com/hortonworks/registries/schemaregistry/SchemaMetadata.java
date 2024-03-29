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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.swagger.annotations.ApiModelProperty;

import javax.annotation.Nonnull;
import java.io.Serializable;


/**
 * This class is about metadata of a schema which includes name, type, schemaGroup, description, compatibility and evolve.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaMetadata implements Serializable {

    private static final long serialVersionUID = -6880986623123299254L;

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
     * Compatibility to be supported for versions of this evolving schema.
     */
    // the field name is different from the accessor name in order to prevent
    // Jackson from setting the field value with reflection
    private SchemaCompatibility schemaCompatibility;

    /**
     * Validation level of this evolving schema, if to validate all or just latest.
     */
    // the field name is different from the accessor name in order to prevent
    // Jackson from setting the field value with reflection
    private SchemaValidationLevel schemaValidationLevel;

    /**
     * Whether this can have evolving schemas or not. If false, this can have only one version of the schema.
     */
    @ApiModelProperty(example = "true")
    @JsonProperty("evolve")
    public boolean evolve = true;

    /**
     * Private constructor for Jackson JSON mapping
     */
    @SuppressWarnings("unused")
    private SchemaMetadata() {
    }

    private SchemaMetadata(String name,
                           String type,
                           String schemaGroup,
                           String description,
                           SchemaCompatibility compatibility,
                           SchemaValidationLevel validationLevel,
                           boolean evolve) {
        Preconditions.checkNotNull(name, "name can not be null");
        Preconditions.checkNotNull(type, "type can not be null");

        this.name = name;
        this.type = type;
        this.schemaGroup = schemaGroup;
        this.description = description;
        this.evolve = evolve;
        this.schemaCompatibility = compatibility;
        this.schemaValidationLevel = validationLevel;
    }

    /**
     * @return unique name of schema with in a group and type.
     */
    @ApiModelProperty(required = true, value = "Name must not be null")
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * @return group of the schema. For ex: Kafka, Hive.
     */
    @ApiModelProperty(required = true, value = "SchemaGroup must not be null")
    @Nonnull
    public String getSchemaGroup() {
        return schemaGroup;
    }

    /**
     * @return type of the schema. For ex: AVRO, JSON
     */
    @ApiModelProperty(required = true, value = "Type must not be null", example = "avro")
    @Nonnull
    public String getType() {
        return type;
    }

    /**
     * @return description about the schema
     */
    public String getDescription() {
        return description;
    }

    /** Internal use only! */
    @com.hortonworks.registries.shaded.com.fasterxml.jackson.annotation.JsonGetter("compatibility")
    @JsonIgnore
    public SchemaCompatibility getInternalCompatibility() {
        return schemaCompatibility;
    }

    /**
     * @return compatibility supported by this schema
     */
    @ApiModelProperty(example = "BACKWARD")
    public SchemaCompatibility getCompatibility() {
        return schemaCompatibility != null ? schemaCompatibility : SchemaCompatibility.DEFAULT_COMPATIBILITY;
    }

    public void setCompatibility(SchemaCompatibility compatibility) {
        this.schemaCompatibility = compatibility;
    }

    /** Internal use only! */
    @com.hortonworks.registries.shaded.com.fasterxml.jackson.annotation.JsonGetter("validationLevel")
    @JsonIgnore
    public SchemaValidationLevel getInternalValidationLevel() {
        return schemaValidationLevel;
    }

    /**
     * @return validation supported by this schema
     */
    public SchemaValidationLevel getValidationLevel() {
        return schemaValidationLevel != null ? schemaValidationLevel : SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL;
    }

    public void setValidationLevel(SchemaValidationLevel schemaValidationLevel) {
        this.schemaValidationLevel = schemaValidationLevel;
    }

    public boolean isEvolve() {
        return evolve;
    }

    @Override
    public String toString() {
        return "SchemaMetadata{" +
                "type='" + type + '\'' +
                ", schemaGroup='" + schemaGroup + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", compatibility=" + schemaCompatibility +
                ", validationLevel=" + schemaValidationLevel +
                ", evolve=" + evolve +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchemaMetadata that = (SchemaMetadata) o;

        if (evolve != that.evolve) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        if (schemaGroup != null ? !schemaGroup.equals(that.schemaGroup) : that.schemaGroup != null) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null) {
            return false;
        }
        if (getValidationLevel() != null ? !getValidationLevel().equals(that.getValidationLevel())
                                         : that.getValidationLevel() != null) {
            return false;
        }
        return getCompatibility() == that.getCompatibility();
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (schemaGroup != null ? schemaGroup.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (getCompatibility() != null ? getCompatibility().hashCode() : 0);
        result = 31 * result + (getValidationLevel() != null ? getValidationLevel().hashCode() : 0);
        result = 31 * result + (evolve ? 1 : 0);
        return result;
    }

    public void trim() {
        name = (name != null) ? name.trim() : name;
        type = (type != null) ? type.trim() : type;
    }

    public static class Builder {

        private final String name;
        private String type;
        private String schemaGroup;
        private String description;
        private SchemaCompatibility compatibility;
        private SchemaValidationLevel validationLevel;

        // default value is always true.
        private boolean evolve = true;

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
            compatibility = schemaMetadata.getInternalCompatibility();
            validationLevel = schemaMetadata.getInternalValidationLevel();
            evolve = schemaMetadata.isEvolve();
        }

        /**
         * @param schemaGroup schema group to which this schema belongs to. For ex: kafka, hive etc
         *                    This can be used in querying schemas belonging to a specific schema group.
         *
         * @return the current instance.
         */
        public Builder schemaGroup(String schemaGroup) {
            this.schemaGroup = schemaGroup;
            return this;
        }

        /**
         * @param type Type of a schema, which can be AVRO, JSON, PROTOBUF etc
         *
         * @return the current instance.
         */
        public Builder type(String type) {
            Preconditions.checkNotNull(type, "type can not be null");
            this.type = type;
            return this;
        }

        /**
         * @param description Description about the schema metadata.
         *
         * @return the current instance.
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * @param compatibility Compatibility to be supported for all versions of this evolving schema.
         *
         * @return the current instance.
         */
        public Builder compatibility(SchemaCompatibility compatibility) {
            this.compatibility = compatibility;
            return this;
        }

        /**
         * @param validationLevel Validation level to be supported for versions of this evolving schema, all or latest.
         *
         * @return the current instance.
         */
        public Builder validationLevel(SchemaValidationLevel validationLevel) {
            this.validationLevel = validationLevel;
            return this;
        }

        /**
         * @param evolve whether to support multiple version of a schema. If it is set to false then only one version
         *               of the schema can be added.
         *
         * @return the current instance.
         */
        public Builder evolve(boolean evolve) {
            this.evolve = evolve;
            return this;
        }

        /**
         * @return SchemaMetadata instance built from the
         */
        public SchemaMetadata build() {
            return new SchemaMetadata(name, type, schemaGroup, description, compatibility, validationLevel, evolve);
        }
    }
}
