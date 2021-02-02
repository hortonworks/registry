/**
 * Copyright 2016-2021 Cloudera, Inc.
 * <p>
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
package com.hortonworks.registries.schemaregistry.exportimport;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RawSchema {

    public static final String DEFAULT_SCHEMA_GROUP = "Kafka";
    public static final String DEFAULT_SCHEMA_TYPE = "avro";

    private final SchemaMetadataInfo metadata;
    private final SchemaVersionInfo version;

    public RawSchema(SchemaMetadataInfo metadata, SchemaVersionInfo version) {
        this.metadata = checkNotNull(metadata, "metadata");
        this.version = checkNotNull(version, "version");
    }

    @Nonnull
    public SchemaMetadataInfo getMetadata() {
        return metadata;
    }

    @Nonnull
    public SchemaVersionInfo getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RawSchema rawSchema = (RawSchema) o;
        return Objects.equals(metadata.getId(), rawSchema.metadata.getId()) &&
                Objects.equals(version.getId(), rawSchema.version.getId()) &&
                Objects.equals(metadata.getSchemaMetadata().getName(), rawSchema.metadata.getSchemaMetadata().getName()) &&
                Objects.equals(version.getVersion(), rawSchema.version.getVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadata.getId(), version.getId(), metadata.getSchemaMetadata().getName(), version.getVersion());
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    /** Create a builder for a new raw schema. */
    public static Builder builder(String schemaName) {
        return new Builder().schemaName(schemaName);
    }

    public static class Builder {

        private Builder() { }

        private Long schemaId;
        private SchemaMetadata.Builder metadataBuilder;
        private String schemaText = "";
        private Long versionId;
        private int versionNo = 1;

        /** Set the unique id of the schema. */
        public Builder schemaId(Long schemaId) {
            this.schemaId = schemaId;
            return this;
        }

        /** Return a copy of the currently existing metadata. Note that this might be different
         * than the final product, in case additional mutations are performed with the builder. */
        @Nullable
        public SchemaMetadata getMetadata() {
            if (metadataBuilder == null) {
                return null;
            }
            return metadataBuilder.build();
        }

        /** Schema name. Note: you should invoke this method before all others. */
        public Builder schemaName(String name) {
            metadataBuilder = new SchemaMetadata.Builder(name)
                    .description("")
                    .evolve(true)
                    .compatibility(SchemaCompatibility.BACKWARD)
                    .validationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL)
                    .schemaGroup(DEFAULT_SCHEMA_GROUP)
                    .type(DEFAULT_SCHEMA_TYPE);
            return this;
        }

        /** Schema description */
        public Builder description(String description) {
            checkNotNull(metadataBuilder, "Must initialize schema name first")
                    .description(description);
            return this;
        }

        /** Schema compatibility, default: BACKWARD */
        public Builder compatibility(SchemaCompatibility comp) {
            checkNotNull(metadataBuilder, "Must initialize schema name first")
                    .compatibility(comp);
            return this;
        }

        /** Validation level, default: ALL */
        public Builder validationLevel(SchemaValidationLevel validationLevel) {
            checkNotNull(metadataBuilder, "Must initialize schema name first")
                    .validationLevel(validationLevel);
            return this;
        }

        /** Schema group, default: Kafka */
        public Builder schemaGroup(String schemaGroup) {
            checkNotNull(metadataBuilder, "Must initialize schema name first")
                    .schemaGroup(schemaGroup);
            return this;
        }

        /** Schema type, default: avro */
        public Builder schemaType(String type) {
            checkNotNull(metadataBuilder, "Must initialize schema name first")
                    .type(type);
            return this;
        }

        /** Set the unique id of the version */
        public Builder versionId(long id) {
            this.versionId = id;
            return this;
        }

        /** Set the version number (of a given schema) */
        public Builder version(int version) {
            this.versionNo = version;
            return this;
        }

        /** Set the schema text */
        public Builder schemaText(String schema) {
            this.schemaText = schema;
            return this;
        }

        /** Build a new raw schema */
        public RawSchema build() {
            checkState(metadataBuilder != null, "Schema fields have not been set.");

            SchemaMetadata meta = metadataBuilder.build();
            SchemaMetadataInfo metaInfo = new SchemaMetadataInfo(meta, schemaId, System.currentTimeMillis());
            SchemaVersionInfo version = new SchemaVersionInfo(versionId, meta.getName(), versionNo, schemaText,
                    System.currentTimeMillis(), meta.getDescription());

            return new RawSchema(metaInfo, version);
        }

    }
}
