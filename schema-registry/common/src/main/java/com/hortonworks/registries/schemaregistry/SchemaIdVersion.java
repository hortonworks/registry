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
 * This class represents schemaMetadataId and version.
 */
public final class SchemaIdVersion implements Serializable {

    private Long schemaMetadataId;
    private Integer version;

    /** Private constructor for Jackson JSON mapping */
    @SuppressWarnings("unused")
    private SchemaIdVersion() { }

    /**
     * @param schemaMetadataId unique id of schema metadata
     * @param version version of the schema
     */
    public SchemaIdVersion(Long schemaMetadataId, Integer version) {
        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId can not be null");
        Preconditions.checkNotNull(version, "version can not be null");
        this.schemaMetadataId = schemaMetadataId;
        this.version = version;
    }

    /**
     * @return version of the schema
     */
    public Integer getVersion() { return version; }

    /**
     * @return unique id of the schema metadata.
     */
    public Long getSchemaMetadataId() {
        return schemaMetadataId;
    }

    @Override
    public String toString() {
        return "SchemaIdVersion{" +
                "schemaMetadataId=" + schemaMetadataId +
                ", version=" + version +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaIdVersion that = (SchemaIdVersion) o;

        if (schemaMetadataId != null ? !schemaMetadataId.equals(that.schemaMetadataId) : that.schemaMetadataId != null)
            return false;
        return version != null ? version.equals(that.version) : that.version == null;

    }

    @Override
    public int hashCode() {
        int result = schemaMetadataId != null ? schemaMetadataId.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }
}
