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

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaSerDesMapping extends AbstractStorable {
    public static final String NAMESPACE = "schema_serdes_mapping";
    public static final String SCHEMA_METADATA_ID = "schemaMetadataId";
    public static final String SERDES_ID = "serDesId";

    private Long schemaMetadataId;
    private Long serDesId;

    public SchemaSerDesMapping() {
    }

    public SchemaSerDesMapping(Long schemaMetadataId, Long serDesId) {
        this.schemaMetadataId = schemaMetadataId;
        this.serDesId = serDesId;
    }

    @Override
    public String getNameSpace() {
        return NAMESPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<>();
        fieldToObjectMap.put(new Schema.Field(SCHEMA_METADATA_ID, Schema.Type.LONG), this.schemaMetadataId);
        fieldToObjectMap.put(new Schema.Field(SERDES_ID, Schema.Type.LONG), this.serDesId);
        return new PrimaryKey(fieldToObjectMap);
    }

    public Long getSchemaMetadataId() {
        return schemaMetadataId;
    }

    public void setSchemaMetadataId(Long schemaMetadataId) {
        this.schemaMetadataId = schemaMetadataId;
    }

    public Long getSerDesId() {
        return serDesId;
    }

    public void setSerDesId(Long serDesId) {
        this.serDesId = serDesId;
    }

    @Override
    public String toString() {
        return "SchemaSerDesMapping{" +
                "schemaMetadataId=" + schemaMetadataId +
                ", serDesId=" + serDesId +
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

        SchemaSerDesMapping that = (SchemaSerDesMapping) o;

        if (schemaMetadataId != null ? !schemaMetadataId.equals(that.schemaMetadataId) : that.schemaMetadataId != null) {
            return false;
        }
        return serDesId != null ? serDesId.equals(that.serDesId) : that.serDesId == null;

    }

    @Override
    public int hashCode() {
        int result = schemaMetadataId != null ? schemaMetadataId.hashCode() : 0;
        result = 31 * result + (serDesId != null ? serDesId.hashCode() : 0);
        return result;
    }
}
