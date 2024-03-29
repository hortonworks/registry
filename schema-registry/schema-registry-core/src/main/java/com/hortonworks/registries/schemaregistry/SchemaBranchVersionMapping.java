/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

public class SchemaBranchVersionMapping extends AbstractStorable {
    public static final String NAMESPACE = "schema_branch_version_mapping";

    public static final String SCHEMA_BRANCH_ID = "schemaBranchId";
    public static final String SCHEMA_VERSION_INFO_ID = "schemaVersionInfoId";

    private Long schemaBranchId;
    private Long schemaVersionInfoId;

    public SchemaBranchVersionMapping() {
    }

    public SchemaBranchVersionMapping(Long schemaBranchId, Long schemaVersionInfoId) {
        this.schemaBranchId = schemaBranchId;
        this.schemaVersionInfoId = schemaVersionInfoId;
    }

    @Override
    public String getNameSpace() {
        return NAMESPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fieldToObjectMap = new HashMap<>();
        fieldToObjectMap.put(new Schema.Field(SCHEMA_BRANCH_ID, Schema.Type.LONG), this.schemaBranchId);
        fieldToObjectMap.put(new Schema.Field(SCHEMA_VERSION_INFO_ID, Schema.Type.LONG), this.schemaVersionInfoId);
        return new PrimaryKey(fieldToObjectMap);
    }

    public Long getSchemaBranchId() {
        return schemaBranchId;
    }

    public void setSchemaBranchId(Long schemaBranchId) {
        this.schemaBranchId = schemaBranchId;
    }

    public Long getSchemaVersionInfoId() {
        return schemaVersionInfoId;
    }

    public void setSchemaVersionInfoId(Long schemaVersionInfoId) {
        this.schemaVersionInfoId = schemaVersionInfoId;
    }

    @Override
    public String toString() {
        return "SchemaBranchVersionMapping{" +
                "schemaBranchId=" + schemaBranchId +
                ", schemaVersionInfoId=" + schemaVersionInfoId +
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

        SchemaBranchVersionMapping that = (SchemaBranchVersionMapping) o;

        if (schemaBranchId != null ? !schemaBranchId.equals(that.schemaBranchId) : that.schemaBranchId != null) {
            return false;
        }
        return schemaVersionInfoId != null ? schemaVersionInfoId.equals(that.schemaVersionInfoId) : that.schemaVersionInfoId == null;

    }

    @Override
    public int hashCode() {
        int result = schemaBranchId != null ? schemaBranchId.hashCode() : 0;
        result = 31 * result + (schemaVersionInfoId != null ? schemaVersionInfoId.hashCode() : 0);
        return result;
    }
}
