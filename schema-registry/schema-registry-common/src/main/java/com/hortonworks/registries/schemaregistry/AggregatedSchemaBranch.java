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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

/**
 *   This class represents the aggregated information about the schema branch which includes all the schema versions tied
 *   to that branch and the schema version from which the branch was created
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregatedSchemaBranch implements Serializable {

    private static final long serialVersionUID = -4013250198049024761L;

    private SchemaBranch schemaBranch;
    private Long rootSchemaVersion;
    private Collection<SchemaVersionInfo> schemaVersionInfos;

    private AggregatedSchemaBranch() {

    }

    public AggregatedSchemaBranch(SchemaBranch schemaBranch, Long rootSchemaVersion, Collection<SchemaVersionInfo> schemaVersionInfos) {
        this.schemaBranch = schemaBranch;
        this.rootSchemaVersion = rootSchemaVersion;
        this.schemaVersionInfos = schemaVersionInfos;
    }

    public SchemaBranch getSchemaBranch() {
        return schemaBranch;
    }

    public Long getRootSchemaVersion() {
        return rootSchemaVersion;
    }

    public Collection<SchemaVersionInfo> getSchemaVersionInfos() {
        return schemaVersionInfos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregatedSchemaBranch that = (AggregatedSchemaBranch) o;
        return Objects.equals(schemaBranch, that.schemaBranch) && Objects.equals(rootSchemaVersion, that.rootSchemaVersion) && Objects.equals(schemaVersionInfos, that.schemaVersionInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaBranch, rootSchemaVersion, schemaVersionInfos);
    }

    @Override
    public String toString() {
        return "AggregatedSchemaBranch{" +
                "schemaBranch=" + schemaBranch +
                ", rootSchemaVersion=" + rootSchemaVersion +
                ", schemaVersionInfos=" + schemaVersionInfos +
                '}';
    }
}
