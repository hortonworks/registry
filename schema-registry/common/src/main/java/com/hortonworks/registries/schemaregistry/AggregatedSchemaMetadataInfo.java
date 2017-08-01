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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Collection;

/**
 * This class represents aggregated information about schema metadata which includes versions and mapped serdes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregatedSchemaMetadataInfo implements Serializable {

    private static final long serialVersionUID = -414992394022547720L;

    /**
     * Metadata about the schema
     */
    private SchemaMetadata schemaMetadata;

    private Long id;

    private Long timestamp;

    private Collection<SchemaVersionInfo> versions;

    private Collection<SerDesInfo> serDesInfos;

    public AggregatedSchemaMetadataInfo(SchemaMetadata schemaMetadata,
                                        Long id,
                                        Long timestamp,
                                        Collection<SchemaVersionInfo> versions,
                                        Collection<SerDesInfo> serDesInfos) {
        this.schemaMetadata = schemaMetadata;
        this.id = id;
        this.timestamp = timestamp;
        this.versions = versions;
        this.serDesInfos = serDesInfos;
    }

    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }

    public Long getId() {
        return id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Collection<SchemaVersionInfo> getVersions() {
        return versions;
    }

    public Collection<SerDesInfo> getSerDesInfos() {
        return serDesInfos;
    }

    @Override
    public String toString() {
        return "AggregatedSchemaMetadataInfo{" +
                "schemaMetadata=" + schemaMetadata +
                ", id=" + id +
                ", timestamp=" + timestamp +
                ", versions=" + versions +
                ", serDesInfos=" + serDesInfos +
                '}';
    }
}
