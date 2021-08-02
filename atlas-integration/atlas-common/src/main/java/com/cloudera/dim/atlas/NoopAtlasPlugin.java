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
package com.cloudera.dim.atlas;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.Map;
import java.util.Optional;

public class NoopAtlasPlugin implements AtlasPlugin {

    @Override
    public void initialize(Map<String, Object> config) { }

    @Override
    public void setupAtlasModel() { }

    @Override
    public boolean isAtlasModelInitialized() {
        return true;
    }

    @Override
    public boolean setupKafkaSchemaModel() {
        return true;
    }

    @Override
    public boolean isKafkaSchemaModelInitialized() {
        return true;
    }

    @Override
    public void connectSchemaWithTopic(String metaGuid, SchemaMetadataInfo schemaMetadataInfo) { }

    @Override
    public String createMeta(SchemaMetadataInfo meta) {
        return null;
    }

    @Override
    public Optional<SchemaMetadataInfo> updateMeta(SchemaMetadata schemaMetadata) throws SchemaNotFoundException {
        return Optional.empty();
    }

    @Override
    public void addSchemaVersion(String schemaName, SchemaVersionInfo schemaVersion) throws SchemaNotFoundException { }
}
