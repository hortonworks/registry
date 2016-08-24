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

import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.VersionedSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface ISchemaRegistry {

    void init(Map<String, Object> props);

    Long addSchemaMetadata(SchemaMetadata schemaMetadata);

    Integer addSchema(SchemaMetadata schemaMetadata, VersionedSchema versionedSchema);

    Integer addSchema(SchemaMetadataKey schemaMetadataKey, VersionedSchema versionedSchema) throws SchemaNotFoundException;

    SchemaMetadataStorable getSchemaMetadata(Long schemaMetadataId);

    Collection<SchemaInfoStorable> findAllVersions(Long schemaMetadataId);

    Integer getSchemaVersion(SchemaMetadataKey schemaMetadataKey, String schemaText);

    SchemaInfoStorable getSchemaInfo(Long schemaMetadataId, Integer version);

    SchemaInfoStorable getLatestSchemaInfo(Long schemaMetadataId);

    SchemaInfoStorable addSchemaInfo(SchemaInfoStorable schemaInfoStorable);

    Collection<SchemaInfoStorable> listAll();

    SchemaInfoStorable getSchemaInfo(Long schemaInfoId);

    SchemaInfoStorable removeSchemaInfo(Long schemaInfoId);

    boolean isCompatible(Long schemaMetadataId, Integer existingSchemaVersion, String schema) throws SchemaNotFoundException;

    boolean isCompatible(Long schemaMetadataId, String toSchema) throws SchemaNotFoundException;

    Collection<SchemaInfoStorable> getCompatibleSchemas(Long schemaMetadataId, SchemaProvider.Compatibility compatibility, String toSchema) throws SchemaNotFoundException;

    SchemaMetadataStorable getOrCreateSchemaMetadata(SchemaMetadata givenSchemaMetadataStorable);

    String uploadFile(InputStream inputStream);

    InputStream downloadFile(String fileId) throws IOException;

    Long addSerDesInfo(SerDesInfoStorable serDesInfoStorable);

    SerDesInfo getSerDesInfo(Long serializerId);

    Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId);

    Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId);

    InputStream downloadJar(Long serializerId);

    void mapSerDesWithSchema(Long schemaMetadataId, Long serDesId);

    SchemaMetadataStorable getSchemaMetadata(SchemaMetadataKey schemaMetadataKey);

}