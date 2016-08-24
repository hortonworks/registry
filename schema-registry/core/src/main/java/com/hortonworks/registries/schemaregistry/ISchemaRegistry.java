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

    public void init(Map<String, Object> props);

    public Long addSchemaMetadata(SchemaMetadata schemaMetadata);

    public Integer addSchema(SchemaMetadata schemaMetadata, VersionedSchema versionedSchema);

    public Integer addSchema(SchemaMetadataKey schemaMetadataKey, VersionedSchema versionedSchema) throws SchemaNotFoundException;

    public SchemaMetadataStorable getSchemaMetadata(SchemaMetadataKey schemaMetadataKey);


    public SchemaMetadataStorable getSchemaMetadata(Long schemaMetadataId);

    public Collection<SchemaInfoStorable> findAllVersions(Long schemaMetadataId);

    public Integer getSchemaVersion(SchemaMetadataKey schemaMetadataKey, String schemaText) throws SchemaNotFoundException;


    public SchemaInfoStorable getSchemaInfo(Long schemaMetadataId, Integer version) throws SchemaNotFoundException;

    public SchemaInfoStorable getLatestSchemaInfo(Long schemaMetadataId) throws SchemaNotFoundException;

    public Collection<SchemaInfoStorable> listAll();

    public SchemaMetadataStorable getOrCreateSchemaMetadata(SchemaMetadata givenSchemaMetadataStorable);


    public boolean isCompatible(Long schemaMetadataId, Integer existingSchemaVersion, String schema) throws SchemaNotFoundException;

    public boolean isCompatible(Long schemaMetadataId, String toSchema) throws SchemaNotFoundException;


    public String uploadFile(InputStream inputStream);

    public InputStream downloadFile(String fileId) throws IOException;

    public Long addSerDesInfo(SerDesInfoStorable serDesInfoStorable);

    public SerDesInfo getSerDesInfo(Long serializerId);

    public Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId);

    public Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId);

    public InputStream downloadJar(Long serializerId);

    public void mapSerDesWithSchema(Long schemaMetadataId, Long serDesId);

}