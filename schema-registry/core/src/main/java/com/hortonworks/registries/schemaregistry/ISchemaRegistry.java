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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface ISchemaRegistry {

    public void init(Map<String, Object> props);

    public SchemaMetadataStorable addSchemaMetadata(SchemaMetadataStorable schemaMetadataStorable);

    public SchemaMetadataStorable getSchemaMetadata(Long schemaMetadataId);

    public Collection<SchemaInfoStorable> findAllVersions(Long schemaMetadataId);

    public SchemaKey getSchemaInfo(String type, String schemaText);

    public SchemaInfoStorable getSchemaInfo(Long schemaMetadataId, Integer version);

    public SchemaInfoStorable getLatestSchemaInfo(Long schemaMetadataId);

    public SchemaInfoStorable addSchemaInfo(SchemaInfoStorable schemaInfoStorable);

    public Collection<SchemaInfoStorable> listAll();

    public SchemaInfoStorable getSchemaInfo(String type, String name, Integer version);

    public SchemaInfoStorable getSchemaInfo(Long schemaInfoId);

    public SchemaInfoStorable removeSchemaInfo(Long schemaInfoId);

    public boolean isCompatible(Long schemaMetadataId, Integer existingSchemaVersion, String schema) throws SchemaNotFoundException;

    public Collection<SchemaInfoStorable> getCompatibleSchemas(Long schemaMetadataId, SchemaProvider.Compatibility compatibility, String toSchema) throws SchemaNotFoundException;

    public SchemaMetadataStorable getOrCreateSchemaMetadata(SchemaMetadataStorable givenSchemaMetadataStorable);

    public String uploadFile(InputStream inputStream);

    public InputStream downloadFile(String fileId) throws IOException;

    public Long addSerDesInfo(SerDesInfoStorable serDesInfoStorable);

    public SerDesInfo getSerDesInfo(Long serializerId);

    public Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId);

    public Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId);

    public InputStream downloadJar(Long serializerId);

    public void mapSerDesWithSchema(Long schemaMetadataId, Long serDesId);

}