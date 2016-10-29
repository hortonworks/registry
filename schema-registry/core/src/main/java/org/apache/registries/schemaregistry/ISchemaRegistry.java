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
package org.apache.registries.schemaregistry;

import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.registries.schemaregistry.errors.UnsupportedSchemaTypeException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface ISchemaRegistry {

    void init(Map<String, Object> props);

    Long addSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException;

    Integer addSchemaVersion(SchemaMetadata schemaMetadata, String schemaText, String description) throws IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException;

    Integer addSchemaVersion(String schemaName, String schemaText, String description) throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException;

    SchemaMetadataInfo getSchemaMetadata(Long schemaMetadataId);

    SchemaMetadataInfo getSchemaMetadata(String schemaName);

    Integer getSchemaVersion(String schemaName, String schemaText) throws SchemaNotFoundException, InvalidSchemaException;

    Collection<SchemaVersionInfo> findAllVersions(String schemaName);

    SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException;

    SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException;

    Collection<SchemaMetadata> findSchemaMetadata(Map<String, String> filters);

    Collection<SchemaVersionKey> findSchemasWithFields(SchemaFieldQuery schemaFieldQuery);

    boolean isCompatible(SchemaVersionKey schemaVersionKey, String schema) throws SchemaNotFoundException;

    boolean isCompatible(String schemaName, String toSchema) throws SchemaNotFoundException;

    /**
     * Uploads the given input stream in the configured file storage and returns a unique identifier to access that file later.
     *
     * @param inputStream
     * @return
     */
    String uploadFile(InputStream inputStream);

    /**
     * Returns {@link InputStream} of the file with the given {@code fileId} if it exists.
     *
     * @param fileId
     * @return
     * @throws IOException when there are any IO errors or there is no file with the given identifier.
     */
    InputStream downloadFile(String fileId) throws IOException;

    /**
     * Returns uniqueid of the added Serializer/Deserializer
     *
     * @param serDesInfo
     * @return
     */
    Long addSerDesInfo(SerDesInfo serDesInfo);

    SerDesInfo getSerDesInfo(Long serDesId);

    Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId);

    Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId);

    /**
     * Download the jar file which contains the classes required for respective serializer/deserializer for given {@code serDesId}
     *
     * @param serDesId
     * @return
     */
    InputStream downloadJar(Long serDesId);

    void mapSerDesWithSchema(Long schemaMetadataId, Long serDesId);

}