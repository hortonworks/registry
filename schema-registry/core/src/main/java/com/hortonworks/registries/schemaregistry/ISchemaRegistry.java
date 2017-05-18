/**
 * Copyright 2016 Hortonworks.
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

import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface ISchemaRegistry {

    String SCHEMA_PROVIDERS = "schemaProviders";

    void init(Map<String, Object> props);

    Collection<SchemaProviderInfo> getRegisteredSchemaProviderInfos();

    Long addSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException;

    Long addSchemaMetadata(SchemaMetadata schemaMetadata, boolean throwErrorIfExists) throws UnsupportedSchemaTypeException;

    Integer addSchemaVersion(SchemaMetadata schemaMetadata, String schemaText, String description) throws IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException, SchemaNotFoundException;

    Integer addSchemaVersion(String schemaName, String schemaText, String description) throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException;

    SchemaMetadataInfo getSchemaMetadata(Long schemaMetadataId);

    SchemaMetadataInfo getSchemaMetadata(String schemaName);

    Integer getSchemaVersion(String schemaName, String schemaText) throws SchemaNotFoundException, InvalidSchemaException;

    List<SchemaVersionInfo> findAllVersions(String schemaName);

    SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException;

    SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException;

    Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> filters);

    Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> filters);

    Collection<SchemaVersionKey> findSchemasWithFields(SchemaFieldQuery schemaFieldQuery);

    CompatibilityResult checkCompatibility(SchemaVersionKey schemaVersionKey, String schema) throws SchemaNotFoundException;

    CompatibilityResult checkCompatibility(String schemaName, String toSchema) throws SchemaNotFoundException;

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
    Long addSerDesInfo(SerDesPair serDesInfo);

    SerDesInfo getSerDesInfo(Long serDesId);

    Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId);

    /**
     * Download the jar file which contains the classes required for respective serializer/deserializer for given {@code serDesId}
     *
     * @param serDesId
     * @return
     */
    InputStream downloadJar(Long serDesId);

    void mapSerDesWithSchema(Long schemaMetadataId, Long serDesId);

    AggregatedSchemaMetadataInfo getAggregatedSchemaMetadata(String schemaName);
}