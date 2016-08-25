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
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.VersionedSchema;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collection;

/**
 * This interface defines different methods to interact with remote schema registry.
 * <p>
 * Below code describes how to register new schemas, add new version of a schema and fetch different versions of a schema.
 * <pre>
 *
 * // registering new schema-metadata
 * SchemaMetadataKey schemaMetadataKey = new SchemaMetadataKey(AvroSchemaProvider.TYPE, SCHEMA_GROUP, schemaName);
 * SchemaProvider.Compatibility compatibility = SchemaProvider.Compatibility.BOTH;
 * SchemaMetadata schemaMetadata = new SchemaMetadata(schemaMetadataKey, "devices schema", compatibility);
 *
 * Long schemaMetadataId = schemaRegistry.addSchemaMetadata(schemaMetadata);
 *
 * Integer v1 = schemaRegistry.addSchema(schemaMetadata, new VersionedSchema(schema1, "initial version of the schema"));
 * Integer v2 = schemaRegistry.addSchema(schemaMetadataKey, new VersionedSchema(schema2, "second version of the the schema"));
 *
 * // get the specific version of the schema
 * SchemaInfo schemaInfo = schemaRegistryClient.getSchema(new SchemaKey(schemaMetadataKey, v2));
 *
 * // get the latest version of the schema
 * SchemaInfo latest = schemaRegistryClient.getLatestSchema(schemaMetadataKey);
 *
 * // get all versions of a specific schema
 * Collection<SchemaInfo> allVersions = schemaRegistryClient.getAllVersions(schemaMetadataKey);
 *
 * </pre>
 * <p>
 * Below code describes how to register serializer and deserializers, map them with a schema etc.
 * <pre>
 * // upload a jar containing serializer and deserializer classes.
 * InputStream inputStream = new FileInputStream("/schema-custom-ser-des.jar");
 * String fileId = schemaRegistryClient.uploadFile(inputStream);
 *
 * // add serializer with the respective uploaded jar file id.
 * SerDesInfo serializerInfo = createSerDesInfo(fileId);
 * Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);
 *
 * schemaMetadataKey = new SchemaMetadataKey(type(), "kafka", "com.hwx.iot.device.schema");
 *
 * // map this serializer with a registered schema
 * schemaRegistryClient.mapSchemaWithSerDes(schemaMetadataKey, serializerId);
 *
 * // get registered serializers
 * Collection<SerDesInfo> serializers = schemaRegistryClient.getSerializers(schemaMetadataKey);
 * SerDesInfo registeredSerializerInfo = serializers.iterator().next();
 *
 * //get serializer and serialize the given payload
 * try(AvroSnapshotSerializer snapshotSerializer = schemaRegistryClient.createInstance(registeredSerializerInfo);) {
 * Map<String, Object> config = Collections.emptyMap();
 * snapshotSerializer.init(config);
 *
 * byte[] serializedData = snapshotSerializer.serialize(input, schemaMetadata);
 *
 * </pre>
 */
public interface ISchemaRegistryClient extends AutoCloseable {

    /**
     * Returns true if the given {@code schemaMetadata} is successfully registered.
     *
     * @param schemaMetadata
     * @return
     */
    boolean registerSchemaMetadata(SchemaMetadata schemaMetadata) throws InvalidSchemaException;

    /**
     * Returns version of the schema added to schema-metadata.
     * <pre>
     * It tries to fetch an existing schema or register the given schema with the below conditions
     *  - Checks whether there exists a schema with the given schemaText, and schemaMetadata
     *      - returns respective schemaKey if it exists.
     *      - Creates a schema for the given name and returns respective schemaKey if it does not exist
     * </pre>
     *
     * @param schemaMetadata
     * @param versionedSchema
     * @return
     * @throws InvalidSchemaException
     */
    Integer registerSchema(SchemaMetadata schemaMetadata, VersionedSchema versionedSchema) throws InvalidSchemaException;

    /**
     * Returns {@link SchemaKey} after adding the given schema as the next version of the schema.
     *
     * @param schemaMetadataKey
     * @param versionedSchema
     * @return
     * @throws InvalidSchemaException      if the given schema is not valid.
     * @throws IncompatibleSchemaException if the given schema is incompatible according to the compatibility set.
     */
    Integer addVersionedSchema(SchemaMetadataKey schemaMetadataKey, VersionedSchema versionedSchema) throws InvalidSchemaException, IncompatibleSchemaException;

    /**
     * Returns all schemas registered in the repository. It may be paging the results internally with out realizing all
     * the results.
     *
     * @return
     */
    Iterable<SchemaInfo> listAllSchemas();

    /**
     * Returns {@link SchemaInfo} for the given {@link SchemaKey}
     *
     * @param schemaKey
     * @return
     */
    SchemaInfo getSchema(SchemaKey schemaKey) throws SchemaNotFoundException;


    /**
     * Returns the latest version of the schema for the given {@param schemaMetadataKey}
     *
     * @param schemaMetadataKey
     * @return
     */
    SchemaInfo getLatestSchema(SchemaMetadataKey schemaMetadataKey) throws SchemaNotFoundException;

    /**
     * Returns all versions of the schemas for given {@param schemaMetadataKey}
     *
     * @param schemaMetadataKey
     * @return
     */
    Collection<SchemaInfo> getAllVersions(SchemaMetadataKey schemaMetadataKey) throws SchemaNotFoundException;

    /**
     * Returns true if the given {@code toSchemaText} is compatible with the latest version of the schema with id as {@code schemaMetadataKey}.
     *
     * @param schemaMetadataKey
     * @param toSchemaText
     * @return
     */
    boolean isCompatibleWithAllVersions(SchemaMetadataKey schemaMetadataKey, String toSchemaText) throws SchemaNotFoundException;

    /**
     * Returns unique id for the uploaded bytes read from input stream to file storage.
     *
     * @param inputStream
     * @return
     */
    String uploadFile(InputStream inputStream) throws SerDeException;

    /**
     * Downloads the content of file stored with the given {@code fileId}.
     *
     * @param fileId
     * @return
     */
    InputStream downloadFile(String fileId) throws FileNotFoundException;

    /**
     * Returns unique id for the added Serializer for the given {@code schemaSerializerInfo}
     *
     * @param serializerInfo
     * @return
     */
    Long addSerializer(SerDesInfo serializerInfo);

    /**
     * Returns unique id for the added Serializer for the given {@code schemaSerializerInfo}
     *
     * @param deserializerInfo
     * @return
     */
    Long addDeserializer(SerDesInfo deserializerInfo);

    /**
     * Maps Serializer/Deserializer of the given {@code serDesId} to Schema with {@code schemaMetadataKey}
     *
     * @param schemaMetadataKey
     * @param serDesId
     */
    void mapSchemaWithSerDes(SchemaMetadataKey schemaMetadataKey, Long serDesId);

    /**
     * Returns Collection of Serializers registered for the schema with {@code schemaMetadataKey}
     *
     * @param schemaMetadataKey
     * @return
     */
    Collection<SerDesInfo> getSerializers(SchemaMetadataKey schemaMetadataKey);

    /**
     * Returns Collection of Deserializers registered for the schema with {@code schemaMetadataKey}
     *
     * @param schemaMetadataKey
     * @return
     */
    Collection<SerDesInfo> getDeserializers(SchemaMetadataKey schemaMetadataKey);

    /**
     * Returns a new instance of the respective Serializer class for the given {@code serializerInfo}
     *
     * @param <T>            type of the instance to be created
     * @param serializerInfo
     * @throws SerDeException throws an Exception if serializer or deserializer class is not an instance of {@code T}
     */
    <T> T createSerializerInstance(SerDesInfo serializerInfo);

    /**
     * Returns a new instance of the respective Deserializer class for the given {@code deserializerInfo}
     *
     * @param <T>              type of the instance to be created
     * @param deserializerInfo
     * @throws SerDeException throws an Exception if serializer or deserializer class is not an instance of {@code T}
     */
    <T> T createDeserializerInstance(SerDesInfo deserializerInfo);

}