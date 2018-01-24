/*
 * Copyright 2016 Hortonworks.
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
 */
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.HAServerNotificationManager;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MockSchemaRegistryClient implements ISchemaRegistryClient {

    private ISchemaRegistry schemaRegistry;

    public MockSchemaRegistryClient() {
        StorageManager storageManager = new InMemoryStorageManager();
        Collection<Map<String, Object>> schemaProvidersConfig = Collections.singleton(Collections.singletonMap("providerClass", AvroSchemaProvider.class.getName()));
        this.schemaRegistry = new DefaultSchemaRegistry(storageManager, null, schemaProvidersConfig, new HAServerNotificationManager());
        this.schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    public MockSchemaRegistryClient(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return schemaRegistry.getSupportedSchemaProviders();
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        try {
            return schemaRegistry.registerSchemaMetadata(schemaMetadata);
        } catch (UnsupportedSchemaTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        return schemaRegistry.addSchemaMetadata(schemaMetadata);
    }

    @Override
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        return schemaRegistry.updateSchemaMetadata(schemaName, schemaMetadata);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaRegistry.getSchemaMetadataInfo(schemaName);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaRegistry.getSchemaMetadataInfo(schemaMetadataId);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        try {

           return schemaRegistry.addSchemaVersion(schemaMetadata,
                                                          new SchemaVersion(schemaVersion.getSchemaText(),
                                                                            schemaMetadata.getDescription()));
        } catch (UnsupportedSchemaTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.addSchemaVersion(schemaBranchName, schemaMetadata, schemaVersion);
    }

    @Override
    public SchemaIdVersion uploadSchemaVersion(String schemaName, String description, InputStream schemaVersionTextFile)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        try {

            return schemaRegistry.addSchemaVersion(schemaName,
                                                          new SchemaVersion(schemaVersion.getSchemaText(),
                                                                            schemaVersion.getDescription()));
        } catch (UnsupportedSchemaTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName, String schemaName, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaRegistry.deleteSchemaVersion(schemaVersionKey);
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) throws SchemaBranchNotFoundException, SchemaNotFoundException {
        return schemaRegistry.findSchemasByFields(schemaFieldQuery);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return schemaRegistry.getSchemaVersionInfo(schemaIdVersion);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return schemaRegistry.getLatestSchemaVersionInfo(schemaName);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.getLatestSchemaVersionInfo(schemaBranchName, schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        return schemaRegistry.getAllVersions(schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.getAllVersions(schemaBranchName, schemaName);
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.checkCompatibility(schemaName, toSchemaText);
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.checkCompatibility(schemaBranchName, schemaName, toSchemaText);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.checkCompatibility(schemaName, toSchemaText).isCompatible();
    }

    @Override
    public String uploadFile(InputStream inputStream) throws SerDesException {
        return schemaRegistry.uploadFile(inputStream);
    }

    @Override
    public InputStream downloadFile(String fileId) throws FileNotFoundException {
        try {
            return schemaRegistry.downloadFile(fileId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long addSerDes(SerDesPair serializerInfo) {
        return schemaRegistry.addSerDes(serializerInfo);
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        schemaRegistry.mapSchemaWithSerDes(schemaName, serDesId);
    }

    @Override
    public SchemaIdVersion uploadSchemaVersion(String schemaBranchName, String schemaName, String description, InputStream schemaVersionTextFile) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return null;
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaBranchName, String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return false;
    }

    @Override
    public <T> T getDefaultSerializer(String type) throws SerDesException {
        return null;
    }

    @Override
    public <T> T getDefaultDeserializer(String type) throws SerDesException {
        return null;
    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        return null;
    }

    @Override
    public void transitionState(Long schemaVersionId,
                                Byte targetStateId,
                                byte [] transitionDetails) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaRegistry.transitionState(schemaVersionId, targetStateId, transitionDetails);
    }

    @Override
    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return schemaRegistry.getSchemaVersionLifecycleStateMachineInfo();
    }

    @Override
    public SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch) throws SchemaBranchAlreadyExistsException, SchemaNotFoundException {
       return schemaRegistry.createSchemaBranch(schemaVersionId, schemaBranch);
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException {
        return schemaRegistry.getSchemaBranches(schemaName);
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {
        schemaRegistry.deleteSchemaBranch(schemaBranchId);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaRegistry.getAllVersions(schemaBranchName, schemaName, stateIds);
    }

    public <T> T createSerializerInstance(SerDesInfo serDesInfo) {
        return null;
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo serDesInfo) {
        return null;
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, IncompatibleSchemaException {
         return schemaRegistry.mergeSchemaVersion(schemaVersionId, SchemaVersionMergeStrategy.OPTIMISTIC);
    }


    @Override
    public void close() throws Exception {

    }
}
