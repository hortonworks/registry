package com.hortonworks.registries.schemaregistry.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;


import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;

/**
 * Created by pearcem on 19/05/2017.
 */
public class MockSchemaRegistryClient implements ISchemaRegistryClient {
    
    private ISchemaRegistry schemaRegistry;
    
    public MockSchemaRegistryClient(){
        StorageManager storageManager = new InMemoryStorageManager();
        Collection<Map<String, Object>> schemaProvidersConfig = Collections.singleton(Collections.singletonMap("providerClass", AvroSchemaProvider.class.getName()));
        this.schemaRegistry = new DefaultSchemaRegistry(storageManager, null, schemaProvidersConfig);
        this.schemaRegistry.init(Collections.<String, Object>emptyMap());
    }
    
    public MockSchemaRegistryClient(ISchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return schemaRegistry.getRegisteredSchemaProviderInfos();
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        try {
            return schemaRegistry.addSchemaMetadata(schemaMetadata);
        } catch (UnsupportedSchemaTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaRegistry.getSchemaMetadata(schemaName);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaRegistry.getSchemaMetadata(schemaMetadataId);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion)
        throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        try {
            int version =  schemaRegistry.addSchemaVersion(schemaMetadata, schemaVersion.getSchemaText(), schemaMetadata.getDescription());
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(schemaMetadata.getName());
            SchemaIdVersion schemaIdVersion = new SchemaIdVersion(schemaMetadataInfo.getId(), version);
            return schemaIdVersion;
        } catch (UnsupportedSchemaTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaIdVersion uploadSchemaVersion(String schemaName, String description, InputStream schemaVersionTextFile)
        throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion)
        throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        try {
            int version = schemaRegistry.addSchemaVersion(schemaName, schemaVersion.getSchemaText(), schemaVersion.getDescription());
            SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(schemaName);
            SchemaIdVersion schemaIdVersion = new SchemaIdVersion(schemaMetadataInfo.getId(), version);
            return schemaIdVersion;
        } catch (UnsupportedSchemaTypeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
        return schemaRegistry.findSchemasWithFields(schemaFieldQuery);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaRegistry.getSchemaVersionInfo(schemaVersionKey);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return schemaRegistry.getLatestSchemaVersionInfo(schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        return schemaRegistry.findAllVersions(schemaName);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException {
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
        return null;
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {

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

    public <T> T createSerializerInstance(SerDesInfo serDesInfo) {
        return null;
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo serDesInfo) {
        return null;
    }


    @Override
    public void close() throws Exception {
        
    }
}
