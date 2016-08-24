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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hortonworks.iotas.common.QueryParam;
import com.hortonworks.iotas.common.util.FileStorage;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.VersionedSchema;
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Default implementation for schema registry.
 * <p>
 * Remove todos with respective JIRAs created
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final Collection<? extends SchemaProvider> schemaProviders;
    private final Map<String, SchemaProvider> schemaTypeWithProviders = new HashMap<>();
    private final Object addOrUpdateLock = new Object();

    public DefaultSchemaRegistry(StorageManager storageManager, FileStorage fileStorage, Collection<? extends SchemaProvider> schemaProviders) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaProviders = schemaProviders;
    }

    @Override
    public void init(Map<String, Object> props) {
        // initialize storage manager
        storageManager.init(props);
        for (SchemaProvider schemaProvider : schemaProviders) {
            schemaTypeWithProviders.put(schemaProvider.getType(), schemaProvider);
        }
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        SchemaMetadataStorable schemaMetadataStorable = getOrCreateSchemaMetadata(schemaMetadata);
        return schemaMetadataStorable.getId();
    }

    @Override
    public Integer addSchema(SchemaMetadata schemaMetadata, VersionedSchema versionedSchema) {

        Integer version;
        // todo handle without using lock.
        synchronized (addOrUpdateLock) {
            // check whether there exists schema-metadata for schema-metadata-key
            SchemaMetadataKey givenSchemaMetadataKey = schemaMetadata.getSchemaMetadataKey();
            SchemaMetadataStorable schemaMetadataStorable = getSchemaMetadata(givenSchemaMetadataKey);
            if (schemaMetadataStorable != null) {
                // check whether the same schema text exists
                try {
                    version = getSchemaVersion(givenSchemaMetadataKey, versionedSchema.getSchemaText());
                } catch (SchemaNotFoundException e) {
                    version = createSchemaInfo(versionedSchema, schemaMetadataStorable.getId());
                }
            } else {
                Long schemaMetadataId = addSchemaMetadata(schemaMetadata);
                version = createSchemaInfo(versionedSchema, schemaMetadataId);
            }
        }

        return version;
    }

    public Integer addSchema(SchemaMetadataKey schemaMetadataKey, VersionedSchema versionedSchema) throws SchemaNotFoundException {

        Integer version = null;
        // todo handle without using lock.
        synchronized (addOrUpdateLock) {
            // check whether there exists schema-metadata for schema-metadata-key
            SchemaMetadataStorable schemaMetadataStorable = getSchemaMetadata(schemaMetadataKey);
            if (schemaMetadataStorable != null) {
                // check whether the same schema text exists
                version = getSchemaVersion(schemaMetadataKey, versionedSchema.getSchemaText());
                if (version == null) {
                    version = createSchemaInfo(versionedSchema, schemaMetadataStorable.getId());
                }
            } else {
                throw new SchemaNotFoundException("Schema not found with the given schemaMetadataKey: " + schemaMetadataKey);
            }
        }

        return version;
    }

    private Integer createSchemaInfo(VersionedSchema versionedSchema, Long schemaMetadataId) {

        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable();
        schemaInfoStorable.setSchemaMetadataId(schemaMetadataId);
        schemaInfoStorable.setSchemaText(versionedSchema.getSchemaText());
        schemaInfoStorable.setDescription(versionedSchema.getDescription());

        return addSchemaInfo(schemaInfoStorable);
    }

    @Override
    public SchemaMetadataStorable getSchemaMetadata(final Long schemaMetadataId) {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaMetadataStorable.ID, schemaMetadataId.toString()));
        return getSchemaMetadataStorable(queryParams);
    }

    @Override
    public SchemaMetadataStorable getSchemaMetadata(SchemaMetadataKey schemaMetadataKey) {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setType(schemaMetadataKey.getType());
        schemaMetadataStorable.setGroup(schemaMetadataKey.getGroup());
        schemaMetadataStorable.setName(schemaMetadataKey.getName());

        return storageManager.get(schemaMetadataStorable.getStorableKey());
    }

    private Integer addSchemaInfo(SchemaInfoStorable givenSchemaInfoStorable) {
        Long schemaMetadataId = givenSchemaInfoStorable.getSchemaMetadataId();

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        Long id = givenSchemaInfoStorable.getId();
        if (id != null) {
            LOG.info("Received ID [{}] in SchemaInfo instance is ignored", id);
        }
        final Long nextId = storageManager.nextId(givenSchemaInfoStorable.getNameSpace());
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable(givenSchemaInfoStorable);
        schemaInfoStorable.setId(nextId);
        schemaInfoStorable.setTimestamp(System.currentTimeMillis());

        //todo fix this by generating version sequence for each schema in storage layer or explore other ways to make it scalable
        synchronized (addOrUpdateLock) {
            Collection<SchemaInfoStorable> schemaInfoStorables = findAllVersions(schemaMetadataId);
            Integer version = 0;
            if (schemaInfoStorables != null && !schemaInfoStorables.isEmpty()) {
                for (SchemaInfoStorable schema : schemaInfoStorables) {
                    version = Math.max(schema.getVersion(), version);
                }
            }
            schemaInfoStorable.setVersion(version + 1);
            schemaInfoStorable.setFingerprint(getFingerprint(getSchemaMetadata(schemaMetadataId).getType(), schemaInfoStorable.getSchemaText()));
            storageManager.add(schemaInfoStorable);
        }

        return schemaInfoStorable.getVersion();
    }

    @Override
    public Collection<SchemaInfoStorable> listAll() {
        return storageManager.list(SchemaInfoStorable.NAME_SPACE);
    }

    @Override
    public Collection<SchemaInfoStorable> findAllVersions(final Long schemaMetadataId) {
        SchemaMetadataStorable schemaMetadataStorable1 = new SchemaMetadataStorable();
        schemaMetadataStorable1.setId(schemaMetadataId);

        SchemaMetadataStorable schemaMetadataStorable = getSchemaMetadata(schemaMetadataId);

        Collection<SchemaInfoStorable> result = null;
        if (schemaMetadataStorable == null) {
            result = Collections.emptyList();
        } else {
            List<QueryParam> queryParams =
                    Collections.singletonList(new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID,
                            schemaMetadataStorable.getId().toString()));
            result = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);
        }

        return result;
    }

    @Override
    public Integer getSchemaVersion(SchemaMetadataKey schemaMetadataKey, String schemaText) throws SchemaNotFoundException {
        SchemaMetadataStorable schemaMetadataStorable = getSchemaMetadata(schemaMetadataKey);
        if (schemaMetadataStorable == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaMetadataKey);
        }

        String fingerPrint = getFingerprint(schemaMetadataKey.getType(), schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID, schemaMetadataStorable.getId().toString()),
                new QueryParam(SchemaInfoStorable.FINGERPRINT, fingerPrint));

        Collection<SchemaInfoStorable> versionedSchemas = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);

        Integer result;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataKey: [{}] and schemaText [{}]", schemaMetadataKey, schemaText);
            }

            SchemaInfoStorable schemaInfoStorable = versionedSchemas.iterator().next();
            result = schemaInfoStorable.getVersion();
        } else {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaMetadataKey);
        }

        return result;
    }

    private String getFingerprint(String type, String schemaText) {
        return Hex.encodeHexString(schemaTypeWithProviders.get(type).getFingerPrint(schemaText));
    }

    @Override
    public SchemaInfoStorable getSchemaInfo(final Long schemaMetadataId, Integer version) {

        SchemaMetadataStorable schemaMetadataStorable = getSchemaMetadata(schemaMetadataId);

        SchemaInfoStorable result = null;
        if (schemaMetadataStorable != null) {
            List<QueryParam> queryParams = new ArrayList<>();
            queryParams.add(new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID,
                    schemaMetadataStorable.getId().toString()));
            queryParams.add(new QueryParam(SchemaInfoStorable.VERSION, version.toString()));
            Collection<SchemaInfoStorable> versionedSchemas = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                LOG.warn("Exists more than one schema with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                result = versionedSchemas.iterator().next();
            }
        }

        return result;
    }

    private SchemaMetadataStorable getSchemaMetadataStorable(List<QueryParam> queryParams) {
        Collection<SchemaMetadataStorable> schemaMetadataStorables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, queryParams);
        SchemaMetadataStorable schemaMetadataStorable = null;
        if (schemaMetadataStorables != null && !schemaMetadataStorables.isEmpty()) {
            if (schemaMetadataStorables.size() > 1) {
                LOG.warn("Received more than one schema with query parameters [{}]", queryParams);
            }
            schemaMetadataStorable = schemaMetadataStorables.iterator().next();
            LOG.debug("Schema found in registry with query parameters [{}]", queryParams);
        } else {
            LOG.debug("No schemas found in registry with query parameters [{}]", queryParams);
        }

        return schemaMetadataStorable;
    }

    @Override
    public SchemaInfoStorable getLatestSchemaInfo(Long schemaMetadataId) {
        Collection<SchemaInfoStorable> schemaInfoStorables = findAllVersions(schemaMetadataId);

        SchemaInfoStorable latestSchema = null;
        if (schemaInfoStorables != null && !schemaInfoStorables.isEmpty()) {
            Integer curVersion = Integer.MIN_VALUE;
            for (SchemaInfoStorable schemaInfoStorable : schemaInfoStorables) {
                if (schemaInfoStorable.getVersion() > curVersion) {
                    latestSchema = schemaInfoStorable;
                    curVersion = schemaInfoStorable.getVersion();
                }
            }
        }

        return latestSchema;
    }

    public boolean isCompatible(Long schemaMetadataId, String toSchema) throws SchemaNotFoundException {
        Collection<SchemaInfoStorable> existingSchemaInfoStorable = findAllVersions(schemaMetadataId);
        Collection<String> schemaTexts =
                existingSchemaInfoStorable.stream()
                        .map(schemaInfoStorable -> schemaInfoStorable.getSchemaText()).collect(Collectors.toList());

        SchemaMetadataStorable schemaMetadata = getSchemaMetadata(schemaMetadataId);

        return isCompatible(schemaMetadata.getType(), toSchema, schemaTexts, schemaMetadata.getCompatibility());
    }

    public boolean isCompatible(Long schemaMetadataId,
                                Integer version,
                                String toSchema) throws SchemaNotFoundException {
        SchemaInfoStorable existingSchemaInfoStorable = getSchemaInfo(schemaMetadataId, version);
        String schemaText = existingSchemaInfoStorable.getSchemaText();
        SchemaMetadataStorable schemaMetadata = getSchemaMetadata(schemaMetadataId);

        return isCompatible(schemaMetadata.getType(), toSchema, Collections.singletonList(schemaText), schemaMetadata.getCompatibility());
    }

    private boolean isCompatible(String type,
                                 String toSchema,
                                 Collection<String> existingSchemas,
                                 SchemaProvider.Compatibility compatibility) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.isCompatible(toSchema, existingSchemas, compatibility);
    }

    public Collection<SchemaInfoStorable> getCompatibleSchemas(Long schemaMetadataId,
                                                               SchemaProvider.Compatibility compatibility,
                                                               String toSchema)
            throws SchemaNotFoundException {
        String type = getSchemaMetadata(schemaMetadataId).getType();
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        List<SchemaInfoStorable> supportedSchemas = new ArrayList<>();
        Collection<SchemaInfoStorable> schemaInfoStorables = findAllVersions(schemaMetadataId);
        for (SchemaInfoStorable schemaInfoStorable : schemaInfoStorables) {
            if (schemaProvider.isCompatible(toSchema, schemaInfoStorable.getSchemaText(), compatibility)) {
                supportedSchemas.add(schemaInfoStorable);
            }
        }

        return supportedSchemas;
    }

    @Override
    public SchemaMetadataStorable getOrCreateSchemaMetadata(SchemaMetadata schemaMetadata) {
        SchemaMetadataStorable givenSchemaMetadataStorable = schemaMetadata.schemaMetadataStorable();

        SchemaMetadataStorable schemaMetadataStorable;
        synchronized (addOrUpdateLock) {
            schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
            if (schemaMetadataStorable == null) {
                final Long nextId = storageManager.nextId(givenSchemaMetadataStorable.getNameSpace());
                givenSchemaMetadataStorable.setId(nextId);
                givenSchemaMetadataStorable.setTimestamp(System.currentTimeMillis());
                storageManager.add(givenSchemaMetadataStorable);
                schemaMetadataStorable = givenSchemaMetadataStorable;
            }
        }

        return schemaMetadataStorable;
    }

    @Override
    public String uploadFile(InputStream inputStream) {
        String fileName = UUID.randomUUID().toString();
        try {
            String uploadedFilePath = fileStorage.uploadFile(inputStream, fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return fileName;
    }

    @Override
    public InputStream downloadFile(String fileId) throws IOException {
        return fileStorage.downloadFile(fileId);
    }

    @Override
    public Long addSerDesInfo(SerDesInfoStorable serDesInfoStorable) {
        Long nextId = storageManager.nextId(serDesInfoStorable.getNameSpace());
        serDesInfoStorable.setId(nextId);
        storageManager.add(serDesInfoStorable);

        return nextId;
    }

    @Override
    public SerDesInfo getSerDesInfo(Long serDesId) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable();
        serDesInfoStorable.setId(serDesId);
        return ((SerDesInfoStorable) storageManager.get(serDesInfoStorable.getStorableKey())).toSerDesInfo();
    }

    @Override
    public Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId) {
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(schemaMetadataId);
        List<SerDesInfo> serializerInfos = null;
        if (schemaSerDesMappings == null || schemaSerDesMappings.isEmpty()) {
            serializerInfos = Collections.emptyList();
        } else {
            serializerInfos = new ArrayList<>();
            for (SchemaSerDesMapping schemaSerDesMapping : schemaSerDesMappings) {
                serializerInfos.add(getSerDesInfo(schemaSerDesMapping.getSerDesId()));
            }
        }

        return serializerInfos;
    }

    private Collection<SchemaSerDesMapping> getSchemaSerDesMappings(Long schemaMetadataId) {
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaSerDesMapping.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                new QueryParam(SchemaSerDesMapping.IS_SERIALIZER, Boolean.TRUE.toString()));

        return storageManager.find(SchemaSerDesMapping.NAMESPACE, queryParams);
    }

    @Override
    public Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId) {
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(schemaMetadataId);
        List<SerDesInfo> SerDesInfos = null;
        if (schemaSerDesMappings == null || schemaSerDesMappings.isEmpty()) {
            SerDesInfos = Collections.emptyList();
        } else {
            SerDesInfos = new ArrayList<>();
            for (SchemaSerDesMapping schemaSerDesMapping : schemaSerDesMappings) {
                SerDesInfos.add((SerDesInfo) getSerDesInfo(schemaSerDesMapping.getSerDesId()));
            }
        }

        return SerDesInfos;
    }

    @Override
    public InputStream downloadJar(Long serializerId) {
        SerDesInfo serDesInfoStorable = getSerDesInfo(serializerId);
        try {
            return fileStorage.downloadFile(serDesInfoStorable.getFileId());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void mapSerDesWithSchema(Long schemaMetadataId, Long serDesId) {
        SerDesInfo serDesInfo = getSerDesInfo(serDesId);
        if (serDesInfo == null) {
            throw new SerDeException("Serializer with given ID " + serDesId + " does not exist");
        }

        SchemaSerDesMapping schemaSerDesMapping = new SchemaSerDesMapping(schemaMetadataId, serDesId, serDesInfo.getIsSerializer());
        storageManager.add(schemaSerDesMapping);
    }

}