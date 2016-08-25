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
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorageManager;
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
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

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
        for (SchemaProvider schemaProvider : schemaProviders) {
            schemaTypeWithProviders.put(schemaProvider.getType(), schemaProvider);
        }
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        SchemaMetadataStorable givenSchemaMetadataStorable = schemaMetadata.toSchemaMetadataStorable();

        Long id;
        synchronized (addOrUpdateLock) {
            Storable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
            if (schemaMetadataStorable != null) {
                id = schemaMetadataStorable.getId();
            } else {
                final Long nextId = storageManager.nextId(givenSchemaMetadataStorable.getNameSpace());
                givenSchemaMetadataStorable.setId(nextId);
                givenSchemaMetadataStorable.setTimestamp(System.currentTimeMillis());
                storageManager.add(givenSchemaMetadataStorable);
                id = givenSchemaMetadataStorable.getId();
            }
        }

        return id;
    }

    @Override
    public Integer addSchema(SchemaMetadata schemaMetadata, VersionedSchema versionedSchema) {

        Integer version;
        // todo handle with minimal lock usage.
        synchronized (addOrUpdateLock) {
            // check whether there exists schema-metadata for schema-metadata-key
            SchemaMetadataKey givenSchemaMetadataKey = schemaMetadata.getSchemaMetadataKey();
            SchemaMetadata schemaMetadataStorable = getSchemaMetadata(givenSchemaMetadataKey);
            if (schemaMetadataStorable != null) {
                // check whether the same schema text exists
                try {
                    version = getSchemaVersion(givenSchemaMetadataKey, versionedSchema.getSchemaText());
                } catch (SchemaNotFoundException e) {
                    version = createSchemaInfo(schemaMetadataStorable.getId(), versionedSchema);
                }
            } else {
                Long schemaMetadataId = addSchemaMetadata(schemaMetadata);
                version = createSchemaInfo(schemaMetadataId, versionedSchema);
            }
        }

        return version;
    }

    public Integer addSchema(SchemaMetadataKey schemaMetadataKey, VersionedSchema versionedSchema) throws SchemaNotFoundException {

        Integer version;
        // todo handle with minimal lock usage.
        synchronized (addOrUpdateLock) {
            // check whether there exists schema-metadata for schema-metadata-key
            SchemaMetadata schemaMetadataStorable = getSchemaMetadata(schemaMetadataKey);
            if (schemaMetadataStorable != null) {
                // check whether the same schema text exists
                version = findSchemaVersion(schemaMetadataKey.getType(), versionedSchema.getSchemaText(), schemaMetadataStorable.getId());
                if (version == null) {
                    version = createSchemaInfo(schemaMetadataStorable.getId(), versionedSchema);
                }
            } else {
                throw new SchemaNotFoundException("Schema not found with the given schemaMetadataKey: " + schemaMetadataKey);
            }
        }

        return version;
    }

    private Integer createSchemaInfo(Long schemaMetadataId, VersionedSchema versionedSchema) {

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable();
        final Long nextId = storageManager.nextId(schemaInfoStorable.getNameSpace());
        schemaInfoStorable.setSchemaMetadataId(schemaMetadataId);
        schemaInfoStorable.setSchemaText(versionedSchema.getSchemaText());
        schemaInfoStorable.setDescription(versionedSchema.getDescription());
        schemaInfoStorable.setId(nextId);
        schemaInfoStorable.setTimestamp(System.currentTimeMillis());

        //todo fix this by generating version sequence for each schema in storage layer or explore other ways to make it scalable
        synchronized (addOrUpdateLock) {
            Collection<SchemaInfo> schemaInfos = findAllVersions(schemaMetadataId);
            Integer version = 0;
            if (schemaInfos != null && !schemaInfos.isEmpty()) {
                for (SchemaInfo schemaInfo : schemaInfos) {
                    version = Math.max(schemaInfo.getVersion(), version);
                }
            }
            schemaInfoStorable.setVersion(version + 1);
            schemaInfoStorable.setFingerprint(getFingerprint(getSchemaMetadata(schemaMetadataId).getSchemaMetadataKey().getType(), schemaInfoStorable.getSchemaText()));

            storageManager.add(schemaInfoStorable);
        }

        return schemaInfoStorable.getVersion();
    }

    @Override
    public SchemaMetadata getSchemaMetadata(final Long schemaMetadataId) {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaMetadataStorable.ID, schemaMetadataId.toString()));
        SchemaMetadataStorable schemaMetadataStorable = getSchemaMetadataStorable(queryParams);
        return schemaMetadataStorable != null ? SchemaMetadata.fromSchemaMetadataStorable(schemaMetadataStorable) : null;
    }

    @Override
    public SchemaMetadata getSchemaMetadata(SchemaMetadataKey schemaMetadataKey) {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setType(schemaMetadataKey.getType());
        schemaMetadataStorable.setDataSourceGroup(schemaMetadataKey.getDataSourceGroup());
        schemaMetadataStorable.setName(schemaMetadataKey.getName());

        SchemaMetadataStorable schemaMetadataStorable1 = storageManager.get(schemaMetadataStorable.getStorableKey());
        return schemaMetadataStorable1 != null ? SchemaMetadata.fromSchemaMetadataStorable(schemaMetadataStorable1) : null;
    }

    @Override
    public Collection<SchemaInfo> listAll() {
        Collection<SchemaInfoStorable> storables = storageManager.list(SchemaInfoStorable.NAME_SPACE);
        return storables != null && !storables.isEmpty()
                ? storables.stream().map(schemaInfoStorable -> new SchemaInfo(schemaInfoStorable)).collect(Collectors.toList())
                : Collections.emptyList();
    }

    @Override
    public Collection<SchemaInfo> findAllVersions(final Long schemaMetadataId) {
        List<QueryParam> queryParams =
                Collections.singletonList(new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()));
        Collection<SchemaInfoStorable> storables = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);

        return (storables != null && !storables.isEmpty())
                ? storables.stream().map(schemaInfoStorable -> new SchemaInfo(schemaInfoStorable)).collect(Collectors.toList())
                : Collections.emptyList();
    }

    @Override
    public Integer getSchemaVersion(SchemaMetadataKey schemaMetadataKey, String schemaText) throws SchemaNotFoundException {
        SchemaMetadata schemaMetadata = getSchemaMetadata(schemaMetadataKey);
        if (schemaMetadata == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaMetadataKey);
        }

        Long schemaMetadataId = schemaMetadata.getId();

        Integer result = findSchemaVersion(schemaMetadataKey.getType(), schemaText, schemaMetadataId);

        if (result == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaMetadataKey);
        }

        return result;
    }

    private Integer findSchemaVersion(String type, String schemaText, Long schemaMetadataId) {
        String fingerPrint = getFingerprint(type, schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                new QueryParam(SchemaInfoStorable.FINGERPRINT, fingerPrint));

        Collection<SchemaInfoStorable> versionedSchemas = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);

        Integer result = null;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataId: [{}] and schemaText [{}]", schemaMetadataId, schemaText);
            }

            SchemaInfoStorable schemaInfoStorable = versionedSchemas.iterator().next();
            result = schemaInfoStorable.getVersion();
        }

        return result;
    }

    private String getFingerprint(String type, String schemaText) {
        return Hex.encodeHexString(schemaTypeWithProviders.get(type).getFingerPrint(schemaText));
    }

    @Override
    public SchemaInfo getSchemaInfo(final Long schemaMetadataId, Integer version) throws SchemaNotFoundException {

        SchemaMetadata schemaMetadata = getSchemaMetadata(schemaMetadataId);

        if (schemaMetadata != null) {
            List<QueryParam> queryParams = Lists.newArrayList(
                    new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                    new QueryParam(SchemaInfoStorable.VERSION, version.toString()));

            Collection<SchemaInfoStorable> versionedSchemas = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                LOG.warn("More than one schema exists with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                return new SchemaInfo(versionedSchemas.iterator().next());
            } else {
                throw new SchemaNotFoundException("No Schema version exists with schemaMetadataId " + schemaMetadataId + " and version " + version);
            }
        }

        throw new SchemaNotFoundException("No SchemaMetadata exists with id " + schemaMetadataId);
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
    public SchemaInfo getLatestSchemaInfo(Long schemaMetadataId) throws SchemaNotFoundException {
        Collection<SchemaInfo> schemaInfos = findAllVersions(schemaMetadataId);

        SchemaInfo latestSchema = null;
        if (schemaInfos != null && !schemaInfos.isEmpty()) {
            Integer curVersion = Integer.MIN_VALUE;
            for (SchemaInfo schemaInfo : schemaInfos) {
                if (schemaInfo.getVersion() > curVersion) {
                    latestSchema = schemaInfo;
                    curVersion = schemaInfo.getVersion();
                }
            }
        }

        return latestSchema;
    }

    public boolean isCompatible(Long schemaMetadataId, String toSchema) throws SchemaNotFoundException {
        Collection<SchemaInfo> existingSchemaInfoStorable = findAllVersions(schemaMetadataId);
        Collection<String> schemaTexts =
                existingSchemaInfoStorable.stream()
                        .map(schemaInfoStorable -> schemaInfoStorable.getSchemaText()).collect(Collectors.toList());

        SchemaMetadata schemaMetadata = getSchemaMetadata(schemaMetadataId);

        return isCompatible(schemaMetadata.getSchemaMetadataKey().getType(), toSchema, schemaTexts, schemaMetadata.getCompatibility());
    }

    public boolean isCompatible(Long schemaMetadataId,
                                Integer version,
                                String toSchema) throws SchemaNotFoundException {
        SchemaInfo existingSchemaInfo = getSchemaInfo(schemaMetadataId, version);
        String schemaText = existingSchemaInfo.getSchemaText();
        SchemaMetadata schemaMetadata = getSchemaMetadata(schemaMetadataId);

        return isCompatible(schemaMetadata.getSchemaMetadataKey().getType(), toSchema, Collections.singletonList(schemaText), schemaMetadata.getCompatibility());
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
    public Long addSerDesInfo(SerDesInfo serDesInfo) {
        SerDesInfoStorable serDesInfoStorable = SerDesInfoStorable.fromSerDesInfo(serDesInfo);
        Long nextId = storageManager.nextId(serDesInfoStorable.getNameSpace());
        serDesInfoStorable.setId(nextId);
        serDesInfoStorable.setTimestamp(System.currentTimeMillis());
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
        List<SerDesInfo> serializerInfos = getSerDesInfos(schemaMetadataId, true);

        return serializerInfos;
    }

    private Collection<SchemaSerDesMapping> getSchemaSerDesMappings(Long schemaMetadataId) {
        List<QueryParam> queryParams =
                Collections.singletonList(new QueryParam(SchemaSerDesMapping.SCHEMA_METADATA_ID, schemaMetadataId.toString()));

        return storageManager.find(SchemaSerDesMapping.NAMESPACE, queryParams);
    }

    @Override
    public Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId) {
        List<SerDesInfo> SerDesInfos = getSerDesInfos(schemaMetadataId, false);

        return SerDesInfos;
    }

    private List<SerDesInfo> getSerDesInfos(Long schemaMetadataId, boolean isSerializer) {
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(schemaMetadataId);
        List<SerDesInfo> serDesInfos;
        if (schemaSerDesMappings == null || schemaSerDesMappings.isEmpty()) {
            serDesInfos = Collections.emptyList();
        } else {
            serDesInfos = new ArrayList<>();
            for (SchemaSerDesMapping schemaSerDesMapping : schemaSerDesMappings) {
                SerDesInfo serDesInfo = getSerDesInfo(schemaSerDesMapping.getSerDesId());
                if((isSerializer && serDesInfo.getIsSerializer())
                        || !serDesInfo.getIsSerializer()) {
                    serDesInfos.add(serDesInfo);
                }
            }
        }
        return serDesInfos;
    }

    @Override
    public InputStream downloadJar(Long serDesId) {
        SerDesInfo serDesInfoStorable = getSerDesInfo(serDesId);
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

        SchemaSerDesMapping schemaSerDesMapping = new SchemaSerDesMapping(schemaMetadataId, serDesId);
        storageManager.add(schemaSerDesMapping);
    }

}