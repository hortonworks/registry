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
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
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
    public Long addSchema(SchemaInfo schemaInfo) {
        SchemaInfoStorable givenSchemaInfoStorable = schemaInfo.toSchemaMetadataStorable();

        Long id;
        synchronized (addOrUpdateLock) {
            Storable schemaMetadataStorable = storageManager.get(givenSchemaInfoStorable.getStorableKey());
            if (schemaMetadataStorable != null) {
                id = schemaMetadataStorable.getId();
            } else {
                final Long nextId = storageManager.nextId(givenSchemaInfoStorable.getNameSpace());
                givenSchemaInfoStorable.setId(nextId);
                givenSchemaInfoStorable.setTimestamp(System.currentTimeMillis());
                storageManager.add(givenSchemaInfoStorable);
                id = givenSchemaInfoStorable.getId();
            }
        }

        return id;
    }

    @Override
    public Integer addSchemaVersion(SchemaInfo schemaInfo, SchemaVersion schemaVersion) throws IncompatibleSchemaException {

        Integer version;
        // todo handle with minimal lock usage.
        synchronized (addOrUpdateLock) {
            // check whether there exists schema-metadata for schema-metadata-key
            SchemaKey givenSchemaKey = schemaInfo.getSchemaKey();
            SchemaInfo retreivedschemaInfo = getSchema(givenSchemaKey);
            if (retreivedschemaInfo != null) {
                // check whether the same schema text exists
                try {
                    version = getSchemaVersion(givenSchemaKey, schemaVersion.getSchemaText());
                } catch (SchemaNotFoundException e) {
                    version = createSchemaInfo(retreivedschemaInfo.getId(), schemaVersion, givenSchemaKey);
                }
            } else {
                Long schemaMetadataId = addSchema(schemaInfo);
                version = createSchemaInfo(schemaMetadataId, schemaVersion, givenSchemaKey);
            }
        }

        return version;
    }

    public Integer adSchemaVersion(SchemaKey schemaKey, SchemaVersion schemaVersion) throws SchemaNotFoundException, IncompatibleSchemaException {

        Integer version;
        // todo handle with minimal lock usage.
        synchronized (addOrUpdateLock) {
            // check whether there exists schema-metadata for schema-metadata-key
            SchemaInfo schemaInfoStorable = getSchema(schemaKey);
            if (schemaInfoStorable != null) {
                // check whether the same schema text exists
                version = findSchemaVersion(schemaKey.getType(), schemaVersion.getSchemaText(), schemaInfoStorable.getId());
                if (version == null) {
                    version = createSchemaInfo(schemaInfoStorable.getId(), schemaVersion, schemaKey);
                }
            } else {
                throw new SchemaNotFoundException("Schema not found with the given schemaMetadataKey: " + schemaKey);
            }
        }

        return version;
    }

    private Integer createSchemaInfo(Long schemaMetadataId, SchemaVersion schemaVersion, SchemaKey schemaKey) throws IncompatibleSchemaException {

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        final Long schemaInstanceId = storageManager.nextId(schemaVersionStorable.getNameSpace());
        schemaVersionStorable.setId(schemaInstanceId);
        schemaVersionStorable.setSchemaMetadataId(schemaMetadataId);

        String type = schemaKey.getType();
        schemaVersionStorable.setType(type);
        schemaVersionStorable.setSchemaGroup(schemaKey.getSchemaGroup());
        schemaVersionStorable.setName(schemaKey.getName());

        schemaVersionStorable.setSchemaText(schemaVersion.getSchemaText());
        schemaVersionStorable.setDescription(schemaVersion.getDescription());
        schemaVersionStorable.setTimestamp(System.currentTimeMillis());

        //todo fix this by generating version sequence for each schema in storage layer or explore other ways to make it scalable
        synchronized (addOrUpdateLock) {
            Collection<SchemaVersionInfo> schemaVersionInfos = findAllVersions(schemaKey);
            Integer version = 0;
            SchemaVersionInfo latestSchemaVersionInfo = null;
            if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
                for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                    if(schemaVersionInfo.getVersion() > version) {
                        latestSchemaVersionInfo = schemaVersionInfo;
                    }
                }
                version = latestSchemaVersionInfo.getVersion();
                // check for compatibility
                SchemaInfo schemaInfo = getSchema(schemaKey);
                if(!schemaTypeWithProviders.get(type).isCompatible(schemaVersion.getSchemaText(), latestSchemaVersionInfo.getSchemaText(), schemaInfo.getCompatibility())) {
                    throw new IncompatibleSchemaException("Given schema is not compatible with earlier schemas");
                }
            }
            schemaVersionStorable.setVersion(version + 1);
            schemaVersionStorable.setFingerprint(getFingerprint(type, schemaVersionStorable.getSchemaText()));

            storageManager.add(schemaVersionStorable);
            String storableNamespace = new SchemaFieldInfoStorable().getNameSpace();
            List<SchemaFieldInfo> schemaFieldInfos = schemaTypeWithProviders.get(type).generateFields(schemaVersionStorable.getSchemaText());
            for (SchemaFieldInfo schemaFieldInfo : schemaFieldInfos) {
                final Long fieldInstanceId = storageManager.nextId(storableNamespace);
                SchemaFieldInfoStorable schemaFieldInfoStorable = schemaFieldInfo.toFieldInfoStorable(fieldInstanceId);
                schemaFieldInfoStorable.setSchemaInstanceId(schemaInstanceId);
                schemaFieldInfoStorable.setTimestamp(System.currentTimeMillis());
                storageManager.add(schemaFieldInfoStorable);
            }
        }

        return schemaVersionStorable.getVersion();
    }

    @Override
    public SchemaInfo getSchema(SchemaKey schemaKey) {
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable();
        schemaInfoStorable.setType(schemaKey.getType());
        schemaInfoStorable.setSchemaGroup(schemaKey.getSchemaGroup());
        schemaInfoStorable.setName(schemaKey.getName());

        SchemaInfoStorable schemaInfoStorable1 = storageManager.get(schemaInfoStorable.getStorableKey());
        return schemaInfoStorable1 != null ? SchemaInfo.fromSchemaMetadataStorable(schemaInfoStorable1) : null;
    }

    @Override
    public Collection<SchemaVersionKey> findSchemas(Map<String, String> filters) {
        // todo get only few selected columns instead of getting the whole row.
        Collection<SchemaVersionStorable> storables;

        if (filters == null || filters.isEmpty()) {
            storables = storageManager.list(SchemaVersionStorable.NAME_SPACE);
        } else {
            List<QueryParam> queryParams =
                    filters.entrySet()
                            .stream()
                            .map(entry -> new QueryParam(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList());
            storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
        }

        return storables != null && !storables.isEmpty()
                ? storables.stream().map(schemaVersionStorable -> getSchemaKey(schemaVersionStorable)).collect(Collectors.toList())
                : Collections.emptyList();
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasWithFields(SchemaFieldQuery schemaFieldQuery) {
        List<QueryParam> queryParams = buildQueryParam(schemaFieldQuery);

        Collection<SchemaFieldInfoStorable> fieldInfos = storageManager.find(SchemaFieldInfoStorable.STORABLE_NAME_SPACE, queryParams);
        Collection<SchemaVersionKey> schemaVersionKeys;
        if (fieldInfos != null && !fieldInfos.isEmpty()) {
            List<Long> schemaIds = fieldInfos.stream()
                    .map(schemaFieldInfoStorable -> schemaFieldInfoStorable.getSchemaInstanceId())
                    .collect(Collectors.toList());

            // todo get only few selected columns instead of getting the whole row.
            // add OR query to find items from store
            schemaVersionKeys = new ArrayList<>();
            for (Long schemaId : schemaIds) {
                SchemaVersionKey schemaVersionKey = getSchemaKey(schemaId);
                if (schemaVersionKey != null) {
                    schemaVersionKeys.add(schemaVersionKey);
                }
            }
        } else {
            schemaVersionKeys = Collections.emptyList();
        }

        return schemaVersionKeys;
    }

    private SchemaVersionKey getSchemaKey(Long schemaId) {
        SchemaVersionKey schemaVersionKey = null;

        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.ID, schemaId.toString()));
        Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            SchemaVersionStorable storable = versionedSchemas.iterator().next();
            schemaVersionKey = new SchemaVersionKey(new SchemaKey(storable.getType(), storable.getSchemaGroup(), storable.getName()), storable.getVersion());
        }

        return schemaVersionKey;
    }

    private List<QueryParam> buildQueryParam(SchemaFieldQuery schemaFieldQuery) {
        List<QueryParam> queryParams = new ArrayList<>(3);
        if (schemaFieldQuery.getNamespace() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfoStorable.FIELD_NAMESPACE, schemaFieldQuery.getNamespace()));
        }
        if (schemaFieldQuery.getName() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfoStorable.NAME, schemaFieldQuery.getName()));
        }
        if (schemaFieldQuery.getType() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfoStorable.TYPE, schemaFieldQuery.getType()));
        }

        return queryParams;
    }

    private SchemaVersionKey getSchemaKey(SchemaVersionStorable storable) {
        return new SchemaVersionKey(new SchemaKey(storable.getType(), storable.getSchemaGroup(), storable.getName()), storable.getVersion());
    }

    @Override
    public Collection<SchemaVersionInfo> findAllVersions(final SchemaKey schemaKey) {
        List<QueryParam> queryParams =
                Lists.newArrayList(
                        new QueryParam(SchemaVersionStorable.TYPE, schemaKey.getType()),
                        new QueryParam(SchemaVersionStorable.SCHEMA_GROUP, schemaKey.getSchemaGroup()),
                        new QueryParam(SchemaVersionStorable.NAME, schemaKey.getName())
                );

        Collection<SchemaVersionStorable> storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);

        return (storables != null && !storables.isEmpty())
                ? storables.stream().map(schemaVersionStorable -> new SchemaVersionInfo(schemaVersionStorable)).collect(Collectors.toList())
                : Collections.emptyList();
    }

    @Override
    public Integer getSchemaVersion(SchemaKey schemaKey, String schemaText) throws SchemaNotFoundException {
        SchemaInfo schemaInfo = getSchema(schemaKey);
        if (schemaInfo == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaKey);
        }

        Long schemaMetadataId = schemaInfo.getId();

        Integer result = findSchemaVersion(schemaKey.getType(), schemaText, schemaMetadataId);

        if (result == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaKey);
        }

        return result;
    }

    private Integer findSchemaVersion(String type, String schemaText, Long schemaMetadataId) {
        String fingerPrint = getFingerprint(type, schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                new QueryParam(SchemaVersionStorable.FINGERPRINT, fingerPrint));

        Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);

        Integer result = null;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataId: [{}] and schemaText [{}]", schemaMetadataId, schemaText);
            }

            SchemaVersionStorable schemaVersionStorable = versionedSchemas.iterator().next();
            result = schemaVersionStorable.getVersion();
        }

        return result;
    }

    private String getFingerprint(String type, String schemaText) {
        return Hex.encodeHexString(schemaTypeWithProviders.get(type).getFingerprint(schemaText));
    }

    @Override
    public SchemaVersionInfo getSchemaInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        // todo add caching.
        SchemaKey schemaKey = schemaVersionKey.getSchemaKey();
        SchemaInfo schemaInfo = getSchema(schemaKey);

        if (schemaInfo != null) {
            Integer version = schemaVersionKey.getVersion();
            Long schemaMetadataId = schemaInfo.getId();
            List<QueryParam> queryParams = Lists.newArrayList(
                    new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                    new QueryParam(SchemaVersionStorable.VERSION, version.toString()));

            Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                if (versionedSchemas.size() > 1) {
                    LOG.warn("More than one schema exists with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                }
                return new SchemaVersionInfo(versionedSchemas.iterator().next());
            } else {
                throw new SchemaNotFoundException("No Schema version exists with schemaMetadataId " + schemaMetadataId + " and version " + version);
            }
        }

        throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaKey);
    }

    private SchemaInfoStorable getSchemaMetadataStorable(List<QueryParam> queryParams) {
        Collection<SchemaInfoStorable> schemaInfoStorables = storageManager.find(SchemaInfoStorable.NAME_SPACE, queryParams);
        SchemaInfoStorable schemaInfoStorable = null;
        if (schemaInfoStorables != null && !schemaInfoStorables.isEmpty()) {
            if (schemaInfoStorables.size() > 1) {
                LOG.warn("Received more than one schema with query parameters [{}]", queryParams);
            }
            schemaInfoStorable = schemaInfoStorables.iterator().next();
            LOG.debug("Schema found in registry with query parameters [{}]", queryParams);
        } else {
            LOG.debug("No schemas found in registry with query parameters [{}]", queryParams);
        }

        return schemaInfoStorable;
    }

    @Override
    public SchemaVersionInfo getLatestSchemaInfo(SchemaKey schemaKey) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> schemaVersionInfos = findAllVersions(schemaKey);

        SchemaVersionInfo latestSchema = null;
        if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
            Integer curVersion = Integer.MIN_VALUE;
            for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                if (schemaVersionInfo.getVersion() > curVersion) {
                    latestSchema = schemaVersionInfo;
                    curVersion = schemaVersionInfo.getVersion();
                }
            }
        }

        return latestSchema;
    }

    public boolean isCompatible(SchemaKey schemaKey, String toSchema) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> existingSchemaVersionInfoStorable = findAllVersions(schemaKey);
        Collection<String> schemaTexts =
                existingSchemaVersionInfoStorable.stream()
                        .map(schemaInfoStorable -> schemaInfoStorable.getSchemaText()).collect(Collectors.toList());

        SchemaInfo schemaInfo = getSchema(schemaKey);

        return isCompatible(schemaInfo.getSchemaKey().getType(), toSchema, schemaTexts, schemaInfo.getCompatibility());
    }

    public boolean isCompatible(SchemaVersionKey schemaVersionKey,
                                String toSchema) throws SchemaNotFoundException {
        SchemaKey schemaKey = schemaVersionKey.getSchemaKey();

        SchemaVersionInfo existingSchemaVersionInfo = getSchemaInfo(schemaVersionKey);
        String schemaText = existingSchemaVersionInfo.getSchemaText();
        SchemaInfo schemaInfo = getSchema(schemaKey);

        return isCompatible(schemaKey.getType(), toSchema, Collections.singletonList(schemaText), schemaInfo.getCompatibility());
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
        return getSerDesInfos(schemaMetadataId, true);
    }

    private Collection<SchemaSerDesMapping> getSchemaSerDesMappings(Long schemaMetadataId) {
        List<QueryParam> queryParams =
                Collections.singletonList(new QueryParam(SchemaSerDesMapping.SCHEMA_METADATA_ID, schemaMetadataId.toString()));

        return storageManager.find(SchemaSerDesMapping.NAMESPACE, queryParams);
    }

    @Override
    public Collection<SerDesInfo> getSchemaDeserializers(Long schemaMetadataId) {
        return getSerDesInfos(schemaMetadataId, false);
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
                if ((isSerializer && serDesInfo.getIsSerializer())
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
            throw new SerDesException("Serializer with given ID " + serDesId + " does not exist");
        }

        SchemaSerDesMapping schemaSerDesMapping = new SchemaSerDesMapping(schemaMetadataId, serDesId);
        storageManager.add(schemaSerDesMapping);
    }

}