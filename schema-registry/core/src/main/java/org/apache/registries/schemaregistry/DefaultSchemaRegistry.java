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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.registries.common.QueryParam;
import org.apache.registries.common.SlotSynchronizer;
import org.apache.registries.common.util.FileStorage;
import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import org.apache.registries.schemaregistry.serde.SerDesException;
import org.apache.registries.storage.Storable;
import org.apache.registries.storage.StorageManager;
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
    private Options options;
    private SchemaVersionInfoCache schemaVersionInfoCache;
    private List<SchemaProviderInfo> schemaProviderInfos;
    private SlotSynchronizer<String> slotSynchronizer = new SlotSynchronizer<>();

    public DefaultSchemaRegistry(StorageManager storageManager,
                                 FileStorage fileStorage,
                                 Collection<? extends SchemaProvider> schemaProviders) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaProviders = schemaProviders;
    }

    @Override
    public void init(Map<String, Object> props) {
        options = new Options(props);
        for (SchemaProvider schemaProvider : schemaProviders) {
            schemaTypeWithProviders.put(schemaProvider.getType(), schemaProvider);
        }

        schemaProviderInfos = schemaProviders.stream()
                .map(schemaProvider
                             -> new SchemaProviderInfo(schemaProvider.getType(),
                                                       schemaProvider.getName(),
                                                       schemaProvider.getDescription(),
                                                       schemaProvider.getDefaultSerializerClassName(),
                                                       schemaProvider.getDefaultDeserializerClassName()
                     )
                ).collect(Collectors.toList());

        schemaVersionInfoCache = new SchemaVersionInfoCache(key -> retrieveSchemaVersionInfo(key),
                                                            options.getMaxSchemaCacheSize(),
                                                            options.getSchemaExpiryInSecs());
        storageManager.registerStorables(
                Lists.newArrayList(SchemaMetadataStorable.class, SchemaVersionStorable.class, SchemaFieldInfoStorable.class,
                                   SerDesInfoStorable.class, SchemaSerDesMapping.class));

    }

    @Override
    public Collection<SchemaProviderInfo> getRegisteredSchemaProviderInfos() {
        return schemaProviderInfos;
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException {
        SchemaMetadataStorable givenSchemaMetadataStorable = SchemaMetadataStorable.fromSchemaMetadataInfo(new SchemaMetadataInfo(schemaMetadata));
        String type = schemaMetadata.getType();
        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }
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

    public Integer addSchemaVersion(SchemaMetadata schemaMetadata, String schemaText, String description)
            throws IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException {
        Integer version;
        // todo handle with minimal lock usage.
        String schemaName = schemaMetadata.getName();
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo retrievedschemaMetadataInfo = getSchemaMetadata(schemaName);
        if (retrievedschemaMetadataInfo != null) {
            // check whether the same schema text exists
            try {
                version = getSchemaVersion(schemaName, schemaText);
            } catch (SchemaNotFoundException e) {
                version = createSchemaVersion(schemaMetadata, retrievedschemaMetadataInfo.getId(), schemaText, description);
            }
        } else {
            Long schemaMetadataId = addSchemaMetadata(schemaMetadata);
            version = createSchemaVersion(schemaMetadata, schemaMetadataId, schemaText, description);
        }

        return version;
    }

    public Integer addSchemaVersion(String schemaName, String schemaText, String description)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException {

        Integer version;
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);
        if (schemaMetadataInfo != null) {
            SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
            // check whether the same schema text exists
            version = findSchemaVersion(schemaMetadata.getType(), schemaText, schemaMetadataInfo.getId());
            if (version == null) {
                version = createSchemaVersion(schemaMetadata, schemaMetadataInfo.getId(), schemaText, description);
            }
        } else {
            throw new SchemaNotFoundException("Schema not found with the given schemaName: " + schemaName);
        }

        return version;
    }


    private Integer createSchemaVersion(SchemaMetadata schemaMetadata, Long schemaMetadataId, String schemaText,
                                        String description)
            throws IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException {

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        String type = schemaMetadata.getType();
        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        // generate fingerprint, it parses the schema and checks for semantic validation.
        // throws InvalidSchemaException for invalid schemas.
        final String fingerprint = getFingerprint(type, schemaText);
        final String schemaName = schemaMetadata.getName();

        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        final Long schemaInstanceId = storageManager.nextId(schemaVersionStorable.getNameSpace());
        schemaVersionStorable.setId(schemaInstanceId);
        schemaVersionStorable.setSchemaMetadataId(schemaMetadataId);

        schemaVersionStorable.setFingerprint(fingerprint);

        schemaVersionStorable.setName(schemaName);

        schemaVersionStorable.setSchemaText(schemaText);
        schemaVersionStorable.setDescription(description);
        schemaVersionStorable.setTimestamp(System.currentTimeMillis());

        // take a lock for a schema with same name.
        SlotSynchronizer.Lock slotLock = slotSynchronizer.lockSlot(schemaName);
        try {
            Collection<SchemaVersionInfo> schemaVersionInfos = findAllVersions(schemaName);
            Integer version = 0;
            if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
                SchemaVersionInfo latestSchemaVersionInfo = null;
                SchemaCompatibility compatibility = schemaMetadata.getCompatibility();
                for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                    // check for compatibility
                    CompatibilityResult compatibilityResult = schemaTypeWithProviders.get(type).checkCompatibility(schemaText, schemaVersionInfo.getSchemaText(), compatibility);
                    if (!compatibilityResult.isCompatible()) {
                        throw new IncompatibleSchemaException("Given schema is not compatible with earlier schema versions. Error encountered is: " + compatibilityResult.getErrorMessage());
                    }

                    Integer curVersion = schemaVersionInfo.getVersion();
                    if (curVersion >= version) {
                        latestSchemaVersionInfo = schemaVersionInfo;
                        version = curVersion;
                    }
                }

                version = latestSchemaVersionInfo.getVersion();
            }
            schemaVersionStorable.setVersion(version + 1);

            storageManager.add(schemaVersionStorable);

            String storableNamespace = new SchemaFieldInfoStorable().getNameSpace();
            List<SchemaFieldInfo> schemaFieldInfos = schemaTypeWithProviders.get(type).generateFields(schemaVersionStorable.getSchemaText());
            for (SchemaFieldInfo schemaFieldInfo : schemaFieldInfos) {
                final Long fieldInstanceId = storageManager.nextId(storableNamespace);
                SchemaFieldInfoStorable schemaFieldInfoStorable = SchemaFieldInfoStorable.fromSchemaFieldInfo(schemaFieldInfo, fieldInstanceId);
                schemaFieldInfoStorable.setSchemaInstanceId(schemaInstanceId);
                schemaFieldInfoStorable.setTimestamp(System.currentTimeMillis());
                storageManager.add(schemaFieldInfoStorable);
            }
        } finally {
            slotLock.unlock();
        }

        return schemaVersionStorable.getVersion();
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadata(Long schemaMetadataId) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setId(schemaMetadataId);

        List<QueryParam> params = Collections.singletonList(new QueryParam(SchemaMetadataStorable.ID, schemaMetadataId.toString()));
        Collection<SchemaMetadataStorable> schemaMetadataStorables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, params);
        SchemaMetadataInfo schemaMetadataInfo = null;
        if (schemaMetadataStorables != null && !schemaMetadataStorables.isEmpty()) {
            schemaMetadataInfo = SchemaMetadataStorable.toSchemaMetadataInfo(schemaMetadataStorables.iterator().next());
            if (schemaMetadataStorables.size() > 1) {
                LOG.warn("No unique entry with schemaMetatadataId: [{}]", schemaMetadataId);
            }
            LOG.info("SchemaMetadata entries with id [{}] is [{}]", schemaMetadataStorables);
        }

        return schemaMetadataInfo;
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadata(String schemaName) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setName(schemaName);

        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
        return schemaMetadataStorable != null ? SchemaMetadataStorable.toSchemaMetadataInfo(schemaMetadataStorable) : null;
    }

    @Override
    public Collection<SchemaMetadata> findSchemaMetadata(Map<String, String> filters) {
        // todo get only few selected columns instead of getting the whole row.
        Collection<SchemaMetadataStorable> storables;

        if (filters == null || filters.isEmpty()) {
            storables = storageManager.list(SchemaMetadataStorable.NAME_SPACE);
        } else {
            List<QueryParam> queryParams = new ArrayList<>(filters.size());
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                queryParams.add(new QueryParam(entry.getKey(), entry.getValue()));
            }
            storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
        }
        List<SchemaMetadata> result;
        if (storables != null && !storables.isEmpty()) {
            result = new ArrayList<>();
            for (SchemaMetadataStorable storable : storables) {
                result.add(new SchemaMetadata.Builder(storable.getName())
                                   .type(storable.getType())
                                   .schemaGroup(storable.getSchemaGroup())
                                   .build());
            }
        } else {
            result = Collections.emptyList();
        }

        return result;
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasWithFields(SchemaFieldQuery schemaFieldQuery) {
        List<QueryParam> queryParams = buildQueryParam(schemaFieldQuery);

        Collection<SchemaFieldInfoStorable> fieldInfos = storageManager.find(SchemaFieldInfoStorable.STORABLE_NAME_SPACE, queryParams);
        Collection<SchemaVersionKey> schemaVersionKeys;
        if (fieldInfos != null && !fieldInfos.isEmpty()) {
            List<Long> schemaIds = new ArrayList<>();
            for (SchemaFieldInfoStorable fieldInfo : fieldInfos) {
                schemaIds.add(fieldInfo.getSchemaInstanceId());
            }

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
            schemaVersionKey = new SchemaVersionKey(storable.getName(), storable.getVersion());
        }

        return schemaVersionKey;
    }

    private List<QueryParam> buildQueryParam(SchemaFieldQuery schemaFieldQuery) {
        List<QueryParam> queryParams = new ArrayList<>(3);
        if (schemaFieldQuery.getNamespace() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfo.FIELD_NAMESPACE, schemaFieldQuery.getNamespace()));
        }
        if (schemaFieldQuery.getName() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfo.NAME, schemaFieldQuery.getName()));
        }
        if (schemaFieldQuery.getType() != null) {
            queryParams.add(new QueryParam(SchemaFieldInfo.TYPE, schemaFieldQuery.getType()));
        }

        return queryParams;
    }

    @Override
    public Collection<SchemaVersionInfo> findAllVersions(final String schemaName) {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.NAME, schemaName));

        Collection<SchemaVersionStorable> storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
        List<SchemaVersionInfo> schemaVersionInfos;
        if (storables != null && !storables.isEmpty()) {
            schemaVersionInfos = new ArrayList<>(storables.size());
            for (SchemaVersionStorable storable : storables) {
                schemaVersionInfos.add(storable.toSchemaVersionInfo());
            }
        } else {
            schemaVersionInfos = Collections.emptyList();
        }
        return schemaVersionInfos;
    }

    @Override
    public Integer getSchemaVersion(String schemaName, String schemaText) throws SchemaNotFoundException, InvalidSchemaException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaName);
        }

        Long schemaMetadataId = schemaMetadataInfo.getId();
        Integer result = findSchemaVersion(schemaMetadataInfo.getSchemaMetadata().getType(), schemaText, schemaMetadataId);

        if (result == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaName);
        }

        return result;
    }

    private Integer findSchemaVersion(String type, String schemaText, Long schemaMetadataId) throws InvalidSchemaException {
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

    private String getFingerprint(String type, String schemaText) throws InvalidSchemaException {
        return Hex.encodeHexString(schemaTypeWithProviders.get(type).getFingerprint(schemaText));
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(schemaVersionKey);
    }

    SchemaVersionInfo retrieveSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        String schemaName = schemaVersionKey.getSchemaName();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);

        if (schemaMetadataInfo != null) {
            Integer version = schemaVersionKey.getVersion();
            Long schemaMetadataId = schemaMetadataInfo.getId();
            List<QueryParam> queryParams = Lists.newArrayList(
                    new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                    new QueryParam(SchemaVersionStorable.VERSION, version.toString()));

            Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                if (versionedSchemas.size() > 1) {
                    LOG.warn("More than one schema exists with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                }
                return versionedSchemas.iterator().next().toSchemaVersionInfo();
            } else {
                throw new SchemaNotFoundException("No Schema version exists with schemaMetadataId " + schemaMetadataId + " and version " + version);
            }
        }

        throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaName);
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
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> schemaVersionInfos = findAllVersions(schemaName);

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

    public CompatibilityResult checkCompatibility(String schemaName, String toSchema) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> schemaVersionInfos = findAllVersions(schemaName);
        Collection<String> schemaTexts = new ArrayList<>(schemaVersionInfos.size());
        for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
            schemaTexts.add(schemaVersionInfo.getSchemaText());
        }

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);

        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        return checkCompatibility(schemaMetadata.getType(), toSchema, schemaTexts, schemaMetadata.getCompatibility());
    }

    public CompatibilityResult checkCompatibility(SchemaVersionKey schemaVersionKey,
                                                  String toSchema) throws SchemaNotFoundException {
        String schemaName = schemaVersionKey.getSchemaName();

        SchemaVersionInfo existingSchemaVersionInfo = getSchemaVersionInfo(schemaVersionKey);
        String schemaText = existingSchemaVersionInfo.getSchemaText();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        return checkCompatibility(schemaMetadata.getType(), toSchema, Collections.singletonList(schemaText), schemaMetadata.getCompatibility());
    }

    private CompatibilityResult checkCompatibility(String type,
                                                   String toSchema,
                                                   Collection<String> existingSchemas,
                                                   SchemaCompatibility compatibility) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.checkCompatibility(toSchema, existingSchemas, compatibility);
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
                if (isSerializer) {
                    if (serDesInfo.getIsSerializer()) {
                        serDesInfos.add(serDesInfo);
                    }
                } else if (!serDesInfo.getIsSerializer()) {
                    serDesInfos.add(serDesInfo);
                }
            }
        }
        return serDesInfos;
    }

    @Override
    public InputStream downloadJar(Long serDesId) {
        InputStream inputStream = null;

        SerDesInfo serDesInfoStorable = getSerDesInfo(serDesId);
        if (serDesInfoStorable != null) {
            try {
                inputStream = fileStorage.downloadFile(serDesInfoStorable.getFileId());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return inputStream;
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

    public static class Options {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        public static final String SCHEMA_CACHE_SIZE = "schemaCacheSize";
        public static final String SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = "schemaCacheExpiryInterval";
        public static final int DEFAULT_SCHEMA_CACHE_SIZE = 10000;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60L;

        private final Map<String, ?> config;

        public Options(Map<String, ?> config) {
            this.config = config;
        }

        private Object getPropertyValue(String propertyKey, Object defaultValue) {
            Object value = config.get(propertyKey);
            return value != null ? value : defaultValue;
        }

        public int getMaxSchemaCacheSize() {
            return Integer.valueOf(getPropertyValue(SCHEMA_CACHE_SIZE, DEFAULT_SCHEMA_CACHE_SIZE).toString());
        }

        public long getSchemaExpiryInSecs() {
            return Long.valueOf(getPropertyValue(SCHEMA_CACHE_EXPIRY_INTERVAL_SECS, DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS).toString());
        }
    }

}