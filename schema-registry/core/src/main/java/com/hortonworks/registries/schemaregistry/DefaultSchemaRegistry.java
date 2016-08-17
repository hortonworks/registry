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
import com.hortonworks.registries.schemaregistry.serde.SerDeException;
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

/**
 *
 * Remove todos with respective JIRAs created
 * todo need to check whether given schema exists in store by using fingerprint, most likely md5
 * For example like avro, use md5 hash. https://avro.apache.org/docs/1.7.7/spec.html#Schema+Fingerprints
 *
 * Use API for respective SchemaProvider,
 * schemaTypeWithProviders.get(schemaMetadataStorable.getType()).getFingerPrint(schemaText);
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
    public SchemaMetadataStorable addSchemaMetadata(SchemaMetadataStorable schemaMetadataStorable) {
        final Long nextId = storageManager.nextId(schemaMetadataStorable.getNameSpace());
        SchemaMetadataStorable updatedSchemaMetadataStorable = new SchemaMetadataStorable(schemaMetadataStorable) {{
            id = nextId;
            timestamp = System.currentTimeMillis();
        }};
        storageManager.addOrUpdate(updatedSchemaMetadataStorable);
        return updatedSchemaMetadataStorable;
    }

    @Override
    public SchemaMetadataStorable getSchemaMetadata(final Long schemaMetadataId) {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable() {{
            id = schemaMetadataId;
        }};
        return storageManager.get(schemaMetadataStorable.getStorableKey());
    }

    @Override
    public SchemaInfoStorable addSchemaInfo(SchemaInfoStorable givenSchemaInfoStorable) {
        Long schemaMetadataId = givenSchemaInfoStorable.getSchemaMetadataId();

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        Long id = givenSchemaInfoStorable.getId();
        if (id != null) {
            LOG.info("Received ID [{}] in SchemaInfo instance is ignored", id);
        }
        final Long nextId = storageManager.nextId(givenSchemaInfoStorable.getNameSpace());
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable(givenSchemaInfoStorable) {{
            id = nextId;
            timestamp = System.currentTimeMillis();
        }};

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
            storageManager.add(schemaInfoStorable);
        }

        return schemaInfoStorable;
    }

    public SchemaInfoStorable getSchemaInfo(final Long schemaInfoId) {
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable() {{
            id = schemaInfoId;
        }};

        return storageManager.get(schemaInfoStorable.getStorableKey());
    }

    @Override
    public Collection<SchemaInfoStorable> listAll() {
        return storageManager.list(SchemaInfoStorable.NAME_SPACE);
    }

    @Override
    public SchemaInfoStorable getSchemaInfo(String type, String name, Integer version) {
        SchemaMetadataStorable schemaMetadataStorable = findSchemaMetadata(type, name);

        List<QueryParam> schemaInfoQueryParams =
                Lists.newArrayList(new QueryParam(SchemaInfoStorable.SCHEMA_METADATA_ID, schemaMetadataStorable.getId().toString()),
                        new QueryParam(SchemaInfoStorable.VERSION, version.toString()));
        Collection<SchemaInfoStorable> schemaInfoStorables = storageManager.find(SchemaInfoStorable.NAME_SPACE, schemaInfoQueryParams);

        SchemaInfoStorable result = null;
        if (schemaInfoStorables != null && !schemaInfoStorables.isEmpty()) {
            if (schemaInfoStorables.size() > 1) {
                LOG.warn("Exists more than one schema with metadataId: [{}] and version [{}]", schemaMetadataStorable.getId(), version);
            } else {
                result = schemaInfoStorables.iterator().next();
            }
        }
        return result;
    }

    @Override
    public Collection<SchemaInfoStorable> findAllVersions(final Long schemaMetadataId) {
        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(new SchemaMetadataStorable() {{
            id = schemaMetadataId;
        }}.getStorableKey());

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
    public SchemaKey getSchemaInfo(String type, String schemaText) {
        SchemaInfoStorable result = null;

        byte[] fingerPrint = schemaTypeWithProviders.get(type).getFingerPrint(schemaText);
        // query param does supports only string representation, it does not support byte array. No way to convert that into string.
//       Collection<SchemaInfoStorable> versionedSchemas = storageManager.find(SchemaInfoStorable.NAME_SPACE, Collections.singletonList(new QueryParam(SchemaInfoStorable.FINGERPRINT, fingerPrint)));
//        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
//            LOG.warn("Exists more than one schema with metadataId: [{}] and version [{}]", schemaMetadataId, version);
//            result = versionedSchemas.iterator().next();
//        }

        return result != null ? new SchemaKey(result.getId(), result.getVersion()) : null;
    }

    @Override
    public SchemaInfoStorable getSchemaInfo(final Long schemaMetadataId, Integer version) {
        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(new SchemaMetadataStorable() {{
            id = schemaMetadataId;
        }}.getStorableKey());

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

    private SchemaMetadataStorable findSchemaMetadata(String type, String name) {
        List<QueryParam> queryParams =
                Lists.newArrayList(new QueryParam(SchemaMetadataStorable.TYPE, type),
                        new QueryParam(SchemaMetadataStorable.NAME, name));

        Collection<SchemaMetadataStorable> schemaInfos = storageManager.find(SchemaMetadataStorable.NAME_SPACE, queryParams);
        SchemaMetadataStorable schemaInfo = null;
        if (schemaInfos != null && !schemaInfos.isEmpty()) {
            if (schemaInfos.size() > 1) {
                LOG.warn("Received more than one schema with query parameters [{}]", queryParams);
            }
            schemaInfo = schemaInfos.iterator().next();
            LOG.debug("Schema found in registry with query parameters [{}]", queryParams);
        } else {
            LOG.debug("No schemas found in registry with query parameters [{}]", queryParams);
        }
        return schemaInfo;
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

    public SchemaInfoStorable removeSchemaInfo(final Long schemaInfoId) {
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable() {{
            id = schemaInfoId;
        }};
        return storageManager.remove(schemaInfoStorable.getStorableKey());
    }

    public boolean isCompatible(Long schemaMetadataId,
                                Integer version,
                                String toSchema) throws SchemaNotFoundException {
        SchemaInfoStorable existingSchemaInfoStorable = getSchemaInfo(schemaMetadataId, version);
        String schemaText = existingSchemaInfoStorable.getSchemaText();
        SchemaMetadataStorable schemaMetadata = getSchemaMetadata(schemaMetadataId);

        return isCompatible(schemaMetadata.getType(), toSchema, schemaText, schemaMetadata.getCompatibility());
    }

    public boolean isCompatible(String type,
                                String toSchema,
                                String existingSchema,
                                SchemaProvider.Compatibility compatibility) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.isCompatible(toSchema, existingSchema, compatibility);
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
    public SchemaMetadataStorable getOrCreateSchemaMetadata(SchemaMetadataStorable givenSchemaMetadataStorable) {
        List<QueryParam> queryParams = new ArrayList<>();
        String name = givenSchemaMetadataStorable.getName();
        String type = givenSchemaMetadataStorable.getType();
        if (name != null) {
            queryParams.add(new QueryParam(SchemaMetadataStorable.NAME, name));
        }
        if (type != null) {
            queryParams.add(new QueryParam(SchemaMetadataStorable.TYPE, type));
        }
        SchemaMetadataStorable schemaMetadataStorable = null;
        Collection<SchemaMetadataStorable> schemaMetadataStorables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, queryParams);
        if (schemaMetadataStorables == null || schemaMetadataStorables.isEmpty()) {
            schemaMetadataStorable = addSchemaMetadata(givenSchemaMetadataStorable);
        } else {
            if (schemaMetadataStorables.size() > 1) {
                LOG.warn("SchemaMetadata instances with name: [{}] and type: [{}] are more than one.", name, type);
            }
            schemaMetadataStorable = schemaMetadataStorables.iterator().next();
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