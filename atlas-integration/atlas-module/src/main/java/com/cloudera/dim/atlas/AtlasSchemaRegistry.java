/**
 * Copyright 2016-2020 Cloudera, Inc.
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
 **/
package com.cloudera.dim.atlas;

import com.cloudera.dim.atlas.shim.AtlasPluginFactory;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaBranchKey;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionLifecycleManager;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.cache.SchemaBranchCache;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.details.InitializedStateDetails;
import com.hortonworks.registries.schemaregistry.state.details.MergeInfo;
import com.hortonworks.registries.schemaregistry.utils.ObjectMapperUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of Schema Registry which relies on Atlas for persistence.
 */
public class AtlasSchemaRegistry implements ISchemaRegistry, SchemaVersionRetriever {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasSchemaRegistry.class);

    private AtlasPlugin atlasClient;
    private final FileStorage fileStorage;
    private final Collection<Map<String, Object>> schemaProvidersConfig;
    private Map<String, SchemaProvider> schemaTypeWithProviders;
    private List<SchemaProviderInfo> schemaProviderInfos;
    private SchemaVersionLifecycleManager schemaVersionLifecycleManager;

    public AtlasSchemaRegistry(FileStorage fileStorage, Collection<Map<String, Object>> schemaProvidersConfig) {
        this.fileStorage = fileStorage;
        this.schemaProvidersConfig = schemaProvidersConfig;
    }

    @Override
    public void init(Map<String, Object> config) {
        LOG.info("Initializing the Atlas integration module");

        // config contains "urls" parameter which tells the client where to connect
        atlasClient = AtlasPluginFactory.create(config);

        Collection<? extends SchemaProvider> schemaProviders = initSchemaProviders(schemaProvidersConfig, this);

        this.schemaTypeWithProviders = schemaProviders.stream().collect(Collectors.toMap(SchemaProvider::getType, Function.identity()));

        schemaProviderInfos = Collections.unmodifiableList(
                schemaProviders.stream().map(schemaProvider -> new SchemaProviderInfo(schemaProvider.getType(),
                                schemaProvider.getName(),
                                schemaProvider.getDescription(),
                                schemaProvider.getDefaultSerializerClassName(),
                                schemaProvider.getDefaultDeserializerClassName()))
                        .collect(Collectors.toList()));

        Options options = new Options(config);
        SchemaBranchCache schemaBranchCache = new SchemaBranchCache(options.getMaxSchemaCacheSize(), options.getSchemaExpiryInSecs(), createSchemaBranchFetcher());

        schemaVersionLifecycleManager = new AtlasSchemaVersionLifecycleManager(atlasClient, config, schemaBranchCache) {
            @Override
            protected SchemaProvider getSchemaProvider(String providerType) {
                return schemaTypeWithProviders.get(providerType);
            }

            @Override
            protected SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
                return AtlasSchemaRegistry.this.getSchemaMetadataInfo(schemaName);
            }

            @Override
            protected SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
                return AtlasSchemaRegistry.this.getSchemaMetadataInfo(schemaMetadataId);
            }
        };
    }

    private Collection<? extends SchemaProvider> initSchemaProviders(final Collection<Map<String, Object>> schemaProvidersConfig,
                                                                     final SchemaVersionRetriever schemaVersionRetriever) {
        if (schemaProvidersConfig == null || schemaProvidersConfig.isEmpty()) {
            throw new IllegalArgumentException("No [" + SCHEMA_PROVIDERS + "] property is configured in schema registry configuration file.");
        }

        return schemaProvidersConfig.stream().map(schemaProviderConfig -> {
            String className = (String) schemaProviderConfig.get("providerClass");
            if (className == null || className.isEmpty()) {
                throw new IllegalArgumentException("Schema provider class name must be non empty, Invalid provider class name [" + className + "]");
            }

            try {
                SchemaProvider schemaProvider = (SchemaProvider) Class.forName(className, true, Thread.currentThread().getContextClassLoader()).newInstance();
                HashMap<String, Object> config = new HashMap<>(schemaProviderConfig);
                config.put(SchemaProvider.SCHEMA_VERSION_RETRIEVER_CONFIG, schemaVersionRetriever);
                schemaProvider.init(Collections.unmodifiableMap(config));

                return schemaProvider;
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                LOG.error("Error encountered while loading SchemaProvider [{}] ", className, e);
                throw new IllegalArgumentException(e);
            }
        })
        .collect(Collectors.toList());
    }

    private SchemaBranchCache.SchemaBranchFetcher createSchemaBranchFetcher() {
        return new SchemaBranchCache.SchemaBranchFetcher() {
            @Override
            public SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaBranchNotFoundException {
                try {
                    return AtlasSchemaRegistry.this.getSchemaBranch(schemaBranchKey);
                } catch (SchemaNotFoundException e) {
                    throw new AtlasUncheckedException(e);
                }
            }

            @Override
            public SchemaBranch getSchemaBranch(Long id) throws SchemaBranchNotFoundException {
                return AtlasSchemaRegistry.this.getSchemaBranch(id);
            }
        };
    }

    public void setupAtlasModel() {
        atlasClient.setupAtlasModel();
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata, boolean throwErrorIfExists) {
        LOG.info("--------------- addSchemaMetadata {}", schemaMetadata);
        Optional<SchemaMetadataInfo> existingMeta = atlasClient.getSchemaMetadataInfo(schemaMetadata.getName());

        if (existingMeta.isPresent()) {
            if (throwErrorIfExists) {
                throw new RuntimeException("Schema " + schemaMetadata.getName() + " already exists.");
            } else {
                return existingMeta.get().getId();
            }
        }

        return atlasClient.createMeta(schemaMetadata);
    }

    @Override
    public Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("--------------- findAggregatedSchemaMetadata {}", props);

        return findSchemaMetadata(props)
                .stream()
                .map(schemaMetadataInfo -> {
                    try {
                        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
                    } catch (SchemaNotFoundException | SchemaBranchNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private AggregatedSchemaMetadataInfo buildAggregatedSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        if (schemaMetadataInfo == null) {
            return null;
        }

        Collection<SerDesInfo> serDesInfos = getSerDesInfos(schemaMetadataInfo.getSchemaMetadata().getName());

        return new AggregatedSchemaMetadataInfo(schemaMetadataInfo.getSchemaMetadata(),
                schemaMetadataInfo.getId(),
                schemaMetadataInfo.getTimestamp(),
                getAggregatedSchemaBranch(schemaMetadataInfo.getSchemaMetadata().getName()),
                serDesInfos);
    }

    private Collection<SerDesInfo> getSerDesInfos(String schemaName) throws SchemaNotFoundException {
        return atlasClient.getSerDesMappingsForSchema(schemaName);
    }

    @Override
    public AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("--------------- getAggregatedSchemaMetadataInfo {}", schemaName);

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
    }

    @Override
    public Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props) {
        LOG.info("--------------- findSchemaMetadata {}", props);

        // return atlasClient.search(name, desc, orderBy);
        // TODO see impl in DefaultSchemaRegistry, it supports all field params, not just these two
        String name = props.get(SchemaMetadataStorable.NAME);
        String desc = props.get(SchemaMetadataStorable.DESCRIPTION);
        String orderBy = props.get("_orderByFields");
        return atlasClient.search(Optional.ofNullable(name), Optional.ofNullable(desc), Optional.ofNullable(orderBy));
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        LOG.info("--------------- getSupportedSchemaProviders {}", schemaProviderInfos);
        return schemaProviderInfos;
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        LOG.info("--------------- registerSchemaMetadata {}", schemaMetadata);
        return addSchemaMetadata(schemaMetadata);
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        LOG.info("--------------- addSchemaMetadata {}", schemaMetadata);
        return addSchemaMetadata(schemaMetadata, false);
    }

    @Override
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        LOG.info("--------------- updateSchemaMetadata {}", schemaName);

        if (!schemaName.equals(schemaMetadata.getName())) {
            throw new IllegalArgumentException("schemaName must match the name in schemaMetadata");
        }

        try {
            return atlasClient.updateMeta(schemaMetadata).orElse(null);
        } catch (SchemaNotFoundException e) {
            throw new AtlasUncheckedException(e);
        }
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        LOG.info("---------------1 getSchemaMetadataInfo {}", schemaName);
        return atlasClient.getSchemaMetadataInfo(schemaName).orElse(null);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        LOG.info("---------------2 getSchemaMetadataInfo {}", schemaMetadataId);
        return atlasClient.getSchemaMetadataInfo(schemaMetadataId).orElse(null);
    }

    @Override
    public void deleteSchema(String schemaName) throws SchemaNotFoundException {
        LOG.info("--------------- deleteSchema {}", schemaName);
        // TODO delete schema, versions, branches, serdes
    }

    @Override
    public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        LOG.info("--------------- retrieveSchemaVersion {}", schemaVersionKey);

        String schemaName = schemaVersionKey.getSchemaName();
        Integer version = schemaVersionKey.getVersion();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);

        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaName);
        }

        return fetchSchemaVersionInfo(schemaVersionKey.getSchemaName(), version);
    }

    private SchemaVersionInfo fetchSchemaVersionInfo(String schemaName,
                                                     Integer version) throws SchemaNotFoundException {
        LOG.info("##### fetching schema version for name: [{}] version: [{}]", schemaName, version);
        SchemaVersionInfo schemaVersionInfo = null;
        if (SchemaVersionKey.LATEST_VERSION.equals(version)) {
            schemaVersionInfo = getLatestSchemaVersionInfo(schemaName);
        } else {
            schemaVersionInfo = atlasClient.getSchemaVersion(schemaName, version)
                    .orElseThrow(() -> new SchemaNotFoundException("No Schema version exists with name " + schemaName + " and version " + version));
        }
        LOG.info("##### fetched schema version info [{}]", schemaVersionInfo);
        return schemaVersionInfo;
    }

    @Override
    public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion idKey) throws SchemaNotFoundException {
        LOG.info("--------------- retrieveSchemaVersion {}", idKey);

        // TODO Review this - legacy code can fetch by meta.id, version.num and version.id
        return null;
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------1 addSchemaVersion {} {}", schemaMetadata, schemaVersion);

        // TODO
        //lockSchemaMetadata(schemaMetadata.getName());  // ??
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, schemaVersion, x -> registerSchemaMetadata(x), disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------2 addSchemaVersion {} {} {}", schemaMetadata, schemaBranchName, schemaVersion);

        // TODO
        //lockSchemaMetadata(schemaMetadata.getName());  // ??
        return schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaMetadata, schemaVersion, x -> registerSchemaMetadata(x), disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------3 addSchemaVersion {}", schemaName);

        // TODO
        // lockSchemaMetadata(schemaName);  // ??
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion, disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName, String schemaName, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------4 addSchemaVersion {} {}", schemaName, schemaBranchName);

        SchemaIdVersion schemaIdVersion;
        try {
            schemaIdVersion = schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, disableCanonicalCheck);
        } catch (SchemaNotFoundException | SchemaBranchNotFoundException ex) {
            throw new RuntimeException("Could not add version to schema " + schemaName, ex);
        }

        LOG.info("----------> returning {}", schemaIdVersion);
        if (schemaIdVersion == null) {
            throw new Error("This doesn't work. Die plz.");
        }

        return schemaIdVersion;
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("--------------- deleteSchemaVersion {}", schemaVersionKey);
        schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionKey);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        LOG.info("---------------1 getSchemaVersionInfo {}", schemaVersionKey);
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaVersionKey);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        LOG.info("---------------2 getSchemaVersionInfo {}", schemaIdVersion);
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        LOG.info("--------------- getLatestSchemaVersionInfo {}", schemaName);
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaName);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("--------------- getLatestSchemaVersionInfo {} {}", schemaName, schemaBranchName);
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaBranchName, schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        LOG.info("---------------1 getAllVersions {}", schemaName);
        return schemaVersionLifecycleManager.getAllVersions(schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------2 getAllVersions {} {}", schemaName, schemaBranchName);
        return schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName);
    }

    @Override
    public Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("--------------- getAggregatedSchemaBranch {}", schemaName);

        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = new ArrayList<>();
        for (SchemaBranch schemaBranch : getSchemaBranches(schemaName)) {
            Long rootVersion = schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH) ? null: schemaVersionLifecycleManager.getRootVersion(schemaBranch).getId();
            Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaBranch.getName(), schemaName);
            schemaVersionInfos.stream().forEach(schemaVersionInfo -> {
                SchemaVersionLifecycleContext context = null;
                try {
                    context = schemaVersionLifecycleManager.createSchemaVersionLifeCycleContext(schemaVersionInfo.getId(), SchemaVersionLifecycleStates.INITIATED);
                    MergeInfo mergeInfo = null;
                    if (context.getDetails() == null) {
                        mergeInfo = null;
                    } else {
                        try {
                            InitializedStateDetails details = ObjectMapperUtils.deserialize(context.getDetails(), InitializedStateDetails.class);
                            mergeInfo = details.getMergeInfo();
                        } catch (IOException e) {
                            throw new RuntimeException(String.format("Failed to serialize state details of schema version : '%s'",context.getSchemaVersionId()),e);
                        }
                    }
                    schemaVersionInfo.setMergeInfo(mergeInfo);
                } catch (SchemaNotFoundException e) {
                    // If the schema version has never been in 'INITIATED' state, then SchemaNotFoundException error is thrown which is expected
                    schemaVersionInfo.setMergeInfo(null);
                }
            });
            aggregatedSchemaBranches.add(new AggregatedSchemaBranch(schemaBranch, rootVersion, schemaVersionInfos));
        }
        return aggregatedSchemaBranches;
    }

    @Override
    public SchemaBranch getSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException {
        LOG.info("---------------1 getSchemaBranch {}", schemaBranchId);

        Optional<SchemaBranch> schemaBranch = atlasClient.getSchemaBranchById(schemaBranchId);
        if (!schemaBranch.isPresent()) {
            throw new SchemaBranchNotFoundException(String.format("Schema branch with id : '%s' not found", schemaBranchId.toString()));
        }
        return schemaBranch.get();
    }

    private SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------2 getSchemaBranch {}", schemaBranchKey);

        Optional<SchemaBranch> schemaBranch = atlasClient.getSchemaBranch(schemaBranchKey.getSchemaMetadataName(), schemaBranchKey.getSchemaBranchName());
        if (!schemaBranch.isPresent()) {
            throw new SchemaBranchNotFoundException(String.format("Schema branch with key : %s not found", schemaBranchKey));
        }

        return schemaBranch.get();
    }

    @Override
    public Collection<SchemaMetadataInfo> searchSchemas(MultivaluedMap<String, String> queryParameters, Optional<String> orderBy) {
        checkNotNull(queryParameters, "Query parameters are empty.");
        String name = StringUtils.trimToNull(queryParameters.getFirst(SchemaMetadataStorable.NAME));
        String desc = StringUtils.trimToNull(queryParameters.getFirst(SchemaMetadataStorable.DESCRIPTION));
        LOG.info("--------------- searchSchemas \"{}\", \"{}\", \"{}\"", name, desc, orderBy.orElse(null));
        return atlasClient.search(Optional.ofNullable(name), Optional.ofNullable(desc), orderBy);
    }

    @Override
    public SchemaVersionInfo getLatestEnabledSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("--------------- getLatestEnabledSchemaVersionInfo {} {}", schemaName, schemaBranchName);
        return schemaVersionLifecycleManager.getLatestEnabledSchemaVersionInfo(schemaBranchName, schemaName);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(String schemaName, String schemaText, boolean disableCanonicalCheck) throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException {
        LOG.info("--------------- getSchemaVersionInfo {}", schemaName);
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaName, schemaText, disableCanonicalCheck);
    }

    @Override
    public SchemaVersionInfo findSchemaVersionByFingerprint(String fingerprint) throws SchemaNotFoundException {
        LOG.info("--------------- findSchemaVersionByFingerprint {}", fingerprint);
        return schemaVersionLifecycleManager.findSchemaVersionInfoByFingerprint(fingerprint);
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy, boolean disableCanonicalCheck) throws IncompatibleSchemaException, SchemaNotFoundException {
        LOG.info("--------------- mergeSchemaVersion {}", schemaVersionId);
        return schemaVersionLifecycleManager.mergeSchemaVersion(schemaVersionId, schemaVersionMergeStrategy, disableCanonicalCheck);
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranchesForVersion(Long versionId) throws SchemaBranchNotFoundException {
        LOG.info("--------------- getSchemaBranchesForVersion {}", versionId);
        return schemaVersionLifecycleManager.getSchemaBranches(versionId);
    }

    @Override
    public void invalidateCache(SchemaRegistryCacheType schemaRegistryCacheType, String keyAsString) {
        LOG.info("--------------- invalidateCache {} {}", schemaRegistryCacheType, keyAsString);
        // TODO
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------1 checkCompatibility {}", schemaName);
        return schemaVersionLifecycleManager.checkCompatibility(SchemaBranch.MASTER_BRANCH, schemaName, toSchemaText);
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("---------------2 checkCompatibility {} {} {} {}", schemaName, schemaBranchName, schemaName, toSchemaText);
        return schemaVersionLifecycleManager.checkCompatibility(schemaBranchName, schemaName, toSchemaText);
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) throws SchemaBranchNotFoundException, SchemaNotFoundException {
        LOG.info("--------------- findSchemasByFields {}", schemaFieldQuery);
        return null;  // TODO
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("--------------- getAllVersions {} {}", schemaName, schemaBranchName);
        if (stateIds == null || stateIds.isEmpty())
            return getAllVersions(schemaBranchName, schemaName);
        else
            return schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName, stateIds);
    }

    @Override
    public SerDesInfo getSerDes(Long serDesId) {
        LOG.info("--------------- getSerDes {}", serDesId);

        return atlasClient.getSerdesById(serDesId).orElse(null);
    }

    @Override
    public String uploadFile(InputStream inputStream) throws SerDesException {
        String fileName = UUID.randomUUID().toString();
        try {
            String uploadedFilePath = fileStorage.upload(inputStream, fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return fileName;
    }

    @Override
    public InputStream downloadFile(String fileId) throws IOException {
        return fileStorage.download(fileId);
    }

    @Override
    public Long addSerDes(SerDesPair serializerInfo) {
        LOG.info("--------------- addSerDes {}", serializerInfo);
        return atlasClient.addSerdes(serializerInfo);
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        LOG.info("--------------- mapSchemaWithSerDes {} {}", schemaName, serDesId);
        try {
            atlasClient.mapSchemaWithSerdes(schemaName, serDesId);
        } catch (SchemaNotFoundException e) {
            throw new AtlasUncheckedException(e);
        }
    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        LOG.info("--------------- getSerDes {}", schemaName);
        try {
            return atlasClient.getAllSchemaSerdes(schemaName);
        } catch (SchemaNotFoundException e) {
            throw new AtlasUncheckedException(e);
        }
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        schemaVersionLifecycleManager.enableSchemaVersion(schemaVersionId);
    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionId);
    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.archiveSchemaVersion(schemaVersionId);
    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.disableSchemaVersion(schemaVersionId);
    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.startSchemaVersionReview(schemaVersionId);
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, boolean disableCanonicalCheck) throws SchemaNotFoundException, IncompatibleSchemaException {
        return mergeSchemaVersion(schemaVersionId, SchemaVersionMergeStrategy.valueOf(DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY), disableCanonicalCheck);
    }

    @Override
    public void transitionState(Long schemaVersionId, Byte targetStateId, byte[] transitionDetails) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("--------------- transitionState {} {}", schemaVersionId, targetStateId);
        schemaVersionLifecycleManager.executeState(schemaVersionId, targetStateId, transitionDetails);
    }

    @Override
    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        LOG.info("--------------- getSchemaVersionLifecycleStateMachineInfo");
        return schemaVersionLifecycleManager.getSchemaVersionLifecycleStateMachine().toConfig();
    }

    @Override
    public SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch) throws SchemaBranchAlreadyExistsException, SchemaNotFoundException {
        LOG.info("--------------- createSchemaBranch {}", schemaVersionId);
        checkNotNull(schemaBranch.getName(), "Schema branch name can't be null");

        SchemaVersionInfo schemaVersionInfo = schemaVersionLifecycleManager.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));

        SchemaBranchKey schemaBranchKey = new SchemaBranchKey(schemaBranch.getName(), schemaVersionInfo.getName());
        Optional<SchemaBranch> existingSchemaBranch = atlasClient.getSchemaBranch(schemaBranchKey.getSchemaMetadataName(), schemaBranchKey.getSchemaBranchName());

        if (existingSchemaBranch.isPresent()) {
            throw new SchemaBranchAlreadyExistsException(String.format("A schema branch with name : '%s' already exists", schemaBranch.getName()));
        }

        try {
            return atlasClient.createBranch(schemaVersionInfo, schemaBranch.getName());
        } catch (SchemaNotFoundException ex) {
            throw new RuntimeException(String.format("Failed to fetch persisted schema branch : '%s' from the database", schemaBranch.getName()));
        }
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException {
        LOG.info("--------------- getSchemaBranches {}", schemaName);
        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaName);
        return schemaVersionInfos.stream().flatMap(schemaVersionInfo -> {
            try {
                return schemaVersionLifecycleManager.getSchemaBranches(schemaVersionInfo.getId()).stream();
            } catch (SchemaBranchNotFoundException e) {
                throw new RuntimeException(String.format("Failed to obtain schema branch associated with schema name : %s", schemaName),e);
            }
        }).collect(Collectors.toSet());
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {
        LOG.info("--------------- deleteSchemaBranch {}", schemaBranchId);
        // TODO
    }
    
    @Override
    public SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.fetchSchemaVersionInfo(id);
    }

}
