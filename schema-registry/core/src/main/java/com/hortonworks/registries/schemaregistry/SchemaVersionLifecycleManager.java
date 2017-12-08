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
package com.hortonworks.registries.schemaregistry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchVersionMapping;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaVersionMergeException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.state.InbuiltSchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.details.InitializedStateDetails;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.CustomSchemaStateExecutor;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateAction;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachine;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateTransition;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionService;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.exception.StorageException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
public class SchemaVersionLifecycleManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionLifecycleManager.class);

    private static final String DEFAULT_SCHEMA_REVIEW_EXECUTOR_CLASS = "com.hortonworks.registries.schemaregistry.state.DefaultCustomSchemaStateExecutor";
    public static final InbuiltSchemaVersionLifecycleState DEFAULT_VERSION_STATE = SchemaVersionLifecycleStates.INITIATED;

    private final SchemaVersionLifecycleStateMachine schemaVersionLifecycleStateMachine;
    private CustomSchemaStateExecutor customSchemaStateExecutor;
    private SchemaVersionInfoCache schemaVersionInfoCache;
    private SchemaVersionRetriever schemaVersionRetriever;
    private static final int DEFAULT_RETRY_CT = 5;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private StorageManager storageManager;
    private SchemaBranchCache schemaBranchCache;
    private DefaultSchemaRegistry.SchemaMetadataFetcher schemaMetadataFetcher;

    public SchemaVersionLifecycleManager(StorageManager storageManager,
                                         Map<String, Object> props,
                                         DefaultSchemaRegistry.SchemaMetadataFetcher schemaMetadataFetcher,
                                         SchemaBranchCache schemaBranchCache) {
        this.storageManager = storageManager;
        this.schemaMetadataFetcher = schemaMetadataFetcher;
        this.schemaBranchCache = schemaBranchCache;
        SchemaVersionLifecycleStateMachine.Builder builder = SchemaVersionLifecycleStateMachine.newBuilder();

        DefaultSchemaRegistry.Options options = new DefaultSchemaRegistry.Options(props);
        schemaVersionRetriever = createSchemaVersionRetriever();

        schemaVersionInfoCache = new SchemaVersionInfoCache(
                schemaVersionRetriever,
                options.getMaxSchemaCacheSize(),
                options.getSchemaExpiryInSecs());

        customSchemaStateExecutor = createSchemaReviewExecutor(props, builder);

        schemaVersionLifecycleStateMachine = builder.build();
    }

    private CustomSchemaStateExecutor createSchemaReviewExecutor(Map<String, Object> props,
                                                                 SchemaVersionLifecycleStateMachine.Builder builder) {
        Map<String, Object> schemaReviewExecConfig = (Map<String, Object>) props.getOrDefault("customSchemaStateExecutor",
                                                                                              Collections.emptyMap());
        String className = (String) schemaReviewExecConfig.getOrDefault("className", DEFAULT_SCHEMA_REVIEW_EXECUTOR_CLASS);
        Map<String, ?> executorProps = (Map<String, ?>) schemaReviewExecConfig.getOrDefault("props", Collections.emptyMap());
        CustomSchemaStateExecutor customSchemaStateExecutor;
        try {
            customSchemaStateExecutor = (CustomSchemaStateExecutor) Class.forName(className,
                                                                                  true,
                                                                                  Thread.currentThread().getContextClassLoader())
                                                                         .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            LOG.error("Error encountered while loading class [{}]", className, e);
            throw new IllegalArgumentException(e);
        }

        customSchemaStateExecutor.init(builder,
                                       SchemaVersionLifecycleStates.REVIEWED.getId(),
                                       SchemaVersionLifecycleStates.CHANGES_REQUIRED.getId(),
                                       executorProps);

        return customSchemaStateExecutor;
    }

    public SchemaVersionLifecycleStateMachine getSchemaVersionLifecycleStateMachine() {
        return schemaVersionLifecycleStateMachine;
    }

    public SchemaVersionRetriever getSchemaVersionRetriever() {
        return schemaVersionRetriever;
    }

    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion,
                                            Function<SchemaMetadata, Long> registerSchemaMetadataFn)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        SchemaVersionInfo schemaVersionInfo;
        String schemaName = schemaMetadata.getName();
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo retrievedschemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        Long schemaMetadataId;
        if (retrievedschemaMetadataInfo != null) {
            schemaMetadataId = retrievedschemaMetadataInfo.getId();
            // check whether the same schema text exists
            schemaVersionInfo = getSchemaVersionInfo(schemaName, schemaVersion.getSchemaText());
            if (schemaVersionInfo == null) {
                schemaVersionInfo = createSchemaVersion(schemaBranchName,
                                                        schemaMetadata,
                                                        retrievedschemaMetadataInfo.getId(),
                                                        schemaVersion);

            }
        } else {
            schemaMetadataId = registerSchemaMetadataFn.apply(schemaMetadata);
            schemaVersionInfo = createSchemaVersion(schemaBranchName,
                                                    schemaMetadata,
                                                    schemaMetadataId,
                                                    schemaVersion);
        }

        return new SchemaIdVersion(schemaMetadataId, schemaVersionInfo.getVersion(), schemaVersionInfo.getId());
    }

    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            String schemaName,
                                            SchemaVersion schemaVersion)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo != null) {
            return addSchemaVersion(schemaBranchName, schemaMetadataInfo, schemaVersion);
        } else {
            throw new SchemaNotFoundException("SchemaMetadata not found with the schemaName: " + schemaName);
        }
    }

    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            SchemaMetadataInfo schemaMetadataInfo,
                                            SchemaVersion schemaVersion)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        SchemaVersionInfo schemaVersionInfo;
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        // check whether the same schema text exists
        schemaVersionInfo = findSchemaVersion(schemaBranchName, schemaMetadata.getType(), schemaVersion.getSchemaText(), schemaMetadataInfo
                    .getId());
        if (schemaVersionInfo == null) {
            schemaVersionInfo = createSchemaVersion(schemaBranchName,
                                                    schemaMetadata,
                                                    schemaMetadataInfo.getId(),
                                                    schemaVersion);
        }

        return new SchemaIdVersion(schemaMetadataInfo.getId(), schemaVersionInfo.getVersion(), schemaVersionInfo.getId());
    }

    public SchemaVersionInfo getLatestEnabledSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        return getLatestSchemaVersionInfo(schemaBranchName, schemaName, SchemaVersionLifecycleStates.ENABLED.getId());
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        return getLatestSchemaVersionInfo(schemaBranchName, schemaName, null);
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName,
                                                        Byte stateId) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaBranchName, schemaName);

        SchemaVersionInfo latestSchema = null;
        if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
            for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                if (stateId == null || schemaVersionInfo.getStateId().equals(stateId)) {
                    latestSchema = schemaVersionInfo;
                    break;
                }
            }
        }

        return latestSchema;
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return getLatestSchemaVersionInfo(schemaName,(Byte) null);
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName,
                                                        Byte stateId) throws SchemaNotFoundException  {
        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaName);

        SchemaVersionInfo latestSchema = null;
        if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
            for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                if (stateId == null || schemaVersionInfo.getStateId().equals(stateId)) {
                    latestSchema = schemaVersionInfo;
                    break;
                }
            }
        }

        return latestSchema;
    }

    private SchemaVersionInfo createSchemaVersion(String schemaBranchName,
                                                  SchemaMetadata schemaMetadata,
                                                  Long schemaMetadataId,
                                                  SchemaVersion schemaVersion)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "schemaBranchName must not be null");
        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        String type = schemaMetadata.getType();
        if (getSchemaProvider(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchName));

        // generate fingerprint, it parses the schema and checks for semantic validation.
        // throws InvalidSchemaException for invalid schemas.
        final String fingerprint = getFingerprint(type, schemaVersion.getSchemaText());
        final String schemaName = schemaMetadata.getName();

        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        final Long schemaVersionStorableId = storageManager.nextId(schemaVersionStorable.getNameSpace());
        schemaVersionStorable.setId(schemaVersionStorableId);
        schemaVersionStorable.setSchemaMetadataId(schemaMetadataId);

        schemaVersionStorable.setFingerprint(fingerprint);

        schemaVersionStorable.setName(schemaName);

        schemaVersionStorable.setSchemaText(schemaVersion.getSchemaText());
        schemaVersionStorable.setDescription(schemaVersion.getDescription());
        schemaVersionStorable.setTimestamp(System.currentTimeMillis());

        schemaVersionStorable.setState(DEFAULT_VERSION_STATE.getId());

        if (!schemaBranchName.equals(SchemaBranch.MASTER_BRANCH)) {
            schemaVersion.setState(SchemaVersionLifecycleStates.INITIATED.getId());
            schemaVersion.setStateDetails(null);
        }

        // take a lock for a schema with same name.
        int retryCt = 0;
        while (true) {
            try {
                Integer version = 0;
                Byte initialState = schemaVersion.getInitialState();
                if (schemaMetadata.isEvolve()) {
                    // if the given version is added with enabled or initiated state then only check for compatibility
                    if (SchemaVersionLifecycleStates.ENABLED.getId().equals(initialState) ||
                            SchemaVersionLifecycleStates.INITIATED.getId().equals(initialState)) {
                        CompatibilityResult compatibilityResult = checkCompatibility(schemaBranchName, schemaName, schemaVersion.getSchemaText());
                        if (!compatibilityResult.isCompatible()) {
                            String errMsg = String.format("Given schema is not compatible with latest schema versions. \n" +
                                            "Error location: [%s] \n" +
                                            "Error encountered is: [%s]",
                                    compatibilityResult.getErrorLocation(),
                                    compatibilityResult.getErrorMessage());
                            LOG.error(errMsg);
                            throw new IncompatibleSchemaException(errMsg);
                        }
                    }
                    SchemaVersionInfo latestSchemaVersionInfo = getLatestSchemaVersionInfo(schemaName);
                    if (latestSchemaVersionInfo != null) {
                        version = latestSchemaVersionInfo.getVersion();
                    }
                }
                schemaVersionStorable.setVersion(version + 1);

                storageManager.add(schemaVersionStorable);
                updateSchemaVersionState(schemaVersionStorable.getId(), initialState, schemaVersion.getStateDetails());

                break;
            } catch (StorageException e) {
                // optimistic to try the next try would be successful. When retry attempts are exhausted, throw error back to invoker.
                if (++retryCt == DEFAULT_RETRY_CT) {
                    LOG.error("Giving up after retry attempts [{}] while trying to add new version of schema with metadata [{}]", retryCt, schemaMetadata, e);
                    throw e;
                }
                LOG.debug("Encountered storage exception while trying to add a new version, attempting again : [{}] with error: [{}]", retryCt, e);
            }
        }

        // fetching this as the ID may have been set by storage manager.
        Long schemaInstanceId = schemaVersionStorable.getId();

        SchemaBranchVersionMapping schemaBranchVersionMapping = new SchemaBranchVersionMapping(schemaBranch.getId(), schemaInstanceId);
        storageManager.add(schemaBranchVersionMapping);

        String storableNamespace = new SchemaFieldInfoStorable().getNameSpace();
        List<SchemaFieldInfo> schemaFieldInfos = getSchemaProvider(type).generateFields(schemaVersionStorable.getSchemaText());
        for (SchemaFieldInfo schemaFieldInfo : schemaFieldInfos) {
            final Long fieldInstanceId = storageManager.nextId(storableNamespace);
            SchemaFieldInfoStorable schemaFieldInfoStorable = SchemaFieldInfoStorable.fromSchemaFieldInfo(schemaFieldInfo, fieldInstanceId);
            schemaFieldInfoStorable.setSchemaInstanceId(schemaInstanceId);
            schemaFieldInfoStorable.setTimestamp(System.currentTimeMillis());
            storageManager.add(schemaFieldInfoStorable);
        }

        return schemaVersionStorable.toSchemaVersionInfo();
    }

    private void updateSchemaVersionState(Long schemaVersionId, Byte initialState, byte[] stateDetails) throws SchemaNotFoundException {
        try {
            SchemaVersionLifecycleContext schemaVersionLifecycleContext =
                    new SchemaVersionLifecycleContext(schemaVersionId,
                                                      1,
                                                      createSchemaVersionService(),
                                                      schemaVersionLifecycleStateMachine,
                                                      customSchemaStateExecutor);
            schemaVersionLifecycleContext.setState(schemaVersionLifecycleStateMachine.getStates().get(initialState));
            schemaVersionLifecycleContext.setDetails(stateDetails);
            schemaVersionLifecycleContext.updateSchemaVersionState();
        } catch (SchemaLifecycleException e) {
            throw new RuntimeException(e);
        }
    }

    private SchemaProvider getSchemaProvider(String type) {
        return schemaMetadataFetcher.getSchemaProvider(type);
    }

    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchema) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        SchemaValidationLevel validationLevel = schemaMetadata.getValidationLevel();
        CompatibilityResult compatibilityResult = null;
        switch (validationLevel) {
            case LATEST:
                SchemaVersionInfo latestSchemaVersionInfo = getLatestEnabledSchemaVersionInfo(schemaBranchName, schemaName);
                if (latestSchemaVersionInfo != null) {
                    compatibilityResult = checkCompatibility(schemaMetadata.getType(),
                                                             toSchema,
                                                             latestSchemaVersionInfo.getSchemaText(),
                                                             schemaMetadata.getCompatibility());
                    if (!compatibilityResult.isCompatible()) {
                        LOG.info("Received schema is not compatible with the latest schema versions [{}] with schema name [{}]",
                                 latestSchemaVersionInfo.getVersion(), schemaName);
                    }
                }
                break;
            case ALL:
                Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaBranchName, schemaName);
                for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                    if (SchemaVersionLifecycleStates.ENABLED.getId().equals(schemaVersionInfo.getStateId())) {
                        compatibilityResult = checkCompatibility(schemaMetadata.getType(),
                                                                 toSchema,
                                                                 schemaVersionInfo.getSchemaText(),
                                                                 schemaMetadata.getCompatibility());
                        if (!compatibilityResult.isCompatible()) {
                            LOG.info("Received schema is not compatible with one of the schema versions [{}] with schema name [{}]",
                                     schemaVersionInfo.getVersion(), schemaName);
                            break;
                        }
                    }
                }
                break;
        }
        return compatibilityResult != null ? compatibilityResult : CompatibilityResult.createCompatibleResult(toSchema);
    }

    private CompatibilityResult checkCompatibility(String type,
                                                   String toSchema,
                                                   String existingSchema,
                                                   SchemaCompatibility compatibility) {
        SchemaProvider schemaProvider = getSchemaProvider(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.checkCompatibility(toSchema, existingSchema, compatibility);
    }

    public Collection<SchemaVersionInfo> getAllVersions(final String schemaBranchName, final String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        Collection<SchemaVersionInfo> schemaVersionInfos;

        if(schemaBranchName.equals(SchemaBranch.MASTER_BRANCH)) {
            List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.NAME, schemaName));
            Collection<SchemaVersionStorable> storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams,
                    Collections.singletonList(OrderByField.of(SchemaVersionStorable.VERSION, true)));
            Set<Long> schemaVersionIds = getSortedSchemaVersions(schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchName))).
                    stream().map(schemaVersionInfo -> schemaVersionInfo.getId()).collect(Collectors.toSet());

            if (storables != null && !storables.isEmpty() && schemaVersionIds !=null && !schemaVersionIds.isEmpty()) {
                    schemaVersionInfos = storables
                            .stream()
                            .filter(schemaVersionInfo -> schemaVersionIds.contains(schemaVersionInfo.getId()))
                            .map(SchemaVersionStorable::toSchemaVersionInfo)
                            .collect(Collectors.toList());
            } else {
                schemaVersionInfos = Collections.emptyList();
            }
        } else  {
            schemaVersionInfos = getSortedSchemaVersions(schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchName)));
            if(schemaVersionInfos == null || schemaVersionInfos.isEmpty())
                 schemaVersionInfos = Collections.emptyList();
        }

        return schemaVersionInfos;
    }

    public Collection<SchemaVersionInfo> getAllVersions(final String schemaName) throws SchemaNotFoundException {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.NAME, schemaName));

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema not found with name " + schemaName);
        }

        Collection<SchemaVersionStorable> storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams,
                Collections.singletonList(OrderByField.of(SchemaVersionStorable.VERSION, true)));
        List<SchemaVersionInfo> schemaVersionInfos;
        if (storables != null && !storables.isEmpty()) {
            schemaVersionInfos = storables
                    .stream()
                    .map(SchemaVersionStorable::toSchemaVersionInfo)
                    .collect(Collectors.toList());
        } else {
            schemaVersionInfos = Collections.emptyList();
        }
        return schemaVersionInfos;
    }

    private SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaMetadataFetcher.getSchemaMetadataInfo(schemaName);
    }

    public SchemaVersionInfo getSchemaVersionInfo(String schemaName,
                                                  String schemaText) throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaName);
        }

        Long schemaMetadataId = schemaMetadataInfo.getId();
        return findSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadataInfo.getSchemaMetadata().getType(), schemaText, schemaMetadataId);
    }

    private SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        StorableKey storableKey = new StorableKey(SchemaVersionStorable.NAME_SPACE, SchemaVersionStorable.getPrimaryKey(id));

        SchemaVersionStorable versionedSchema = storageManager.get(storableKey);
        if (versionedSchema == null) {
            throw new SchemaNotFoundException("No Schema version exists with id " + id);
        }
        return versionedSchema.toSchemaVersionInfo();
    }

    private SchemaVersionInfo findSchemaVersion(String schemaBranchName,
                                                String type,
                                                String schemaText,
                                                Long schemaMetadataId) throws InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        String fingerPrint = getFingerprint(type, schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                new QueryParam(SchemaVersionStorable.FINGERPRINT, fingerPrint));

        Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);

        SchemaVersionStorable schemaVersionStorable = null;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataId: [{}] and schemaText [{}]", schemaMetadataId, schemaText);
            }

            schemaVersionStorable = versionedSchemas.iterator().next();
        }

        if (schemaVersionStorable == null) {
            return null;
        } else {

            SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchName));
            final Long schemaVersionId = schemaVersionStorable.getId();

            if (getSortedSchemaVersions(schemaBranch).stream().filter( storable -> storable.getId().equals(schemaVersionId)).findAny().isPresent()) {
                return schemaVersionStorable.toSchemaVersionInfo();
            } else {
                return null;
            }
        }
    }

    private String getFingerprint(String type,
                                  String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        SchemaProvider schemaProvider = getSchemaProvider(type);
        return Hex.encodeHexString(schemaProvider.getFingerprint(schemaText));
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
    }

    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        SchemaVersionInfoCache.Key schemaVersionCacheKey = new SchemaVersionInfoCache.Key(schemaVersionKey);
        SchemaVersionInfo schemaVersionInfo = schemaVersionInfoCache.getSchema(schemaVersionCacheKey);
        schemaVersionInfoCache.invalidateSchema(schemaVersionCacheKey);
        storageManager.remove(createSchemaVersionStorableKey(schemaVersionInfo.getId()));
        deleteSchemaVersionBranchMapping(schemaVersionInfo.getId());
    }

    public SchemaIdVersion mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy) throws SchemaNotFoundException, IncompatibleSchemaException {

        try {
            SchemaVersionInfo schemaVersionInfo = getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));
            SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaVersionInfo.getName());
            Set<SchemaBranch> schemaBranches = getSchemaBranches(schemaVersionId);
            if (schemaBranches.size() > 1)
                throw new SchemaVersionMergeException(String.format("Can't determine a unique schema branch for schema version id : '%s' not found", schemaVersionId));
            else if (schemaBranches.size() == 0)
                throw new SchemaVersionMergeException(String.format("Schema version id : '%s' is not associated with any branch", schemaVersionId));
            Long schemaBranchId = schemaBranches.iterator().next().getId();
            SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchId));

            if (schemaVersionMergeStrategy.equals(SchemaVersionMergeStrategy.PESSIMISTIC)) {
                SchemaVersionInfo latestSchemaVersion = getLatestEnabledSchemaVersionInfo(SchemaBranch.MASTER_BRANCH, schemaMetadataInfo.getSchemaMetadata().getName());
                SchemaVersionInfo rootSchemaVersion = getRootVersion(schemaBranch);
                if (latestSchemaVersion.getId() != rootSchemaVersion.getId())
                    throw new SchemaVersionMergeException(String.format("The latest version of '%s' is different from the root version of the branch : '%s'",
                            SchemaBranch.MASTER_BRANCH, schemaMetadataInfo.getSchemaMetadata().getName()));

            }

            byte [] initializedStateDetails;
            try {
                initializedStateDetails = objectMapper.writeValueAsBytes(new InitializedStateDetails(schemaBranch.getName(), schemaVersionInfo.getId()));
            } catch (JsonProcessingException e) {
                 throw new RuntimeException(String.format("Failed to serialize intializedState for %s and %s",schemaBranch.getName(), schemaVersionInfo.getId()));
            }

            SchemaIdVersion schemaIdVersion = null;
            try {
                schemaIdVersion = addSchemaVersion(SchemaBranch.MASTER_BRANCH,
                        schemaMetadataInfo,
                        new SchemaVersion(schemaVersionInfo.getSchemaText(),
                                schemaVersionInfo.getDescription(),
                                SchemaVersionLifecycleStates.INITIATED.getId(),
                                initializedStateDetails)
                );
            } catch (InvalidSchemaException e) {
                throw new SchemaVersionMergeException(String.format("Failed to merge schema version : '%s'", schemaVersionId.toString()), e);
            }

            updateSchemaVersionState(schemaIdVersion.getSchemaVersionId(), SchemaVersionLifecycleStates.ENABLED.getId(), null);

            return schemaIdVersion;
        } catch (SchemaBranchNotFoundException e) {
            throw new SchemaVersionMergeException(String.format("Failed to merge schema version : '%s'",schemaVersionId.toString()),e);
        }
    }

    private ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState>
    createSchemaVersionLifeCycleContextAndState(Long schemaVersionId) throws SchemaNotFoundException {
        // get the current state from storage for the given versionID
        // we can use a query to get max value for the column for a given schema-version-id but StorageManager does not
        // have API to take custom queries.
        Collection<SchemaVersionStateStorable> schemaVersionStates =
                storageManager.find(SchemaVersionStateStorable.NAME_SPACE,
                                    Collections.singletonList(new QueryParam(SchemaVersionStateStorable.SCHEMA_VERSION_ID,
                                                                             schemaVersionId.toString())),
                                    Collections.singletonList(OrderByField.of(SchemaVersionStateStorable.SEQUENCE, true)));
        if (schemaVersionStates.isEmpty()) {
            throw new SchemaNotFoundException("No schema versions found with id " + schemaVersionId);
        }
        SchemaVersionStateStorable stateStorable = schemaVersionStates.iterator().next();

        SchemaVersionLifecycleState schemaVersionLifecycleState = schemaVersionLifecycleStateMachine.getStates()
                                                                                                    .get(stateStorable.getStateId());
        SchemaVersionService schemaVersionService = createSchemaVersionService();
        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(stateStorable.getSchemaVersionId(),
                                                                                  stateStorable.getSequence(),
                                                                                  schemaVersionService,
                                                                                  schemaVersionLifecycleStateMachine,
                                                                                  customSchemaStateExecutor);
        return new ImmutablePair<>(context, schemaVersionLifecycleState);
    }

    private SchemaVersionService createSchemaVersionService() {
        return new SchemaVersionService() {

            public void updateSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException {
                storeSchemaVersionState(schemaVersionLifecycleContext);
            }

            public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException {
                doDeleteSchemaVersion(schemaVersionId);
            }

            @Override
            public SchemaMetadataInfo getSchemaMetadata(long schemaVersionId) throws SchemaNotFoundException {
                SchemaVersionInfo schemaVersionInfo = getSchemaVersionInfo(schemaVersionId);
                return getSchemaMetadataInfo(schemaVersionInfo.getName());
            }

            @Override
            public SchemaVersionInfo getSchemaVersionInfo(long schemaVersionId) throws SchemaNotFoundException {
                return SchemaVersionLifecycleManager.this.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));
            }

            @Override
            public CompatibilityResult checkForCompatibility(SchemaMetadata schemaMetadata,
                                                             String toSchemaText,
                                                             String existingSchemaText) {
                return checkCompatibility(schemaMetadata.getType(), toSchemaText, existingSchemaText, schemaMetadata.getCompatibility());
            }

            @Override
            public Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {
                return getAllVersions(schemaBranchName, schemaName);
            }
        };
    }

    private void storeSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException {
        // store versions state, sequence
        SchemaVersionStateStorable stateStorable = new SchemaVersionStateStorable();
        Long schemaVersionId = schemaVersionLifecycleContext.getSchemaVersionId();
        byte stateId = schemaVersionLifecycleContext.getState().getId();

        stateStorable.setSchemaVersionId(schemaVersionId);
        stateStorable.setSequence(schemaVersionLifecycleContext.getSequence() + 1);
        stateStorable.setStateId(stateId);
        stateStorable.setTimestamp(System.currentTimeMillis());
        stateStorable.setId(storageManager.nextId(SchemaVersionStateStorable.NAME_SPACE));
        try {
            stateStorable.setDetails(objectMapper.writeValueAsBytes(schemaVersionLifecycleContext.getDetails()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("Failed to persist state details for the schema version : '%s'", schemaVersionId.toString()), e);
        }

        storageManager.add(stateStorable);

        // store latest state in versions entity
        StorableKey storableKey = new StorableKey(SchemaVersionStorable.NAME_SPACE, SchemaVersionStorable.getPrimaryKey(schemaVersionId));
        SchemaVersionStorable versionedSchema = storageManager.get(storableKey);
        if (versionedSchema == null) {
            throw new SchemaNotFoundException("No Schema version exists with id " + schemaVersionId);
        }
        versionedSchema.setState(stateId);
        storageManager.addOrUpdate(versionedSchema);

        // invalidate schema version from cache
        SchemaVersionInfoCache.Key schemaVersionCacheKey = SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId));
        schemaVersionInfoCache.invalidateSchema(schemaVersionCacheKey);
    }

    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).enable(pair.getLeft());
    }

    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).delete(pair.getLeft());
    }

    private void doDeleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException {
        SchemaVersionInfoCache.Key schemaVersionCacheKey = SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId));
        schemaVersionInfoCache.invalidateSchema(schemaVersionCacheKey);
        storageManager.remove(createSchemaVersionStorableKey(schemaVersionId));
        deleteSchemaVersionBranchMapping(schemaVersionId);
    }

    private StorableKey createSchemaVersionStorableKey(Long id) {
        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        schemaVersionStorable.setId(id);
        return schemaVersionStorable.getStorableKey();
    }

    private void deleteSchemaVersionBranchMapping (Long schemaVersionId) throws SchemaNotFoundException {
        List<QueryParam> schemaVersionMappingStorableQueryParams = Lists.newArrayList();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, schemaVersionId.toString()));
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(OrderByField.of(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, false));

        Collection<SchemaBranchVersionMapping> storables = storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionMappingStorableQueryParams, orderByFields);

        if (storables == null || storables.isEmpty()) {
            LOG.debug("No need to delete schema version mapping as the database did a cascade delete");
            return;
        }

        if (storables.size() > 1)
            throw new RuntimeException(String.format("Schema version with id : '%s' is tied with more than one branch",schemaVersionId));

        storageManager.remove(new StorableKey(SchemaBranchVersionMapping.NAMESPACE, storables.iterator().next().getPrimaryKey()));
    }

    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).archive(pair.getLeft());
    }

    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).disable(pair.getLeft());
    }

    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).startReview(pair.getLeft());
    }

    public void executeState(Long schemaVersionId, Byte targetState)
            throws SchemaLifecycleException, SchemaNotFoundException {
        ImmutablePair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> schemaLifeCycleContextAndState =
                createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        SchemaVersionLifecycleContext schemaVersionLifecycleContext = schemaLifeCycleContextAndState.getLeft();
        SchemaVersionLifecycleState currentState = schemaLifeCycleContextAndState.getRight();

        schemaVersionLifecycleContext.setState(currentState);
        SchemaVersionLifecycleStateTransition transition =
                new SchemaVersionLifecycleStateTransition(currentState.getId(), targetState);
        SchemaVersionLifecycleStateAction action = schemaVersionLifecycleContext.getSchemaLifeCycleStatesMachine()
                                                                                .getTransitions()
                                                                                .get(transition);
        try {
            action.execute(schemaVersionLifecycleContext);
        } catch (SchemaLifecycleException e) {
            Throwable cause = e.getCause();
            if (cause != null && cause instanceof SchemaNotFoundException) {
                throw (SchemaNotFoundException) cause;
            }
            throw e;
        }
    }

    private SchemaVersionInfo retrieveSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        String schemaName = schemaVersionKey.getSchemaName();
        Integer version = schemaVersionKey.getVersion();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);

        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaName);
        }

        return fetchSchemaVersionInfo(schemaVersionKey.getSchemaName(), version);
    }

    private SchemaVersionInfo retrieveSchemaVersionInfo(SchemaIdVersion key) throws SchemaNotFoundException {
        SchemaVersionInfo schemaVersionInfo = null;
        if (key.getSchemaVersionId() != null) {
            schemaVersionInfo = fetchSchemaVersionInfo(key.getSchemaVersionId());
        } else if (key.getSchemaMetadataId() != null) {
            SchemaMetadataInfo schemaMetadataInfo = schemaMetadataFetcher.getSchemaMetadataInfo(key.getSchemaMetadataId());
            Integer version = key.getVersion();
            schemaVersionInfo = fetchSchemaVersionInfo(schemaMetadataInfo.getSchemaMetadata().getName(), version);
        } else {
            throw new IllegalArgumentException("Invalid SchemaIdVersion: " + key);
        }

        return schemaVersionInfo;
    }

    private SchemaVersionRetriever createSchemaVersionRetriever() {
        return new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return retrieveSchemaVersionInfo(key);
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                return retrieveSchemaVersionInfo(key);
            }
        };
    }

    private SchemaVersionInfo fetchSchemaVersionInfo(String schemaName,
                                                     Integer version) throws SchemaNotFoundException {
        LOG.info("##### fetching schema version for name: [{}] version: [{}]", schemaName, version);
        SchemaVersionInfo schemaVersionInfo = null;
        if (SchemaVersionKey.LATEST_VERSION.equals(version)) {
            schemaVersionInfo = getLatestSchemaVersionInfo(schemaName);
        } else {
            List<QueryParam> queryParams = Lists.newArrayList(
                    new QueryParam(SchemaVersionStorable.NAME, schemaName),
                    new QueryParam(SchemaVersionStorable.VERSION, version.toString()));

            Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                if (versionedSchemas.size() > 1) {
                    LOG.warn("More than one schema exists with name: [{}] and version [{}]", schemaName, version);
                }
                schemaVersionInfo = versionedSchemas.iterator().next().toSchemaVersionInfo();
            } else {
                throw new SchemaNotFoundException("No Schema version exists with name " + schemaName + " and version " + version);
            }
        }
        LOG.info("##### fetched schema version info [{}]", schemaVersionInfo);
        return schemaVersionInfo;
    }

    private Set<SchemaBranch> getSchemaBranches(Long schemaVersionId) throws SchemaBranchNotFoundException {
        List<QueryParam> schemaVersionMappingStorableQueryParams = new ArrayList<>();
        Set<SchemaBranch> schemaBranches = new HashSet<>();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, schemaVersionId.toString()));

        for (Storable storable : storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionMappingStorableQueryParams)) {
            schemaBranches.add(schemaBranchCache.get(SchemaBranchCache.Key.of(((SchemaBranchVersionMapping)storable).getSchemaBranchId())));
        }

        return schemaBranches;
    }

    private List<SchemaVersionInfo> getSortedSchemaVersions(Long schemaBranchId) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        List<QueryParam> schemaVersionMappingStorableQueryParams = Lists.newArrayList();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_BRANCH_ID, schemaBranchId.toString()));
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(OrderByField.of(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, false));
        List<SchemaVersionInfo> schemaVersionInfos = new ArrayList<>();

        Collection<SchemaBranchVersionMapping> storables = storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionMappingStorableQueryParams, orderByFields);
        if ( storables == null || storables.size() == 0 ) {
            if (schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchId)).getName().equals(SchemaBranch.MASTER_BRANCH))
                return Collections.emptyList();
            else
                throw new InvalidSchemaBranchVersionMapping(String.format("No schema versions are attached to the schema branch id : '%s'", schemaBranchId));
        }

        for (SchemaBranchVersionMapping storable : storables ) {
            SchemaIdVersion schemaIdVersion = new SchemaIdVersion(storable.getSchemaVersionInfoId());
            schemaVersionInfos.add(schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion)));
        }

        return schemaVersionInfos;
    }

    public List<SchemaVersionInfo> getSortedSchemaVersions(SchemaBranch schemaBranch) throws SchemaNotFoundException {
        try {
            return getSortedSchemaVersions(schemaBranch.getId());
        } catch (SchemaBranchNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public SchemaVersionInfo getRootVersion(SchemaBranch schemaBranch) throws SchemaNotFoundException {

        if(schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH))
            throw new SchemaNotFoundException(String.format("There is no root schema version attached to the schema branch '%s'", schemaBranch.getName()));

        List<SchemaVersionInfo> sortedVersionInfo;
        try {
            sortedVersionInfo = getSortedSchemaVersions(schemaBranch.getId());
        } catch (SchemaBranchNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (sortedVersionInfo == null)
            throw new SchemaNotFoundException(String.format("There were no schema versions attached to schema branch '%s'",schemaBranch.getName()));
        return sortedVersionInfo.iterator().next();
    }

}
