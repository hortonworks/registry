/*
 * Copyright 2016-2019 Cloudera, Inc.
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.schemaregistry.cache.SchemaBranchCache;
import com.hortonworks.registries.schemaregistry.cache.SchemaVersionInfoCache;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchVersionMapping;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaVersionMergeException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.state.InbuiltSchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateAction;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateTransition;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateTransitionListener;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.details.InitializedStateDetails;
import com.hortonworks.registries.schemaregistry.utils.ObjectMapperUtils;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
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
public class DefaultSchemaVersionLifecycleManager extends SchemaVersionLifecycleManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaVersionLifecycleManager.class);

    private static final List<SchemaVersionLifecycleStateTransitionListener> DEFAULT_LISTENERS = new ArrayList<>();

    private StorageManager storageManager;
    private DefaultSchemaRegistry.SchemaMetadataFetcher schemaMetadataFetcher;

    public DefaultSchemaVersionLifecycleManager(StorageManager storageManager,
                                         Map<String, Object> props,
                                         DefaultSchemaRegistry.SchemaMetadataFetcher schemaMetadataFetcher,
                                         SchemaBranchCache schemaBranchCache) {
        super(props, schemaBranchCache);

        this.storageManager = storageManager;
        this.schemaMetadataFetcher = schemaMetadataFetcher;
    }

    @Override @Nonnull
    protected SchemaVersionInfo createSchemaVersion(String schemaBranchName,
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

        SchemaBranch schemaBranch = getSchemaBranch(schemaBranchName, schemaMetadata);

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

        Byte initialState = schemaVersion.getInitialState();
        int version = checkEvolvability(schemaMetadata, schemaVersion, schemaBranchName);

        schemaVersionStorable.setVersion(version + 1);

        storageManager.add(schemaVersionStorable);
        updateSchemaVersionState(schemaVersionStorable.getId(), 1, initialState, schemaVersion.getStateDetails());

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



    @Override
    protected SchemaProvider getSchemaProvider(String type) {
        return schemaMetadataFetcher.getSchemaProvider(type);
    }

    @Override
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

    @Override
    protected SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaMetadataFetcher.getSchemaMetadataInfo(schemaName);
    }

    @Override
    protected SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaMetadataFetcher.getSchemaMetadataInfo(schemaMetadataId);
    }

    @Override
    public SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        StorableKey storableKey = new StorableKey(SchemaVersionStorable.NAME_SPACE, SchemaVersionStorable.getPrimaryKey(id));

        SchemaVersionStorable versionedSchema = storageManager.get(storableKey);
        if (versionedSchema == null) {
            throw new SchemaNotFoundException("No Schema version exists with id " + id);
        }
        return versionedSchema.toSchemaVersionInfo();
    }

    protected SchemaVersionInfo findSchemaVersion(String schemaBranchName,
                                                String type,
                                                String schemaText,
                                                String schemaMetadataName,
                                                boolean disableCanonicalCheck) throws InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        String fingerPrint = getFingerprint(type, schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);
        List<QueryParam> queryParams = Lists.newArrayList(
                new QueryParam(SchemaVersionStorable.NAME, schemaMetadataName),
                new QueryParam(SchemaVersionStorable.FINGERPRINT, fingerPrint));

        Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);

        Map<Long, SchemaVersionStorable> matchedSchemaVersionMap = null;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataName: [{}] and schemaText [{}]", schemaMetadataName, schemaText);
            }

            matchedSchemaVersionMap = versionedSchemas.stream().collect(Collectors.toMap(SchemaVersionStorable::getId, v -> v));
        }

        if (matchedSchemaVersionMap == null) {
            return null;
        } else {
            SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(new SchemaBranchKey(schemaBranchName, schemaMetadataName)));
            SchemaVersionInfo matchedSchemaVersionInfo = null;

            // If the disableCanonicalCheck is set to false, then return the lastest schema version that matches the fingerprint
            for (SchemaVersionInfo schemaVersionInfo : getSortedSchemaVersions(schemaBranch)) {
                if (matchedSchemaVersionMap.containsKey(schemaVersionInfo.getId())) {
                    if (!disableCanonicalCheck || schemaVersionInfo.getSchemaText().equals(schemaText)) {
                        matchedSchemaVersionInfo = schemaVersionInfo;
                    }
                }
            }

            return matchedSchemaVersionInfo;
        }
    }

    @Override
    public SchemaVersionInfo findSchemaVersionInfoByFingerprint(final String fingerprint) throws SchemaNotFoundException {
        final List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.FINGERPRINT, fingerprint));
        final List<OrderByField> orderParams = Collections.singletonList(OrderByField.of(SchemaVersionStorable.TIMESTAMP, true));

        final Collection<SchemaVersionStorable> schemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams, orderParams);

        if (schemas.isEmpty()) {
            throw new SchemaNotFoundException(String.format("No schema found for fingerprint: %s", fingerprint));
        } else {
            if (schemas.size() > 1) {
                LOG.warn(String.format("Multiple schemas found for the same fingerprint: %s", fingerprint));
            }

            return schemas.stream()
                    .findFirst()
                    .get()
                    .toSchemaVersionInfo();
        }
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        SchemaVersionInfoCache.Key schemaVersionCacheKey = new SchemaVersionInfoCache.Key(schemaVersionKey);
        SchemaVersionInfo schemaVersionInfo = schemaVersionInfoCache.getSchema(schemaVersionCacheKey);
        invalidateSchemaVersionCache(schemaVersionCacheKey);
        storageManager.remove(createSchemaVersionStorableKey(schemaVersionInfo.getId()));
        deleteSchemaVersionBranchMapping(schemaVersionInfo.getId());
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId,
                                                       SchemaVersionMergeStrategy schemaVersionMergeStrategy,
                                                       boolean disableCanonicalCheck) throws SchemaNotFoundException, IncompatibleSchemaException {

        try {
            SchemaVersionInfo schemaVersionInfo = getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));
            SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaVersionInfo.getName());

            Set<SchemaBranch> schemaBranches = getSchemaBranches(schemaVersionId).stream().filter(schemaBranch -> {
                try {
                    return !getRootVersion(schemaBranch).getId().equals(schemaVersionId);
                } catch (SchemaNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toSet());

            if (schemaBranches.size() > 1) {
                throw new SchemaVersionMergeException(String.format("Can't determine a unique schema branch for schema version id : '%s'", schemaVersionId));
            } else if (schemaBranches.size() == 0) {
                throw new SchemaVersionMergeException(String.format("Schema version id : '%s' is not associated with any branch", schemaVersionId));
            }

            Long schemaBranchId = schemaBranches.iterator().next().getId();
            SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchId));

            if (schemaVersionMergeStrategy.equals(SchemaVersionMergeStrategy.PESSIMISTIC)) {
                SchemaVersionInfo latestSchemaVersion = getLatestEnabledSchemaVersionInfo(SchemaBranch.MASTER_BRANCH,
                                                                                          schemaMetadataInfo.getSchemaMetadata()
                                                                                                            .getName());
                SchemaVersionInfo rootSchemaVersion = getRootVersion(schemaBranch);
                if (!latestSchemaVersion.getId().equals(rootSchemaVersion.getId())) {
                    throw new SchemaVersionMergeException(String.format("The latest version of '%s' is different from the root version of the branch : '%s'",
                                                                        SchemaBranch.MASTER_BRANCH, schemaMetadataInfo.getSchemaMetadata()
                                                                                                                      .getName()));
                }
            }

            byte[] initializedStateDetails;
            try {
                initializedStateDetails = ObjectMapperUtils.serialize(new InitializedStateDetails(schemaBranch.getName(), schemaVersionInfo
                        .getId()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(String.format("Failed to serialize initializedState for %s and %s", schemaBranch
                        .getName(), schemaVersionInfo.getId()));
            }

            SchemaVersionInfo createdSchemaVersionInfo;
            try {
                SchemaVersionInfo existingSchemaVersionInfo = findSchemaVersion(SchemaBranch.MASTER_BRANCH,
                                                                                schemaMetadataInfo.getSchemaMetadata().getType(),
                                                                                schemaVersionInfo.getSchemaText(),
                                                                                schemaMetadataInfo.getSchemaMetadata().getName(),
                                                                                disableCanonicalCheck);
                if (existingSchemaVersionInfo != null) {
                    String mergeMessage = String.format("Given version %d is already merged to master with version %d",
                                                        schemaVersionId, existingSchemaVersionInfo.getVersion());
                    LOG.info(mergeMessage);
                    return new SchemaVersionMergeResult(new SchemaIdVersion(schemaMetadataInfo.getId(),
                                                                            existingSchemaVersionInfo.getVersion(),
                                                                            existingSchemaVersionInfo.getId()),
                                                        mergeMessage);
                }

                createdSchemaVersionInfo = createSchemaVersion(SchemaBranch.MASTER_BRANCH,
                                                               schemaMetadataInfo.getSchemaMetadata(),
                                                               schemaMetadataInfo.getId(),
                                                               new SchemaVersion(schemaVersionInfo.getSchemaText(),
                                                                                 schemaVersionInfo.getDescription(),
                                                                                 SchemaVersionLifecycleStates.INITIATED.getId(),
                                                                                 initializedStateDetails));
            } catch (InvalidSchemaException e) {
                throw new SchemaVersionMergeException(String.format("Failed to merge schema version : '%s'", schemaVersionId
                        .toString()), e);
            }

            Collection<SchemaVersionStateStorable> schemaVersionStates =
                    storageManager.find(SchemaVersionStateStorable.NAME_SPACE,
                                        Collections.singletonList(new QueryParam(SchemaVersionStateStorable.SCHEMA_VERSION_ID,
                                                                                 schemaVersionId.toString())),
                                        Collections.singletonList(OrderByField.of(SchemaVersionStateStorable.SEQUENCE, true)));

            if (schemaVersionStates == null || schemaVersionStates.isEmpty()) {
                throw new RuntimeException(String.format("The database doesn't have any state transition recorded for the schema version id : '%s'", schemaVersionId));
            }

            updateSchemaVersionState(createdSchemaVersionInfo.getId(),
                                     schemaVersionStates.iterator().next().getSequence(),
                                     SchemaVersionLifecycleStates.ENABLED.getId(),
                                     null);

            String mergeMessage = String.format("Given version %d is merged successfully to master with version %d",
                                                schemaVersionId, createdSchemaVersionInfo.getVersion());
            LOG.info(mergeMessage);
            return new SchemaVersionMergeResult(new SchemaIdVersion(schemaMetadataInfo.getId(),
                                                                    createdSchemaVersionInfo.getVersion(),
                                                                    createdSchemaVersionInfo.getId()),
                                                mergeMessage);
        } catch (SchemaBranchNotFoundException e) {
            throw new SchemaVersionMergeException(String.format("Failed to merge schema version : '%s'", schemaVersionId
                    .toString()), e);
        }
    }

    private Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState>
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

        SchemaVersionLifecycleState schemaVersionLifecycleState = schemaVersionLifecycleStateMachine.getStates().get(stateStorable.getStateId());

        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(stateStorable.getSchemaVersionId(),
                                                                                  stateStorable.getSequence(),
                                                                                  schemaVersionService,
                                                                                  schemaVersionLifecycleStateMachine,
                                                                                  customSchemaStateExecutor);
        return ImmutablePair.of(context, schemaVersionLifecycleState);
    }

    @Override @Nonnull
    public SchemaVersionLifecycleContext createSchemaVersionLifeCycleContext(Long schemaVersionId,
                                                                             SchemaVersionLifecycleState schemaVersionLifecycleState) throws SchemaNotFoundException {
        // get the current state from storage for the given versionID
        // we can use a query to get max value for the column for a given schema-version-id but StorageManager does not
        // have API to take custom queries.

        List<QueryParam> queryParams = new ArrayList<>();
        queryParams.add(new QueryParam(SchemaVersionStateStorable.SCHEMA_VERSION_ID, schemaVersionId.toString()));
        queryParams.add(new QueryParam(SchemaVersionStateStorable.STATE, schemaVersionLifecycleState.getId()
                                                                                                    .toString()));

        Collection<SchemaVersionStateStorable> schemaVersionStates =
                storageManager.find(SchemaVersionStateStorable.NAME_SPACE,
                                    queryParams,
                                    Collections.singletonList(OrderByField.of(SchemaVersionStateStorable.SEQUENCE, true)));
        if (schemaVersionStates.isEmpty()) {
            throw new SchemaNotFoundException("No schema versions found with id " + schemaVersionId);
        }
        SchemaVersionStateStorable stateStorable = schemaVersionStates.iterator().next();

        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(stateStorable.getSchemaVersionId(),
                                                                                  stateStorable.getSequence(),
                                                                                  schemaVersionService,
                                                                                  schemaVersionLifecycleStateMachine,
                                                                                  customSchemaStateExecutor);
        context.setDetails(stateStorable.getDetails());
        return context;
    }

    @Override
    protected void storeSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException {
        // store versions state, sequence
        SchemaVersionStateStorable stateStorable = new SchemaVersionStateStorable();
        Long schemaVersionId = schemaVersionLifecycleContext.getSchemaVersionId();
        final SchemaVersionLifecycleState state = schemaVersionLifecycleContext.getState();
        final byte stateId = state.getId();

        stateStorable.setSchemaVersionId(schemaVersionId);
        stateStorable.setSequence(schemaVersionLifecycleContext.getSequence() + 1);
        stateStorable.setStateId(stateId);
        stateStorable.setTimestamp(System.currentTimeMillis());
        stateStorable.setDetails(schemaVersionLifecycleContext.getDetails());
        stateStorable.setId(storageManager.nextId(SchemaVersionStateStorable.NAME_SPACE));

        storageManager.add(stateStorable);

        // store latest state in versions entity
        StorableKey storableKey = new StorableKey(SchemaVersionStorable.NAME_SPACE, SchemaVersionStorable.getPrimaryKey(schemaVersionId));
        SchemaVersionStorable versionedSchema = storageManager.get(storableKey);
        if (versionedSchema == null) {
            throw new SchemaNotFoundException("No Schema version exists with id " + schemaVersionId);
        }
        versionedSchema.setState(state.getId());
        LOG.debug("New state for version {}: {}", versionedSchema.getVersion(), state.getName());
        storageManager.update(versionedSchema);

        // invalidate schema version from cache
        SchemaVersionInfoCache.Key schemaVersionCacheKey = SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId));
        invalidateSchemaVersionCache(schemaVersionCacheKey);
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).enable(pair.getLeft());
    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).delete(pair.getLeft());
    }

    protected void doDeleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        SchemaVersionInfoCache.Key schemaVersionCacheKey = SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId));
        invalidateSchemaVersionCache(schemaVersionCacheKey);
        storageManager.remove(createSchemaVersionStorableKey(schemaVersionId));
        deleteSchemaVersionBranchMapping(schemaVersionId);
    }

    private StorableKey createSchemaVersionStorableKey(Long id) {
        SchemaVersionStorable schemaVersionStorable = new SchemaVersionStorable();
        schemaVersionStorable.setId(id);
        return schemaVersionStorable.getStorableKey();
    }

    private void deleteSchemaVersionBranchMapping(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        List<QueryParam> schemaVersionMappingStorableQueryParams = Lists.newArrayList();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, schemaVersionId
                .toString()));
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(OrderByField.of(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, false));

        Collection<SchemaBranchVersionMapping> storables = storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionMappingStorableQueryParams, orderByFields);

        if (storables == null || storables.isEmpty()) {
            LOG.debug("No need to delete schema version mapping as the database did a cascade delete");
            return;
        }

        if (storables.size() > 1) {
            List<String> branchNamesTiedToSchema = storables.stream().map(storable -> schemaBranchCache.get(SchemaBranchCache.Key.of(storable.getSchemaBranchId())).getName()).collect(Collectors.toList());
            throw new SchemaLifecycleException(String.format("Schema version with id : '%s' is tied with more than one branch : '%s' ", schemaVersionId.toString(), Arrays.toString(branchNamesTiedToSchema.toArray())));
        }

        storageManager.remove(new StorableKey(SchemaBranchVersionMapping.NAMESPACE,
                                              storables.iterator()
                                                       .next()
                                                       .getPrimaryKey()));
    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).archive(pair.getLeft());
    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).disable(pair.getLeft());
    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> pair = createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        ((InbuiltSchemaVersionLifecycleState) pair.getRight()).startReview(pair.getLeft());
    }

    @Override
    public void executeState(Long schemaVersionId, Byte targetState, byte[] transitionDetails)
            throws SchemaLifecycleException, SchemaNotFoundException {
        Pair<SchemaVersionLifecycleContext, SchemaVersionLifecycleState> schemaLifeCycleContextAndState =
                createSchemaVersionLifeCycleContextAndState(schemaVersionId);
        SchemaVersionLifecycleContext schemaVersionLifecycleContext = schemaLifeCycleContextAndState.getLeft();
        SchemaVersionLifecycleState currentState = schemaLifeCycleContextAndState.getRight();

        schemaVersionLifecycleContext.setState(currentState);
        schemaVersionLifecycleContext.setDetails(transitionDetails);
        SchemaVersionLifecycleStateTransition transition =
                new SchemaVersionLifecycleStateTransition(currentState.getId(), targetState);
        SchemaVersionLifecycleStateAction action = schemaVersionLifecycleContext.getSchemaLifeCycleStatesMachine()
                                                                                .getTransitions()
                                                                                .get(transition);
        try {
            List<SchemaVersionLifecycleStateTransitionListener> listeners =
                    schemaVersionLifecycleContext.getSchemaLifeCycleStatesMachine()
                                                 .getListeners()
                                                 .getOrDefault(transition, DEFAULT_LISTENERS);

            listeners.stream().forEach(listener -> listener.preStateTransition(schemaVersionLifecycleContext));
            action.execute(schemaVersionLifecycleContext);
            listeners.stream().forEach(listener -> listener.postStateTransition(schemaVersionLifecycleContext));
        } catch (SchemaLifecycleException e) {
            Throwable cause = e.getCause();
            if (cause != null && cause instanceof SchemaNotFoundException) {
                throw (SchemaNotFoundException) cause;
            }
            throw e;
        }
    }

    @Override
    protected SchemaVersionInfo fetchSchemaVersionInfo(String schemaName, Integer version) throws SchemaNotFoundException {
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

    @Override
    public Set<SchemaBranch> getSchemaBranches(Long schemaVersionId) throws SchemaBranchNotFoundException {
        List<QueryParam> schemaVersionMappingStorableQueryParams = new ArrayList<>();
        Set<SchemaBranch> schemaBranches = new HashSet<>();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, schemaVersionId
                .toString()));

        for (Storable storable : storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionMappingStorableQueryParams)) {
            schemaBranches.add(schemaBranchCache.get(SchemaBranchCache.Key.of(((SchemaBranchVersionMapping) storable).getSchemaBranchId())));
        }

        return schemaBranches;
    }

    @Override
    protected List<SchemaVersionInfo> getSortedSchemaVersions(Long schemaBranchId) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        List<QueryParam> schemaVersionMappingStorableQueryParams = Lists.newArrayList();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_BRANCH_ID, schemaBranchId
                .toString()));
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(OrderByField.of(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, false));
        List<SchemaVersionInfo> schemaVersionInfos = new ArrayList<>();

        Collection<SchemaBranchVersionMapping> storables = storageManager.find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionMappingStorableQueryParams, orderByFields);
        if (storables == null || storables.size() == 0) {
            if (schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchId))
                                 .getName()
                                 .equals(SchemaBranch.MASTER_BRANCH))
                return Collections.emptyList();
            else
                throw new InvalidSchemaBranchVersionMapping(String.format("No schema versions are attached to the schema branch id : '%s'", schemaBranchId));
        }

        for (SchemaBranchVersionMapping storable : storables) {
            SchemaIdVersion schemaIdVersion = new SchemaIdVersion(storable.getSchemaVersionInfoId());
            schemaVersionInfos.add(schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion)));
        }

        return schemaVersionInfos;
    }

}
