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

import com.google.common.base.Preconditions;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaBranchKey;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionLifecycleManager;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.cache.SchemaBranchCache;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

// TODO CDPD-18843 Refactor this.
public abstract class AtlasSchemaVersionLifecycleManager extends SchemaVersionLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasSchemaVersionLifecycleManager.class);

    private final AtlasPlugin atlasClient;

    public AtlasSchemaVersionLifecycleManager(AtlasPlugin atlasClient, Map<String, Object> props, SchemaBranchCache schemaBranchCache) {
        super(props, schemaBranchCache);
        this.atlasClient = checkNotNull(atlasClient, "atlasClient");
    }

    @Override @Nonnull
    protected SchemaVersionInfo createSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, Long schemaMetadataId, SchemaVersion schemaVersion) throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        checkNotNull(schemaBranchName, "schemaBranchName must not be null");
        checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        String type = schemaMetadata.getType();
        if (getSchemaProvider(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        SchemaBranch schemaBranch = getSchemaBranch(schemaBranchName, schemaMetadata);

        // generate fingerprint, it parses the schema and checks for semantic validation.
        // throws InvalidSchemaException for invalid schemas.
        final String fingerprint = getFingerprint(type, schemaVersion.getSchemaText());
        final String schemaName = schemaMetadata.getName();

        if (!schemaBranchName.equals(SchemaBranch.MASTER_BRANCH)) {
            schemaVersion.setState(SchemaVersionLifecycleStates.INITIATED.getId());
            schemaVersion.setStateDetails(null);
        }

        Byte initialState = schemaVersion.getInitialState();
        // TODO version is fetched here too and also in atlasClient
        checkEvolvability(schemaMetadata, schemaVersion, schemaBranchName);

        schemaVersion.setState(DEFAULT_VERSION_STATE.getId());
        SchemaIdVersion schemaIdVersion = atlasClient.addSchemaVersion(schemaName, schemaVersion, fingerprint, schemaBranch);
        final Long vid = schemaIdVersion.getSchemaVersionId();

        updateSchemaVersionState(vid, 1, initialState, schemaVersion.getStateDetails());

        // TODO
        // extract fields from the Avro schema
        /*
        String storableNamespace = new SchemaFieldInfoStorable().getNameSpace();
        List<SchemaFieldInfo> schemaFieldInfos = getSchemaProvider(type).generateFields(schemaVersionStorable.getSchemaText());
        for (SchemaFieldInfo schemaFieldInfo : schemaFieldInfos) {
            final Long fieldInstanceId = storageManager.nextId(storableNamespace);
            SchemaFieldInfoStorable schemaFieldInfoStorable = SchemaFieldInfoStorable.fromSchemaFieldInfo(schemaFieldInfo, fieldInstanceId);
            schemaFieldInfoStorable.setSchemaInstanceId(schemaInstanceId);
            schemaFieldInfoStorable.setTimestamp(System.currentTimeMillis());
            storageManager.add(schemaFieldInfoStorable);
        }
         */

        return new SchemaVersionInfo(vid, schemaName, schemaIdVersion.getVersion(), schemaIdVersion.getSchemaMetadataId(),
                schemaVersion.getSchemaText(), System.currentTimeMillis(), schemaVersion.getDescription(),
                schemaVersion.getInitialState());
    }

    @Override
    protected void storeSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException {
        LOG.info("+++++++++++++ storeSchemaVersionState {}", schemaVersionLifecycleContext);
    }

    @Override
    protected void doDeleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("++++++++++++ doDeleteSchemaVersion {}", schemaVersionId);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        LOG.info("++++++++++++ getAllVersions {}", schemaName);
        try {
            return atlasClient.getAllSchemaVersions(schemaName);
        } catch (AtlasUncheckedException npex) {
            throw new SchemaNotFoundException("Schema not found with name " + schemaName, npex);
        }
    }

    @Override
    public SchemaVersionInfo findSchemaVersion(String schemaBranchName, String type, String schemaText, String schemaMetadataName, boolean disableCanonicalCheck) throws InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("++++++++++++++ findSchemaVersion {} {} {}", schemaMetadataName, schemaBranchName, type);

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        String fingerPrint = getFingerprint(type, schemaText);
        LOG.debug("Fingerprint of the given schema [{}] is [{}]", schemaText, fingerPrint);

        Collection<SchemaVersionInfo> versionedSchemas = atlasClient.searchVersions(schemaMetadataName, fingerPrint);

        Set<Long> versionIdSet = null;
        if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
            if (versionedSchemas.size() > 1) {
                LOG.warn("Exists more than one schema with schemaMetadataName: [{}] and schemaText [{}]", schemaMetadataName, schemaText);
            }

            versionIdSet = versionedSchemas.stream().map(SchemaVersionInfo::getId).collect(Collectors.toSet());
        }

        if (versionIdSet == null) {
            return null;
        } else {
            SchemaBranch schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(new SchemaBranchKey(schemaBranchName, schemaMetadataName)));
            SchemaVersionInfo matchedSchemaVersionInfo = null;

            // If the disableCanonicalCheck is set to false, then return the lastest schema version that matches the fingerprint
            for (SchemaVersionInfo schemaVersionInfo : getSortedSchemaVersions(schemaBranch)) {
                if (versionIdSet.contains(schemaVersionInfo.getId())) {
                    if (!disableCanonicalCheck || schemaVersionInfo.getSchemaText().equals(schemaText)) {
                        matchedSchemaVersionInfo = schemaVersionInfo;
                    }
                }
            }

            return matchedSchemaVersionInfo;
        }
    }

    @Override
    public SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        LOG.info("++++++++++++ fetchSchemaVersionInfo {}", id);
        Optional<SchemaVersionInfo> sv = atlasClient.getSchemaVersionById(id);
        if (!sv.isPresent()) {
            throw new SchemaNotFoundException("No Schema version exists with id " + id);
        }
        return sv.get();
    }

    @Override
    protected SchemaVersionInfo fetchSchemaVersionInfo(String schemaName, Integer version) throws SchemaNotFoundException {
        LOG.info("++++++++++++ fetchSchemaVersionInfo {} {}", schemaName, version);
        return null;
    }

    @Override
    public SchemaVersionInfo findSchemaVersionInfoByFingerprint(String fingerprint) throws SchemaNotFoundException {
        LOG.info("++++++++++++ findSchemaVersionInfoByFingerprint {}", fingerprint);
        return null;
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("++++++++++++ deleteSchemaVersion {}", schemaVersionKey);
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, SchemaVersionMergeStrategy schemaVersionMergeStrategy, boolean disableCanonicalCheck) throws SchemaNotFoundException, IncompatibleSchemaException {
        LOG.info("++++++++++++ mergeSchemaVersion {} {}", schemaVersionId, schemaVersionMergeStrategy);
        return null;
    }

    @Override @Nonnull
    public SchemaVersionLifecycleContext createSchemaVersionLifeCycleContext(Long schemaVersionId, SchemaVersionLifecycleState schemaVersionLifecycleState) throws SchemaNotFoundException {
        LOG.info("++++++++++++ createSchemaVersionLifeCycleContext {} {}", schemaVersionId, schemaVersionLifecycleState);

        // get the current state from storage for the given versionID
        // we can use a query to get max value for the column for a given schema-version-id but StorageManager does not
        // have API to take custom queries.

        /*
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

         */

        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(schemaVersionId,
                0, //stateStorable.getSequence(),
                schemaVersionService,
                schemaVersionLifecycleStateMachine,
                customSchemaStateExecutor);
        //context.setDetails(stateStorable.getDetails());
        /*
        no idea what this is but if null doesn't work then try this... I got it from mergeSVersion
        byte[] initializedStateDetails;
            try {
                initializedStateDetails = ObjectMapperUtils.serialize(new InitializedStateDetails(schemaBranch.getName(), schemaVersionInfo
                        .getId()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(String.format("Failed to serialize initializedState for %s and %s", schemaBranch
                        .getName(), schemaVersionInfo.getId()));
            }
         */
        return context;
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
        LOG.info("++++++++++++ enableSchemaVersion {}", schemaVersionId);
    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("++++++++++++ deleteSchemaVersion {}", schemaVersionId);
    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("++++++++++++ archiveSchemaVersion {}", schemaVersionId);
    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("++++++++++++ disableSchemaVersion {}", schemaVersionId);
    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        LOG.info("++++++++++++ startSchemaVersionReview {}", schemaVersionId);
    }

    @Override
    public void executeState(Long schemaVersionId, Byte targetState, byte[] transitionDetails) throws SchemaLifecycleException, SchemaNotFoundException {
        LOG.info("++++++++++++ executeState {} {}", schemaVersionId, targetState);
    }

    @Override
    public Set<SchemaBranch> getSchemaBranches(Long schemaVersionId) throws SchemaBranchNotFoundException {
        LOG.info("++++++++++++ getSchemaBranches {}", schemaVersionId);

        return new HashSet<>(atlasClient.getSchemaBranchesByVersionId(schemaVersionId));
    }

    @Override
    protected List<SchemaVersionInfo> getSortedSchemaVersions(Long schemaBranchId) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        LOG.info("++++++++++++ getSortedSchemaVersions {}", schemaBranchId);

        List<SchemaVersionInfo> versions = atlasClient.getSchemaVersionsByBranchId(schemaBranchId);
        versions.sort(Comparator.comparing(SchemaVersionInfo::getId));

        return Collections.unmodifiableList(versions);
    }


}
