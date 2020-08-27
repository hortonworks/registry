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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hortonworks.registries.schemaregistry.cache.SchemaBranchCache;
import com.hortonworks.registries.schemaregistry.cache.SchemaVersionInfoCache;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.CustomSchemaStateExecutor;
import com.hortonworks.registries.schemaregistry.state.InbuiltSchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleState;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachine;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionService;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class SchemaVersionLifecycleManager {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionLifecycleManager.class);

    public static final InbuiltSchemaVersionLifecycleState DEFAULT_VERSION_STATE = SchemaVersionLifecycleStates.INITIATED;
    private static final String DEFAULT_SCHEMA_REVIEW_EXECUTOR_CLASS = "com.hortonworks.registries.schemaregistry.state.DefaultCustomSchemaStateExecutor";

    protected final SchemaVersionLifecycleStateMachine schemaVersionLifecycleStateMachine;
    protected final SchemaBranchCache schemaBranchCache;
    protected final SchemaVersionRetriever schemaVersionRetriever;
    protected final SchemaVersionInfoCache schemaVersionInfoCache;
    protected final CustomSchemaStateExecutor customSchemaStateExecutor;

    public SchemaVersionLifecycleManager(Map<String, Object> props, SchemaBranchCache schemaBranchCache) {
        this.schemaBranchCache = schemaBranchCache;
        schemaVersionRetriever = createSchemaVersionRetriever();
        ISchemaRegistry.Options options = new ISchemaRegistry.Options(props);
        schemaVersionInfoCache = new SchemaVersionInfoCache(
                schemaVersionRetriever,
                options.getMaxSchemaCacheSize(),
                options.getSchemaExpiryInSecs() * 1000L);

        SchemaVersionLifecycleStateMachine.Builder builder = SchemaVersionLifecycleStateMachine.newBuilder();
        customSchemaStateExecutor = createSchemaReviewExecutor(props, builder);

        schemaVersionLifecycleStateMachine = builder.build();
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
                                            Function<SchemaMetadata, Long> registerSchemaMetadataFn,
                                            boolean disableCanonicalCheck)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(schemaMetadata, "schemaMetadata can't be null");
        Preconditions.checkNotNull(schemaVersion, "schemaVersion can't be null");

        checkSchemaText(schemaVersion.getSchemaText());

        //
        SchemaVersionInfo schemaVersionInfo;
        String schemaName = schemaMetadata.getName();
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo retrievedschemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        Long schemaMetadataId;
        if (retrievedschemaMetadataInfo != null) {
            schemaMetadataId = retrievedschemaMetadataInfo.getId();
            // check whether the same schema text exists
            schemaVersionInfo = getSchemaVersionInfo(schemaName, schemaVersion.getSchemaText(), disableCanonicalCheck);
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

    @Nonnull
    protected abstract SchemaVersionInfo createSchemaVersion(String schemaBranchName,
                                                    SchemaMetadata schemaMetadata,
                                                    Long schemaMetadataId,
                                                    SchemaVersion schemaVersion)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;

    @Nonnull
    protected SchemaBranch getSchemaBranch(String schemaBranchName, SchemaMetadata schemaMetadata) throws SchemaNotFoundException {
        SchemaBranch schemaBranch = null;
        try {
            schemaBranch = schemaBranchCache.get(SchemaBranchCache.Key.of(new SchemaBranchKey(schemaBranchName, schemaMetadata.getName())));
        } catch (SchemaBranchNotFoundException e) {
            // Ignore this error
        }

        if (schemaBranch == null) {
            if (getAllVersions(schemaBranchName, schemaMetadata.getName()).size() != 0)
                throw new RuntimeException(String.format("Schema name : '%s' and branch name : '%s' has schema version, yet failed to obtain schema branch instance", schemaMetadata
                        .getName(), schemaBranchName));
            else
                throw new NullPointerException("Could not find branch " + schemaBranchName + " for schema " + schemaMetadata.getName());
        }

        return schemaBranch;
    }

    private void checkSchemaText(String schemaText) throws InvalidSchemaException {
        if(schemaText == null || schemaText.trim().isEmpty()) {
            throw new InvalidSchemaException();
        }
    }

    protected int checkEvolvability(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, String schemaBranchName) throws SchemaNotFoundException, IncompatibleSchemaException {
        int version = 0;
        Byte initialState = schemaVersion.getInitialState();
        if (schemaMetadata.isEvolve()) {
            // if the given version is added with enabled or initiated state then only check for compatibility
            if (SchemaVersionLifecycleStates.ENABLED.getId().equals(initialState) ||
                    SchemaVersionLifecycleStates.INITIATED.getId().equals(initialState)) {
                CompatibilityResult compatibilityResult = checkCompatibility(schemaBranchName, schemaMetadata.getName(), schemaVersion.getSchemaText());
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
            SchemaVersionInfo latestSchemaVersionInfo = getLatestSchemaVersionInfo(schemaMetadata.getName());
            if (latestSchemaVersionInfo != null) {
                version = latestSchemaVersionInfo.getVersion();
            }
        }
        return version;
    }

    protected void updateSchemaVersionState(Long schemaVersionId,
                                          Integer sequence,
                                          Byte initialState,
                                          byte[] stateDetails) throws SchemaNotFoundException {
        try {
            SchemaVersionLifecycleContext schemaVersionLifecycleContext =
                    new SchemaVersionLifecycleContext(schemaVersionId,
                            sequence,
                            schemaVersionService,
                            schemaVersionLifecycleStateMachine,
                            customSchemaStateExecutor);
            schemaVersionLifecycleContext.setState(schemaVersionLifecycleStateMachine.getStates().get(initialState));
            schemaVersionLifecycleContext.setDetails(stateDetails);
            schemaVersionLifecycleContext.updateSchemaVersionState();
        } catch (SchemaLifecycleException e) {
            throw new RuntimeException(e);
        }
    }

    protected final SchemaVersionService schemaVersionService = new SchemaVersionService() {

        public void updateSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException {
            LOG.debug("Update schema version state for {}", schemaVersionLifecycleContext);
            storeSchemaVersionState(schemaVersionLifecycleContext);
        }

        public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
            LOG.debug("Delete schema version {}", schemaVersionId);
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
        public Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaBranchName,
                                                                  String schemaName)
                throws SchemaNotFoundException, SchemaBranchNotFoundException {

            return getAllVersions(schemaBranchName, schemaName);
        }
    };

    protected abstract void storeSchemaVersionState(SchemaVersionLifecycleContext schemaVersionLifecycleContext) throws SchemaNotFoundException;

    protected abstract void doDeleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException;

    @Nonnull
    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            String schemaName,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");
        Preconditions.checkNotNull(schemaVersion, "schemaVersion can't be null");

        LOG.debug("Add schema version {} to schema \"{}\"", schemaVersion, schemaName);

        checkSchemaText(schemaVersion.getSchemaText());

        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        LOG.debug("Get schema metadata: {}", schemaMetadataInfo);
        if (schemaMetadataInfo != null) {
            return addSchemaVersion(schemaBranchName, schemaMetadataInfo, schemaVersion, disableCanonicalCheck);
        } else {
            throw new SchemaNotFoundException("SchemaMetadata not found with the schemaName: " + schemaName);
        }
    }

    @Nonnull
    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            SchemaMetadataInfo schemaMetadataInfo,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(schemaMetadataInfo, "Schema metadata info was null.");
        Preconditions.checkNotNull(schemaMetadataInfo.getSchemaMetadata(), "Schema metadata was null.");
        checkSchemaText(schemaVersion.getSchemaText());

        LOG.debug("Add schema version {} branch {}", schemaVersion, schemaBranchName);

        SchemaVersionInfo schemaVersionInfo;
        // check whether there exists schema-metadata for schema-metadata-key
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        // check whether the same schema text exists
        schemaVersionInfo = findSchemaVersion(schemaBranchName, schemaMetadata.getType(), schemaVersion.getSchemaText(),
                schemaMetadataInfo.getSchemaMetadata().getName(), disableCanonicalCheck);
        if (schemaVersionInfo == null) {
            schemaVersionInfo = createSchemaVersion(schemaBranchName, schemaMetadata, schemaMetadataInfo.getId(), schemaVersion);
        }

        return new SchemaIdVersion(schemaMetadataInfo.getId(), schemaVersionInfo.getVersion(), schemaVersionInfo.getId());
    }

    public SchemaVersionInfo getLatestEnabledSchemaVersionInfo(String schemaBranchName,
                                                               String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");

        return getLatestSchemaVersionInfo(schemaBranchName, schemaName, SchemaVersionLifecycleStates.ENABLED.getId());
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName,
                                                        String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");

        return getLatestSchemaVersionInfo(schemaBranchName, schemaName, null);
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName,
                                                        String schemaName,
                                                        Byte stateId) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");

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
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");

        return getLatestSchemaVersionInfo(schemaName,(Byte) null);
    }

    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName, Byte stateId) throws SchemaNotFoundException {
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");

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

    public CompatibilityResult checkCompatibility(String schemaBranchName,
                                                  String schemaName,
                                                  String toSchema) throws SchemaNotFoundException, SchemaBranchNotFoundException {

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

    protected CompatibilityResult checkCompatibility(String type,
                                                   String toSchema,
                                                   String existingSchema,
                                                   SchemaCompatibility compatibility) {
        SchemaProvider schemaProvider = getSchemaProvider(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.checkCompatibility(toSchema, existingSchema, compatibility);
    }

    protected abstract SchemaProvider getSchemaProvider(String type);

    public Collection<SchemaVersionInfo> getAllVersions(final String schemaBranchName,
                                                        final String schemaName) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");

        Collection<SchemaVersionInfo> schemaVersionInfos;
        SchemaBranchKey schemaBranchKey = new SchemaBranchKey(schemaBranchName, schemaName);

        schemaVersionInfos = Lists.reverse(getSortedSchemaVersions(schemaBranchCache.get(SchemaBranchCache.Key.of(schemaBranchKey))));
        if (schemaVersionInfos == null || schemaVersionInfos.isEmpty())
            schemaVersionInfos = Collections.emptyList();

        return schemaVersionInfos;
    }


    public Collection<SchemaVersionInfo> getAllVersions(final String schemaBranchName,
                                                        final String schemaName,
                                                        final List<Byte> stateIds) throws SchemaNotFoundException, SchemaBranchNotFoundException {

        Preconditions.checkNotNull(schemaBranchName, "Schema branch name can't be null");
        Preconditions.checkNotNull(stateIds, "State Ids can't be null");

        Set<Byte> stateIdSet = new HashSet<>(stateIds);

        return getAllVersions(schemaBranchName, schemaName).stream().
                filter(schemaVersionInfo -> stateIdSet.contains(schemaVersionInfo.getStateId())).
                collect(Collectors.toList());
    }

    public abstract Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException;

    public SchemaVersionInfo getSchemaVersionInfo(String schemaName,
                                                  String schemaText,
                                                  boolean disableCanonicalCheck) throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No schema found for schema metadata key: " + schemaName);
        }

        return findSchemaVersion(SchemaBranch.MASTER_BRANCH,
                                 schemaMetadataInfo.getSchemaMetadata().getType(),
                                 schemaText,
                                 schemaName,
                                 disableCanonicalCheck);
    }

    protected abstract SchemaVersionInfo findSchemaVersion(String schemaBranchName,
                                                           String type,
                                                           String schemaText,
                                                           String schemaMetadataName,
                                                           boolean disableCanonicalCheck) throws InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException;

    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
    }

    public abstract SchemaVersionInfo findSchemaVersionInfoByFingerprint(String fingerprint) throws SchemaNotFoundException;

    protected String getFingerprint(String type, String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        SchemaProvider schemaProvider = getSchemaProvider(type);
        return Hex.encodeHexString(schemaProvider.getFingerprint(schemaText));
    }

    public abstract void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException;

    public abstract SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId,
                                                SchemaVersionMergeStrategy schemaVersionMergeStrategy,
                                                boolean disableCanonicalCheck) throws SchemaNotFoundException, IncompatibleSchemaException;

    @Nonnull
    public abstract SchemaVersionLifecycleContext createSchemaVersionLifeCycleContext(Long schemaVersionId,
                                                                      SchemaVersionLifecycleState schemaVersionLifecycleState) throws SchemaNotFoundException;

    public abstract void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException;

    public abstract void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException;

    public abstract void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException;

    public abstract void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException;

    public abstract void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException;

    public abstract void executeState(Long schemaVersionId, Byte targetState, byte[] transitionDetails)
            throws SchemaLifecycleException, SchemaNotFoundException;

    public abstract Set<SchemaBranch> getSchemaBranches(Long schemaVersionId) throws SchemaBranchNotFoundException;

    public List<SchemaVersionInfo> getSortedSchemaVersions(SchemaBranch schemaBranch) throws SchemaNotFoundException {
        Preconditions.checkNotNull(schemaBranch, "Schema branch can't be null");
        try {
            return getSortedSchemaVersions(schemaBranch.getId());
        } catch (SchemaBranchNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract List<SchemaVersionInfo> getSortedSchemaVersions(Long schemaBranchId) throws SchemaNotFoundException, SchemaBranchNotFoundException;

    public SchemaVersionInfo getRootVersion(SchemaBranch schemaBranch) throws SchemaNotFoundException {

        if (schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH)) {
            throw new SchemaNotFoundException(String.format("There is no root schema version attached to the schema branch '%s'",
                    schemaBranch.getName()));
        }

        List<SchemaVersionInfo> sortedVersionInfo;
        try {
            sortedVersionInfo = getSortedSchemaVersions(schemaBranch.getId());
        } catch (SchemaBranchNotFoundException e) {
            throw new RuntimeException(e);
        }

        if (sortedVersionInfo == null)
            throw new SchemaNotFoundException(String.format("There were no schema versions attached to schema branch '%s'",
                    schemaBranch.getName()));
        return sortedVersionInfo.iterator().next();
    }

    public void invalidateAllSchemaVersionCache() {
        schemaVersionInfoCache.invalidateAll();
    }

    public void invalidateSchemaVersionCache(SchemaVersionInfoCache.Key key) {
        schemaVersionInfoCache.invalidateSchema(key);
    }

    @SuppressWarnings("unchecked")
    protected CustomSchemaStateExecutor createSchemaReviewExecutor(Map<String, Object> props,
                                                                 SchemaVersionLifecycleStateMachine.Builder builder) {
        Map<String, Object> schemaReviewExecConfig = (Map<String, Object>) props.getOrDefault("customSchemaStateExecutor",
                Collections.emptyMap());
        String className = (String) schemaReviewExecConfig.getOrDefault("className", DEFAULT_SCHEMA_REVIEW_EXECUTOR_CLASS);
        Map<String, ?> executorProps = (Map<String, ?>) schemaReviewExecConfig.getOrDefault("props", Collections.emptyMap());
        CustomSchemaStateExecutor customSchemaStateExecutor;
        try {
            customSchemaStateExecutor = (CustomSchemaStateExecutor) Class.forName(className,
                    true,
                    Thread.currentThread().getContextClassLoader()).newInstance();
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

    protected SchemaVersionRetriever createSchemaVersionRetriever() {
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

    protected abstract SchemaMetadataInfo getSchemaMetadataInfo(String schemaName);

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
            SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(key.getSchemaMetadataId());
            Integer version = key.getVersion();
            schemaVersionInfo = fetchSchemaVersionInfo(schemaMetadataInfo.getSchemaMetadata().getName(), version);
        } else {
            throw new IllegalArgumentException("Invalid SchemaIdVersion: " + key);
        }

        return schemaVersionInfo;
    }

    public abstract SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException;

    protected abstract SchemaVersionInfo fetchSchemaVersionInfo(String schemaName, Integer version) throws SchemaNotFoundException;

    protected abstract SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId);

}
