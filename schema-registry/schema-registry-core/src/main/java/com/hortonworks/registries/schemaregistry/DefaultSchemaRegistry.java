/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.hortonworks.registries.common.CompatibilityConfig;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.exportimport.BulkUploadInputFormat;
import com.hortonworks.registries.schemaregistry.exportimport.BulkUploadService;
import com.hortonworks.registries.schemaregistry.exportimport.UploadResult;
import com.hortonworks.registries.schemaregistry.locks.Lock;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.details.InitializedStateDetails;
import com.hortonworks.registries.schemaregistry.state.details.MergeInfo;
import com.hortonworks.registries.schemaregistry.utils.ObjectMapperUtils;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.catalog.AbstractStorable;
import com.hortonworks.registries.storage.impl.jdbc.sequences.NamespaceSequenceStorable;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import com.hortonworks.registries.storage.search.WhereClauseCombiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Default implementation for schema registry.
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    public static final String ORDER_BY_FIELDS_PARAM_NAME = "_orderByFields";
    private static final Long DEFAULT_SCHEMA_LOCK_TIMEOUT_IN_SECS = 120L;

    private final StorageManager storageManager;
    private final FileStorage fileStorage;

    private final Map<String, SchemaProvider> schemaTypeWithProviders;
    private final List<SchemaProviderInfo> schemaProviderInfos;
    private final SchemaVersionLifecycleManager schemaVersionLifecycleManager;
    private final SchemaLockManager schemaLockManager;
    private final CompatibilityConfig compatibilityConfig;
    private final BulkUploadService bulkUploadService;

    @Inject
    public DefaultSchemaRegistry(RegistryConfiguration configuration,
                                 StorageManager storageManager,
                                 FileStorage fileStorage,
                                 Collection<Map<String, Object>> schemaProvidersConfig,
                                 SchemaLockManager schemaLockManager,
                                 CompatibilityConfig compatibilityConfig) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaLockManager = schemaLockManager;
        this.compatibilityConfig = compatibilityConfig;
        this.bulkUploadService = new BulkUploadService(this);

        storageManager.registerStorables(
                Arrays.asList(
                        NamespaceSequenceStorable.class,
                        SchemaMetadataStorable.class,
                        SchemaVersionStorable.class,
                        SchemaVersionStateStorable.class,
                        SchemaFieldInfoStorable.class,
                        SerDesInfoStorable.class,
                        SchemaSerDesMapping.class,
                        SchemaBranchStorable.class,
                        SchemaBranchVersionMapping.class,
                        SchemaLockStorable.class,
                        AtlasEventStorable.class
                ));

        SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        this.schemaVersionLifecycleManager = new DefaultSchemaVersionLifecycleManager(storageManager,
                configuration, schemaMetadataFetcher, this::getSchemaBranch, this::getSchemaBranch);

        Collection<SchemaProvider> schemaProviders = initSchemaProviders(schemaProvidersConfig, schemaVersionLifecycleManager.getSchemaVersionRetriever());

        this.schemaTypeWithProviders = schemaProviders.stream().collect(Collectors.toMap(SchemaProvider::getType, Function.identity()));

        this.schemaProviderInfos = Collections.unmodifiableList(
                schemaProviders.stream().map(schemaProvider ->
                        new SchemaProviderInfo(schemaProvider.getType(), schemaProvider.getName(),
                                schemaProvider.getDescription(), schemaProvider.getDefaultSerializerClassName(),
                                schemaProvider.getDefaultDeserializerClassName()))
                        .collect(Collectors.toList()));
    }

    private SchemaMetadataFetcher createSchemaMetadataFetcher() {
        return new SchemaMetadataFetcher() {

            @Override
            public SchemaMetadataInfo getSchemaMetadataInfo(
                    String schemaName) {
                return DefaultSchemaRegistry.this.getSchemaMetadataInfo(schemaName);
            }

            @Override
            public SchemaMetadataInfo getSchemaMetadataInfo(
                    Long schemaMetadataId) {
                return DefaultSchemaRegistry.this.getSchemaMetadataInfo(schemaMetadataId);
            }

            @Override
            public SchemaProvider getSchemaProvider(String providerType) {
                return schemaTypeWithProviders.get(providerType);
            }
        };
    }

    public interface SchemaMetadataFetcher {
        SchemaMetadataInfo getSchemaMetadataInfo(String schemaName);

        SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId);

        SchemaProvider getSchemaProvider(String providerType);
    }

    private Collection<SchemaProvider> initSchemaProviders(final Collection<Map<String, Object>> schemaProvidersConfig,
                                                                     final SchemaVersionRetriever schemaVersionRetriever) {
        if (schemaProvidersConfig == null || schemaProvidersConfig.isEmpty()) {
            throw new IllegalArgumentException("No [" + SCHEMA_PROVIDERS + "] property is configured in schema registry configuration file.");
        }

        final ImmutableList.Builder<SchemaProvider> result = ImmutableList.builder();
        for (Map<String, Object> schemaProviderConfig : schemaProvidersConfig) {
            String className = (String) schemaProviderConfig.get("providerClass");
            if (className == null || className.isEmpty()) {
                throw new IllegalArgumentException("Schema provider class name must be non empty, Invalid provider class name [" + className + "]");
            }

            try {
                SchemaProvider schemaProvider = (SchemaProvider) Class.forName(className, true, Thread.currentThread().getContextClassLoader()).newInstance();
                HashMap<String, Object> config = new HashMap<>(schemaProviderConfig);
                config.put(SchemaProvider.SCHEMA_VERSION_RETRIEVER_CONFIG, schemaVersionRetriever);
                schemaProvider.init(Collections.unmodifiableMap(config));

                result.add(schemaProvider);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                LOG.error("Error encountered while loading SchemaProvider [{}] ", className, e);
                throw new IllegalArgumentException(e);
            }
        }

        return result.build();
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return schemaProviderInfos;
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException {
        return addSchemaMetadata(schemaMetadata);
    }

    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException {
        return addSchemaMetadata(schemaMetadata, false);
    }

    public Long addSchemaMetadata(SchemaMetadata schemaMetadata,
                                  boolean throwErrorIfExists) throws UnsupportedSchemaTypeException {
        return addSchemaMetadata(null, schemaMetadata, throwErrorIfExists);
    }

    @Override
    public Long addSchemaMetadata(Long id, SchemaMetadata schemaMetadata) {
        return addSchemaMetadata(id, schemaMetadata, true);
    }

    private Long addSchemaMetadata(@Nullable Long metadataId, SchemaMetadata schemaMetadata, boolean throwErrorIfExists) {
        Pair<SchemaMetadataStorable, Boolean> metaAndWasNewlyAdded = createSchemaMetadata(metadataId, schemaMetadata, throwErrorIfExists);
        Boolean metaWasNewlyAdded = metaAndWasNewlyAdded.getRight();
        SchemaMetadataStorable metaStorable = metaAndWasNewlyAdded.getLeft();

        // Only add the MASTER branch if the metadata was newly added and not an already existing one was returned
        if (metaWasNewlyAdded) {
            SchemaBranchStorable schemaBranchStorable = new SchemaBranchStorable(SchemaBranch.MASTER_BRANCH,
                    schemaMetadata.getName(), String.format(SchemaBranch.MASTER_BRANCH_DESC, schemaMetadata.getName()), System.currentTimeMillis());
            Long branchId = storageManager.nextId(schemaBranchStorable.getNameSpace());
            branchId = checkIfIdIsTaken(SchemaBranchStorable.class, branchId);
            schemaBranchStorable.setId(branchId);
            storageManager.add(schemaBranchStorable);

            storageManager.add(new SchemaLockStorable(metaStorable.getNameSpace(),
                    metaStorable.getName(), System.currentTimeMillis()));
        }

        return metaStorable.getId();
    }
    
    @Override
    public Long addSchemaMetadataWithoutBranch(Supplier<Long> id, SchemaMetadata schemaMetadata, boolean throwErrorIfExists) {
        SchemaMetadataStorable metaStorable = createSchemaMetadata(id.get(), schemaMetadata, throwErrorIfExists).getLeft();
        return metaStorable.getId();
    }

    private Pair<SchemaMetadataStorable, Boolean> createSchemaMetadata(@Nullable Long id, SchemaMetadata schemaMetadata, boolean throwErrorIfExists) {
        if (schemaMetadata.getInternalCompatibility() == null) {
            schemaMetadata.setCompatibility(SchemaCompatibility.valueOf(compatibilityConfig.getAvroCompatibility()));
        }
        if (schemaMetadata.getInternalValidationLevel() == null) {
            schemaMetadata.setValidationLevel(SchemaValidationLevel.valueOf(compatibilityConfig.getValidationLevel()));
        }
        final SchemaMetadataStorable givenSchemaMetadataStorable = ensureJsonCompatibility(
                SchemaMetadataStorable.fromSchemaMetadataInfo(new SchemaMetadataInfo(schemaMetadata)));
        String type = schemaMetadata.getType();

        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        if (!throwErrorIfExists) {
            Storable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
            if (schemaMetadataStorable != null) {
                return Pair.of((SchemaMetadataStorable) schemaMetadataStorable, false);
            }
        }
        Long nextId = id != null ? id :
                storageManager.nextId(givenSchemaMetadataStorable.getNameSpace());
        if (id == null) {
            LOG.debug("Given id is null, id is generated");
            nextId = checkIfIdIsTaken(SchemaMetadataStorable.class, nextId);
        }
        givenSchemaMetadataStorable.setId(nextId);
        givenSchemaMetadataStorable.setTimestamp(System.currentTimeMillis());
        storageManager.add(givenSchemaMetadataStorable);

        return Pair.of(givenSchemaMetadataStorable, true);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setId(schemaMetadataId);

        List<QueryParam> params = Collections.singletonList(new QueryParam(SchemaMetadataStorable.ID, schemaMetadataId.toString()));
        Collection<SchemaMetadataStorable> schemaMetadataStorables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, params);
        SchemaMetadataInfo schemaMetadataInfo = null;
        if (schemaMetadataStorables != null && !schemaMetadataStorables.isEmpty()) {
            schemaMetadataInfo = schemaMetadataStorables.iterator().next().toSchemaMetadataInfo();
            if (schemaMetadataStorables.size() > 1) {
                LOG.warn("No unique entry with schemaMetatadataId: [{}]", schemaMetadataId);
            }
            LOG.info("SchemaMetadata entries with id [{}] is [{}]", schemaMetadataId, schemaMetadataStorables);
        }

        return schemaMetadataInfo;
    }

    @Override
    public void deleteSchema(String schemaName) {
        // Remove all serdes mappings for this schema name
        SchemaMetadataInfo schemaMetadataInfo = checkNotNull(getSchemaMetadataInfo(schemaName),
                "Could not find schema meta \"%s\"", schemaName);
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(schemaMetadataInfo.getId());
        if (schemaSerDesMappings != null) {
            for (SchemaSerDesMapping schemaSerDesMapping: schemaSerDesMappings) {
                storageManager.remove(schemaSerDesMapping.getStorableKey());
            }
        }
        // Finally remove the schema metadata entry that will remove other related entries on cascade at DB level
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setName(schemaName);
        storageManager.remove(schemaMetadataStorable.getStorableKey());

        SchemaLockStorable schemaLockStorable = new SchemaLockStorable(SchemaMetadataStorable.NAME_SPACE, schemaName);
        storageManager.remove(schemaLockStorable.getStorableKey());
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setName(schemaName);

        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());

        return schemaMetadataStorable != null ? schemaMetadataStorable.toSchemaMetadataInfo() : null;
    }

    public Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props)
            throws SchemaBranchNotFoundException, SchemaNotFoundException {

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

    @Override
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        if (!schemaName.equals(schemaMetadata.getName())) {
            throw new IllegalArgumentException("schemaName must match the name in schemaMetadata");
        }
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setName(schemaName);

        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
        if (schemaMetadataStorable != null) {
            schemaMetadataStorable = schemaMetadataStorable.updateSchemaMetadata(schemaMetadata);
            schemaMetadataStorable = ensureJsonCompatibility(schemaMetadataStorable);
            storageManager.update(schemaMetadataStorable);
            return schemaMetadataStorable.toSchemaMetadataInfo();
        } else {
            return null;
        }
    }

    /** JSON schema compatibility is always NONE. */
    private SchemaMetadataStorable ensureJsonCompatibility(@Nonnull SchemaMetadataStorable schemaMetadata) {
        if (StringUtils.equalsIgnoreCase("json", schemaMetadata.getType())) {
            return schemaMetadata.copy(
                SchemaCompatibility.valueOf(compatibilityConfig.getJsonCompatibility()));
        }
        return schemaMetadata;
    }

    @Override
    public Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> props) {
        // todo get only few selected columns instead of getting the whole row.
        Collection<SchemaMetadataStorable> storables;

        if (props == null || props.isEmpty()) {
            storables = storageManager.list(SchemaMetadataStorable.NAME_SPACE);
        } else {
            List<QueryParam> orderByFieldQueryParams = new ArrayList<>();
            List<QueryParam> queryParams = new ArrayList<>(props.size());
            for (Map.Entry<String, String> entry : props.entrySet()) {
                QueryParam queryParam = new QueryParam(entry.getKey(), entry.getValue());
                if (ORDER_BY_FIELDS_PARAM_NAME.equals(entry.getKey())) {
                    orderByFieldQueryParams.add(queryParam);
                } else {
                    queryParams.add(queryParam);
                }
            }
            storables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, queryParams, getOrderByFields(orderByFieldQueryParams));
        }

        List<SchemaMetadataInfo> result;
        if (storables != null && !storables.isEmpty()) {
            result = storables.stream().map(SchemaMetadataStorable::toSchemaMetadataInfo).collect(Collectors.toList());
        } else {
            result = Collections.emptyList();
        }

        return result;
    }

    private List<OrderByField> getOrderByFields(List<QueryParam> queryParams) {
        if (queryParams == null || queryParams.isEmpty()) {
            return Collections.emptyList();
        }

        List<OrderByField> orderByFields = new ArrayList<>();
        for (QueryParam queryParam : queryParams) {
            if (ORDER_BY_FIELDS_PARAM_NAME.equals(queryParam.getName())) {
                // _orderByFields=[<field-name>,<a/d>,]*
                // example can be : _orderByFields=foo,a,bar,d
                // order by foo with ascending then bar with descending
                String value = queryParam.getValue();
                String[] splitStrings = value.split(",");
                for (int i = 0; i < splitStrings.length; i += 2) {
                    String ascStr = splitStrings[i + 1];
                    boolean descending;
                    if ("a".equals(ascStr)) {
                        descending = false;
                    } else if ("d".equals(ascStr)) {
                        descending = true;
                    } else {
                        throw new IllegalArgumentException("Ascending or Descending identifier can only be 'a' or 'd' respectively.");
                    }

                    orderByFields.add(OrderByField.of(splitStrings[i], descending));
                }
            }
        }

        return orderByFields;
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
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
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        lockSchemaMetadata(schemaMetadata.getName());
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, schemaVersion,
                x -> registerSchemaMetadata(x), disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        lockSchemaMetadata(schemaMetadata.getName());
        return schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaMetadata, schemaVersion,
                x -> registerSchemaMetadata(x), disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaName,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {
        lockSchemaMetadata(schemaName);
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion, disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            String schemaName,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, SchemaBranchNotFoundException {
        lockSchemaMetadata(schemaName);
        return schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, Long versionId, SchemaVersion schemaVersion) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        lockSchemaMetadata(schemaMetadata.getName());
        return schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata,
            schemaVersion, this::registerSchemaMetadata, false);
    }

    @Override
    public SchemaIdVersion addSchemaVersionWithBranchName(String branchName, SchemaMetadata schemaMetadata, Long versionId, SchemaVersionInfo schemaVersionInfo) 
        throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        lockSchemaMetadata(schemaMetadata.getName());
        return schemaVersionLifecycleManager.addSchemaVersion(branchName, schemaMetadata,
            versionId, new SchemaVersion(schemaVersionInfo), schemaVersionInfo.getVersion(),
            this::registerSchemaMetadata, SchemaDeduplicationMode.NO_CHECK);
    }

    @Override
    public SchemaBranch createMasterBranch(Long branchId, Long metadataId) throws SchemaNotFoundException {
        Long timeMillis = System.currentTimeMillis();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(metadataId);
        if (schemaMetadataInfo != null) {
            SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
            SchemaBranch branch = new SchemaBranch(SchemaBranch.MASTER_BRANCH, schemaMetadata.getName(), String.format(SchemaBranch.MASTER_BRANCH_DESC, schemaMetadata.getName()), timeMillis);
            SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
            givenSchemaMetadataStorable.setName(schemaMetadata.getName());
            SchemaBranchStorable schemaBranchStorable = new SchemaBranchStorable(SchemaBranch.MASTER_BRANCH,
                schemaMetadata.getName(), String.format(SchemaBranch.MASTER_BRANCH_DESC, schemaMetadata.getName()), timeMillis);
            Long id = checkIfIdIsTaken(SchemaBranchStorable.class, branchId);
            schemaBranchStorable.setId(id);
            storageManager.add(schemaBranchStorable);

            storageManager.add(new SchemaLockStorable(givenSchemaMetadataStorable.getNameSpace(),
                givenSchemaMetadataStorable.getName(), timeMillis));
            return branch;
        } else {
            throw new SchemaNotFoundException(String.format("SchemaMetadata with id {} is not found", metadataId));
        }
        
    }

    private void lockSchemaMetadata(String schemaName) {
        String lockName = new SchemaLockStorable(SchemaMetadataStorable.NAME_SPACE, schemaName).getName();
        Lock writeLock = schemaLockManager.getWriteLock(lockName);
        if (!writeLock.lock(DEFAULT_SCHEMA_LOCK_TIMEOUT_IN_SECS, TimeUnit.SECONDS)) {
            throw new RuntimeException("Failed to obtain write lock : " + lockName + " in " + DEFAULT_SCHEMA_LOCK_TIMEOUT_IN_SECS + " sec");
        }
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(final String schemaName) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getAllVersions(schemaName);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(final String schemaBranchName, final String schemaName)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(String schemaName,
                                                  String schemaText,
                                                  boolean disableCanonicalCheck)
            throws SchemaNotFoundException, InvalidSchemaException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaName, schemaText, disableCanonicalCheck);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaVersionKey);
    }

    @Override
    public SchemaVersionInfo findSchemaVersionByFingerprint(String fingerprint) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.findSchemaVersionInfoByFingerprint(fingerprint);
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionKey);
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId)
            throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException, SchemaBranchNotFoundException {
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
    public void transitionState(Long schemaVersionId, Byte targetStateId, byte[] transitionDetails)
            throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.executeState(schemaVersionId, targetStateId, transitionDetails);
    }

    @Override
    public Collection<AggregatedSchemaBranch> getAggregatedSchemaBranch(String schemaName)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = new ArrayList<>();
        for (SchemaBranch schemaBranch : getSchemaBranches(schemaName)) {
            Long rootVersion = schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH) ? null :
                    schemaVersionLifecycleManager.getRootVersion(schemaBranch).getId();
            Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaBranch.getName(), schemaName);
            schemaVersionInfos.stream().forEach(schemaVersionInfo -> {
                SchemaVersionLifecycleContext context = null;
                try {
                    context = schemaVersionLifecycleManager
                            .createSchemaVersionLifeCycleContext(schemaVersionInfo.getId(), SchemaVersionLifecycleStates.INITIATED);
                    MergeInfo mergeInfo = null;
                    if (context.getDetails() == null) {
                        mergeInfo = null;
                    } else {
                        try {
                            InitializedStateDetails details = ObjectMapperUtils.deserialize(context.getDetails(), InitializedStateDetails.class);
                            mergeInfo = details.getMergeInfo();
                        } catch (IOException e) {
                            throw new RuntimeException(String.format("Failed to serialize state details of schema version : '%s'",
                                    context.getSchemaVersionId()), e);
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
    public UploadResult bulkUploadSchemas(InputStream file, boolean failOnError, BulkUploadInputFormat format) throws IOException {
        return bulkUploadService.bulkUploadSchemas(file, failOnError, format);
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId,
                                                       boolean disableCanonicalCheck) throws SchemaNotFoundException, IncompatibleSchemaException {
        return mergeSchemaVersion(schemaVersionId, SchemaVersionMergeStrategy.valueOf(DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY), disableCanonicalCheck);
    }

    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return schemaVersionLifecycleManager.getSchemaVersionLifecycleStateMachine().toConfig();
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaName);
    }

    public CompatibilityResult checkCompatibility(String schemaName, String toSchema) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.checkCompatibility(SchemaBranch.MASTER_BRANCH, schemaName, toSchema);
    }

    @Override
    public SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch)
            throws SchemaBranchAlreadyExistsException, SchemaNotFoundException {

        checkNotNull(schemaBranch.getName(), "Schema branch name can't be null");

        SchemaVersionInfo schemaVersionInfo = schemaVersionLifecycleManager.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));

        SchemaBranchKey schemaBranchKey = new SchemaBranchKey(schemaBranch.getName(), schemaVersionInfo.getName());
        SchemaBranch existingSchemaBranch = null;
        try {
            existingSchemaBranch = getSchemaBranch(schemaBranchKey);
        } catch (SchemaBranchNotFoundException e) {
            // Ignore this error
        }

        if (existingSchemaBranch != null) {
            throw new SchemaBranchAlreadyExistsException(String.format("A schema branch with name : '%s' already exists", schemaBranch.getName()), schemaBranch.getName());
        }
        SchemaBranchStorable schemaBranchStorable = SchemaBranchStorable.from(schemaBranch);
        schemaBranchStorable.setSchemaMetadataName(schemaVersionInfo.getName());
        Long id = schemaBranch.getId() != null ? schemaBranch.getId() :
            storageManager.nextId(SchemaBranchStorable.NAME_SPACE);
        if (schemaBranch.getId() == null) {
            LOG.debug("Given id is null, id is generated");
            id = checkIfIdIsTaken(SchemaBranchStorable.class, id);
        }
        schemaBranchStorable.setId(id);
        storageManager.add(schemaBranchStorable);

        SchemaBranch persistedSchemaBranch;
        try {
           persistedSchemaBranch = getSchemaBranch(schemaBranchKey);
        } catch (SchemaBranchNotFoundException e) {
            throw new RuntimeException(String.format("Failed to fetch persisted schema branch : '%s' from the database", schemaBranch.getName()));
        }

        SchemaBranchVersionMapping schemaBranchVersionMapping =
                new SchemaBranchVersionMapping(persistedSchemaBranch.getId(), schemaVersionInfo.getId());
        storageManager.add(schemaBranchVersionMapping);

        return persistedSchemaBranch;
    }

    @VisibleForTesting
    Long checkIfIdIsTaken(Class<? extends AbstractStorable> storable, Long id) {
        String namespace = "";
        String idName = "";
        if (storable.equals(SchemaBranchStorable.class)) {
            namespace = SchemaBranchStorable.NAME_SPACE;
            idName = SchemaBranchStorable.ID;
        } else if (storable.equals(SchemaMetadataStorable.class)) {
            namespace = SchemaMetadataStorable.NAME_SPACE;
            idName = SchemaMetadataStorable.ID;
        } else {
            LOG.debug("Given storable is not branch or metadata.");
        }
        if (!(namespace.isEmpty() || idName.isEmpty())) {
            while (!storageManager.find(namespace, Collections.singletonList(new QueryParam(idName, String.valueOf(id)))).isEmpty()) {
                LOG.info("Next ID {} for Schema Branch already exists. Generating next ID.", id);
                id = storageManager.nextId(namespace);
            }
            return id;
        } else {
            return null;
        }
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException {
        if (getSchemaMetadataInfo(schemaName) == null) {
            throw new SchemaNotFoundException(String.format("No schema metadata with name {}", schemaName));
        }
        Collection<SchemaBranchStorable> branchStorables = storageManager.find(SchemaBranchStorable.NAME_SPACE, Collections.singletonList(new QueryParam(SchemaBranchStorable.SCHEMA_METADATA_NAME, schemaName)));
        return branchStorables.stream()
            .map(SchemaBranchStorable::toSchemaBranch)
            .collect(Collectors.toList());
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {

        checkNotNull(schemaBranchId, "Schema branch name can't be null");

        SchemaBranch schemaBranch = getSchemaBranch(schemaBranchId);

        if (schemaBranch.getName().equals(SchemaBranch.MASTER_BRANCH)) {
            throw new InvalidSchemaBranchDeletionException(String.format("Can't delete '%s' branch", SchemaBranch.MASTER_BRANCH));
        }

        List<QueryParam> schemaVersionMappingStorableQueryParams = new ArrayList<>();
        schemaVersionMappingStorableQueryParams.add(new QueryParam(SchemaBranchVersionMapping.SCHEMA_BRANCH_ID, schemaBranch.getId().toString()));
        List<OrderByField> schemaVersionMappingOrderbyFields = new ArrayList<>();
        schemaVersionMappingOrderbyFields.add(OrderByField.of(SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, false));
        Collection<SchemaBranchVersionMapping> schemaBranchVersionMappings = storageManager.find(SchemaBranchVersionMapping.NAMESPACE,
                schemaVersionMappingStorableQueryParams,
                schemaVersionMappingOrderbyFields);

        if (schemaBranchVersionMappings == null) {
            throw new RuntimeException("Schema branch is invalid state, its not associated with any schema versions");
        }

        // Ignore the first version as it used in the 'MASTER' branch
        Iterator<SchemaBranchVersionMapping> schemaBranchVersionMappingIterator = schemaBranchVersionMappings.iterator();
        SchemaBranchVersionMapping rootVersionMapping = schemaBranchVersionMappingIterator.next();
        storageManager.remove(rootVersionMapping.getStorableKey());

        // Validate if the schema versions in the branch to be deleted are the root versions for other branches
        Map<Integer, List<String>> schemaVersionTiedToOtherBranch = new HashMap<>();
        List<Long> schemaVersionsToBeDeleted = new ArrayList<>();

        while (schemaBranchVersionMappingIterator.hasNext()) {
            SchemaBranchVersionMapping schemaBranchVersionMapping = schemaBranchVersionMappingIterator.next();
            Long schemaVersionId = schemaBranchVersionMapping.getSchemaVersionInfoId();
            try {
                List<QueryParam> schemaVersionCountParam = new ArrayList<>();
                schemaVersionCountParam.add(new QueryParam(
                        SchemaBranchVersionMapping.SCHEMA_VERSION_INFO_ID, schemaBranchVersionMapping.getSchemaVersionInfoId().toString())
                );
                Collection<SchemaBranchVersionMapping> mappingsForSchemaTiedToMutlipleBranch = storageManager
                        .find(SchemaBranchVersionMapping.NAMESPACE, schemaVersionCountParam);
                if (mappingsForSchemaTiedToMutlipleBranch.size() > 1) {
                    SchemaVersionInfo schemaVersionInfo = schemaVersionLifecycleManager.getSchemaVersionInfo(new SchemaIdVersion(schemaVersionId));
                    List<String> forkedBranchName = mappingsForSchemaTiedToMutlipleBranch.stream().
                            filter(mapping -> !mapping.getSchemaBranchId().equals(schemaBranchId)).
                            map(mappping -> getSchemaBranch(mappping.getSchemaBranchId()).getName()).
                            collect(Collectors.toList());
                    schemaVersionTiedToOtherBranch.put(schemaVersionInfo.getVersion(), forkedBranchName);
                } else {
                    schemaVersionsToBeDeleted.add(schemaVersionId);
                }
            } catch (SchemaNotFoundException e) {
                throw new RuntimeException(String.format("Failed to delete schema version : '%s' of schema branch : '%s'",
                        schemaVersionId.toString(), schemaBranchId), e);
            }
        }

        if (!schemaVersionTiedToOtherBranch.isEmpty()) {
            StringBuilder message = new StringBuilder();
            message.append("Failed to delete branch");
            schemaVersionTiedToOtherBranch.entrySet().stream().forEach(versionWithBranch -> {
                message.append(", schema version : '").append(versionWithBranch.getKey()).append("'");
                message.append(" is tied to branch : '").append(Arrays.toString(versionWithBranch.getValue().toArray())).append("'");
            });
            throw new InvalidSchemaBranchDeletionException(message.toString());
        } else {

            // Delete schema versions after validation

            for (Long schemaVersionId : schemaVersionsToBeDeleted) {
                try {
                    schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionId);
                } catch (SchemaLifecycleException e) {
                    throw new InvalidSchemaBranchDeletionException("Failed to delete schema branch, all schema versions in the branch should be in one " +
                            "of 'INITIATED', 'ChangesRequired' or 'Archived' state ", e);
                } catch (SchemaNotFoundException e) {
                    throw new RuntimeException(String.format("Failed to delete schema version : '%s' of schema branch : '%s'",
                            schemaVersionId.toString(), schemaBranchId), e);
                }
            }
        }

        storageManager.remove(new SchemaBranchStorable(schemaBranchId).getStorableKey());
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        if (stateIds == null || stateIds.isEmpty()) {
            return getAllVersions(schemaBranchName, schemaName);
        } else {
            return schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName, stateIds);
        }
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaBranchName, schemaName);
    }

    @Override
    public SchemaVersionInfo getLatestEnabledSchemaVersionInfo(String schemaBranchName, String schemaName)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getLatestEnabledSchemaVersionInfo(schemaBranchName, schemaName);
    }


    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName, String toSchema)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.checkCompatibility(schemaBranchName, schemaName, toSchema);
    }

    @Override
    public String uploadFile(InputStream inputStream) {
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
    public Long addSerDes(SerDesPair serDesInfo) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable(serDesInfo);
        Long nextId = storageManager.nextId(serDesInfoStorable.getNameSpace());
        serDesInfoStorable.setId(nextId);
        serDesInfoStorable.setTimestamp(System.currentTimeMillis());
        storageManager.add(serDesInfoStorable);

        return serDesInfoStorable.getId();
    }

    @Override
    public SerDesInfo getSerDes(Long serDesId) {
        SerDesInfoStorable serDesInfoStorable = storageManager.get(createSerDesStorableKey(serDesId));
        return serDesInfoStorable != null ? serDesInfoStorable.toSerDesInfo() : null;
    }

    private StorableKey createSerDesStorableKey(Long serDesId) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable();
        serDesInfoStorable.setId(serDesId);
        return serDesInfoStorable.getStorableKey();
    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        return getSerDesInfos(schemaName);
    }

    private Collection<SchemaSerDesMapping> getSchemaSerDesMappings(Long schemaMetadataId) {
        List<QueryParam> queryParams =
                Collections.singletonList(new QueryParam(SchemaSerDesMapping.SCHEMA_METADATA_ID, schemaMetadataId.toString()));

        return storageManager.find(SchemaSerDesMapping.NAMESPACE, queryParams);
    }

    private List<SerDesInfo> getSerDesInfos(String schemaName) {
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(
                checkNotNull(getSchemaMetadataInfo(schemaName), "Did not find schema meta for \"%s\"", schemaName).getId());
        List<SerDesInfo> serDesInfos;
        if (schemaSerDesMappings == null || schemaSerDesMappings.isEmpty()) {
            serDesInfos = Collections.emptyList();
        } else {
            serDesInfos = new ArrayList<>();
            for (SchemaSerDesMapping schemaSerDesMapping : schemaSerDesMappings) {
                SerDesInfo serDesInfo = getSerDes(schemaSerDesMapping.getSerDesId());
                serDesInfos.add(serDesInfo);
            }
        }
        return serDesInfos;
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        SerDesInfo serDesInfo = getSerDes(serDesId);
        if (serDesInfo == null) {
            throw new SerDesException("Serializer with given ID " + serDesId + " does not exist");
        }

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        SchemaSerDesMapping schemaSerDesMapping = new SchemaSerDesMapping(schemaMetadataInfo.getId(), serDesId);
        storageManager.add(schemaSerDesMapping);
    }

    /**
     * Searches the registry to find schemas according to the given {@code whereClause} and orders the results by given {@code orderByFields}
     *
     * @param whereClause
     * @param orderByFields
     *
     * @return Collection of schemas from the results of given where clause.
     */
    private Collection<SchemaMetadataInfo> searchSchemas(WhereClause whereClause, List<OrderBy> orderByFields) {
        SearchQuery searchQuery = SearchQuery.searchFrom(SchemaMetadataStorable.NAME_SPACE)
                                             .orderBy(orderByFields.toArray(new OrderBy[orderByFields.size()]));
        if (whereClause != null) {
            searchQuery = searchQuery.where(whereClause);
        }
        return storageManager.search(searchQuery)
                             .stream()
                             .map(y -> ((SchemaMetadataStorable) y).toSchemaMetadataInfo())
                             .collect(Collectors.toList());

    }

    @Override
    public Collection<SchemaMetadataInfo> searchSchemas(MultivaluedMap<String, String> queryParameters, Optional<String> orderBy) {
        WhereClause whereClause = getWhereClause(queryParameters);
        List<OrderBy> orderByFields = getOrderByFields(orderBy.orElse(""));
        return searchSchemas(whereClause, orderByFields);
    }

    @VisibleForTesting
    WhereClause getWhereClause(MultivaluedMap<String, String> queryParameters) {
        String name = queryParameters.getFirst(SchemaMetadataStorable.NAME);
        String description = queryParameters.getFirst(SchemaMetadataStorable.DESCRIPTION);

        WhereClause.Builder builder = WhereClause.begin();
        WhereClauseCombiner whereClauseCombiner = null;
        if (isNotBlank(name)) {
            whereClauseCombiner = builder.contains(SchemaMetadataStorable.NAME, name);
        }
        if (isNotBlank(description)) {
            if (whereClauseCombiner != null) {
                whereClauseCombiner = whereClauseCombiner.and().contains(SchemaMetadataStorable.DESCRIPTION, description);
            } else {
                whereClauseCombiner = builder.contains(SchemaMetadataStorable.DESCRIPTION, description);
            }
        }
        if (whereClauseCombiner == null) {
            return null;
        }
        return whereClauseCombiner.combine();
    }

    private List<OrderBy> getOrderByFields(String value) {
        List<OrderBy> orderByList = new ArrayList<>();
        // _orderByFields=[<field-name>,<a/d>,]*
        // example can be : _orderByFields=foo,a,bar,d
        // order by foo with ascending then bar with descending
        String[] splitStrings = value.split(",");
        for (int i = 0; i < splitStrings.length; i += 2) {
            String ascStr = splitStrings[i + 1];
            boolean descending;
            if ("a".equals(ascStr)) {
                descending = false;
            } else if ("d".equals(ascStr)) {
                descending = true;
            } else {
                throw new IllegalArgumentException("Ascending or Descending identifier can only be 'a' or 'd' respectively.");
            }

            String fieldName = splitStrings[i];
            orderByList.add(descending ? OrderBy.desc(fieldName) : OrderBy.asc(fieldName));
        }

        return orderByList;
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId,
                                                       SchemaVersionMergeStrategy schemaVersionMergeStrategy,
                                                       boolean disableCanonicalCheck) throws IncompatibleSchemaException, SchemaNotFoundException {
        return schemaVersionLifecycleManager.mergeSchemaVersion(schemaVersionId, schemaVersionMergeStrategy, disableCanonicalCheck);
    }

    public AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
    }

    private AggregatedSchemaMetadataInfo buildAggregatedSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo)
            throws SchemaNotFoundException, SchemaBranchNotFoundException {

        if (schemaMetadataInfo == null) {
            return null;
        }

        List<SerDesInfo> serDesInfos = getSerDesInfos(schemaMetadataInfo.getSchemaMetadata().getName());

        return new AggregatedSchemaMetadataInfo(schemaMetadataInfo.getSchemaMetadata(),
                                                schemaMetadataInfo.getId(),
                                                schemaMetadataInfo.getTimestamp(),
                                                getAggregatedSchemaBranch(schemaMetadataInfo.getSchemaMetadata().getName()),
                                                serDesInfos);
    }

    private SchemaBranch getSchemaBranch(SchemaBranchKey schemaBranchKey) throws SchemaBranchNotFoundException {
        List<QueryParam> queryParams = new ArrayList<>();
        queryParams.add(new QueryParam(SchemaBranchStorable.NAME, schemaBranchKey.getSchemaBranchName()));
        queryParams.add(new QueryParam(SchemaBranchStorable.SCHEMA_METADATA_NAME, schemaBranchKey.getSchemaMetadataName()));
        Collection<SchemaBranchStorable> schemaBranchStorables = storageManager.find(SchemaBranchStorable.NAME_SPACE, queryParams);
        if (schemaBranchStorables == null || schemaBranchStorables.isEmpty()) {
            throw new SchemaBranchNotFoundException(String.format("Schema branch with key : %s not found", schemaBranchKey));
        } else if (schemaBranchStorables.size() > 1) {
            throw new SchemaBranchNotFoundException(String.format("Failed to unique determine a schema branch with key : %s", schemaBranchKey));
        }
        return schemaBranchStorables.iterator().next().toSchemaBranch();
    }

    @Override
    public SchemaBranch getSchemaBranch(Long id) throws SchemaBranchNotFoundException {
        List<QueryParam> schemaBranchQueryParam = new ArrayList<>();
        schemaBranchQueryParam.add(new QueryParam(SchemaBranchStorable.ID, id.toString()));
        Collection<SchemaBranchStorable> schemaBranchStorables = storageManager.find(SchemaBranchStorable.NAME_SPACE, schemaBranchQueryParam);
        if (schemaBranchStorables == null || schemaBranchStorables.isEmpty()) {
            throw new SchemaBranchNotFoundException(String.format("Schema branch with id : '%s' not found", id.toString()));
        }
        // size of the collection will always be less than 2, as ID is a primary key, so no need handle the case where size > 1
        return schemaBranchStorables.iterator().next().toSchemaBranch();
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranchesForVersion(Long vertionId) throws SchemaBranchNotFoundException {
        return schemaVersionLifecycleManager.getSchemaBranches(vertionId);
    }

    @Override
    public SchemaVersionInfo fetchSchemaVersionInfo(Long id) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.fetchSchemaVersionInfo(id);
    }
}
