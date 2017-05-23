/**
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
 **/
package com.hortonworks.registries.schemaregistry;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.SlotSynchronizer;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    public static final String ORDER_BY_FIELDS_PARAM_NAME = "_orderByFields";

    private static final int DEFAULT_RETRY_CT = 5;

    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final Collection<Map<String, Object>> schemaProvidersConfig;
    private final Map<String, SchemaProvider> schemaTypeWithProviders = new HashMap<>();
    private final Object addOrUpdateLock = new Object();

    private Options options;
    private SchemaVersionInfoCache schemaVersionInfoCache;
    private List<SchemaProviderInfo> schemaProviderInfos;
    private SlotSynchronizer<String> slotSynchronizer = new SlotSynchronizer<>();

    public DefaultSchemaRegistry(StorageManager storageManager,
                                 FileStorage fileStorage,
                                 Collection<Map<String, Object>> schemaProvidersConfig) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaProvidersConfig = schemaProvidersConfig;
    }

    private Collection<? extends SchemaProvider> initSchemaProviders(Collection<Map<String, Object>> schemaProvidersConfig) {
        if (schemaProvidersConfig == null || schemaProvidersConfig.isEmpty()) {
            throw new IllegalArgumentException("No [" + SCHEMA_PROVIDERS + "] property is configured in schema registry configuration file.");
        }

        SchemaVersionRetriever schemaVersionRetriever = key -> getSchemaVersionInfo(key);

        return Collections2.transform(schemaProvidersConfig, new Function<Map<String, Object>, SchemaProvider>() {
            @Nullable
            @Override
            public SchemaProvider apply(@Nullable Map<String, Object> schemaProviderConfig) {
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
            }
        });
    }

    @Override
    public void init(Map<String, Object> props) {
        Collection<? extends SchemaProvider> schemaProviders = initSchemaProviders(schemaProvidersConfig);

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

        storageManager.registerStorables(Lists.newArrayList(SchemaMetadataStorable.class, SchemaVersionStorable.class,
                                                            SchemaFieldInfoStorable.class, SerDesInfoStorable.class,
                                                            SchemaSerDesMapping.class));
    }

    @Override
    public Collection<SchemaProviderInfo> getRegisteredSchemaProviderInfos() {
        return schemaProviderInfos;
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) throws UnsupportedSchemaTypeException {
        return addSchemaMetadata(schemaMetadata, false);
    }

    public Long addSchemaMetadata(SchemaMetadata schemaMetadata, boolean throwErrorIfExists) throws UnsupportedSchemaTypeException {
        SchemaMetadataStorable givenSchemaMetadataStorable = SchemaMetadataStorable.fromSchemaMetadataInfo(new SchemaMetadataInfo(schemaMetadata));
        String type = schemaMetadata.getType();
        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        synchronized (addOrUpdateLock) {
            if(!throwErrorIfExists) {
                Storable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
                if (schemaMetadataStorable != null) {
                    return schemaMetadataStorable.getId();
                }
            }
            final Long nextId = storageManager.nextId(givenSchemaMetadataStorable.getNameSpace());
            givenSchemaMetadataStorable.setId(nextId);
            givenSchemaMetadataStorable.setTimestamp(System.currentTimeMillis());
            storageManager.add(givenSchemaMetadataStorable);
            return givenSchemaMetadataStorable.getId();
        }
    }

    public Integer addSchemaVersion(SchemaMetadata schemaMetadata, String schemaText, String description)
            throws IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException, SchemaNotFoundException {
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


    private Integer createSchemaVersion(SchemaMetadata schemaMetadata,
                                        Long schemaMetadataId,
                                        String schemaText,
                                        String description)
            throws IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException, SchemaNotFoundException {

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
        final Long schemaVersionStorableId = storageManager.nextId(schemaVersionStorable.getNameSpace());
        schemaVersionStorable.setId(schemaVersionStorableId);
        schemaVersionStorable.setSchemaMetadataId(schemaMetadataId);

        schemaVersionStorable.setFingerprint(fingerprint);

        schemaVersionStorable.setName(schemaName);

        schemaVersionStorable.setSchemaText(schemaText);
        schemaVersionStorable.setDescription(description);
        schemaVersionStorable.setTimestamp(System.currentTimeMillis());

        // take a lock for a schema with same name.
        SlotSynchronizer.Lock slotLock = slotSynchronizer.lockSlot(schemaName);
        try {
            int retryCt = 0;
            while (true) {
                try {
                    Integer version = 0;
                    if (schemaMetadata.isEvolve()) {
                        Collection<SchemaVersionInfo> schemaVersionInfos = findAllVersions(schemaName);
                        if (schemaVersionInfos != null && !schemaVersionInfos.isEmpty()) {
                            SchemaVersionInfo latestSchemaVersionInfo = null;
                            SchemaCompatibility compatibility = schemaMetadata.getCompatibility();
                            for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
                                // check for compatibility
                                CompatibilityResult compatibilityResult =
                                        schemaTypeWithProviders
                                                .get(type)
                                                .checkCompatibility(schemaText,
                                                                    schemaVersionInfo.getSchemaText(),
                                                                    compatibility);
                                if (!compatibilityResult.isCompatible()) {
                                    String errMsg = String.format("Given schema is not compatible with earlier schema versions. \n" +
                                                                  "Error location: [%s] \n" +
                                                                  "Error encountered is: [%s]",
                                                                  compatibilityResult.getErrorLocation(),
                                                                  compatibilityResult.getErrorMessage());
                                    LOG.error(errMsg);
                                    throw new IncompatibleSchemaException(errMsg);
                                }

                                Integer curVersion = schemaVersionInfo.getVersion();
                                if (curVersion >= version) {
                                    latestSchemaVersionInfo = schemaVersionInfo;
                                    version = curVersion;
                                }
                            }

                            version = latestSchemaVersionInfo.getVersion();
                        }
                    }
                    schemaVersionStorable.setVersion(version + 1);

                    storageManager.add(schemaVersionStorable);

                    break;
                } catch (StorageException e) {
                    // optimistic to try the next try would be successful. When retr attemps are exhausted, throw error back to invoker.
                    if(++retryCt == DEFAULT_RETRY_CT) {
                        LOG.error("Giving up after retry attempts [{}] while trying to add new version of schema with metadata [{}]", retryCt, schemaMetadata, e);
                        throw e;
                    }
                    LOG.debug("Encountered storage exception while trying to add a new version, attempting again : [{}] with error: [{}]", retryCt, e);
                }
            }

            // fetching this as the ID may have been set by storage manager.
            Long schemaInstanceId = schemaVersionStorable.getId();
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
            schemaMetadataInfo = schemaMetadataStorables.iterator().next().toSchemaMetadataInfo();
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

        return schemaMetadataStorable != null ? schemaMetadataStorable.toSchemaMetadataInfo() : null;
    }

    public Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> filters) {
        return findSchemaMetadata(filters)
                .stream()
                .map(this::buildAggregatedSchemaMetadataInfo)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<SchemaMetadataInfo> findSchemaMetadata(Map<String, String> filters) {
        // todo get only few selected columns instead of getting the whole row.
        Collection<SchemaMetadataStorable> storables;

        if (filters == null || filters.isEmpty()) {
            storables = storageManager.list(SchemaMetadataStorable.NAME_SPACE);
        } else {
            List<QueryParam> orderByFieldQueryParams = new ArrayList<>();
            List<QueryParam> queryParams = new ArrayList<>(filters.size());
            for (Map.Entry<String, String> entry : filters.entrySet()) {
                QueryParam queryParam = new QueryParam(entry.getKey(), entry.getValue());
                if(ORDER_BY_FIELDS_PARAM_NAME.equals(entry.getKey())) {
                    orderByFieldQueryParams.add(queryParam);
                } else{
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
        if(queryParams == null || queryParams.isEmpty()) {
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
                    String ascStr = splitStrings[i+1];
                    boolean descending;
                    if("a".equals(ascStr)) {
                        descending = false;
                    } else if("d".equals(ascStr)) {
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
    public List<SchemaVersionInfo> findAllVersions(final String schemaName) {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaVersionStorable.NAME, schemaName));

        Collection<SchemaVersionStorable> storables = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
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

    private Integer findSchemaVersion(String type,
                                      String schemaText,
                                      Long schemaMetadataId) throws InvalidSchemaException, SchemaNotFoundException {
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

    private String getFingerprint(String type, String schemaText) throws InvalidSchemaException, SchemaNotFoundException {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        return Hex.encodeHexString(schemaProvider.getFingerprint(schemaText));
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        return schemaVersionInfoCache.getSchema(schemaVersionKey);
    }

    SchemaVersionInfo retrieveSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        String schemaName = schemaVersionKey.getSchemaName();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);

        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("No SchemaMetadata exists with key: " + schemaName);
        }

        SchemaVersionInfo schemaVersionInfo = null;
        Integer version = schemaVersionKey.getVersion();
        if (SchemaVersionKey.LATEST_VERSION.equals(version)) {
            schemaVersionInfo = getLatestSchemaVersionInfo(schemaVersionKey.getSchemaName());
        } else {
            Long schemaMetadataId = schemaMetadataInfo.getId();
            List<QueryParam> queryParams = Lists.newArrayList(
                    new QueryParam(SchemaVersionStorable.SCHEMA_METADATA_ID, schemaMetadataId.toString()),
                    new QueryParam(SchemaVersionStorable.VERSION, version.toString()));

            Collection<SchemaVersionStorable> versionedSchemas = storageManager.find(SchemaVersionStorable.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                if (versionedSchemas.size() > 1) {
                    LOG.warn("More than one schema exists with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                }
                schemaVersionInfo = versionedSchemas.iterator().next().toSchemaVersionInfo();
            } else {
                throw new SchemaNotFoundException("No Schema version exists with schemaMetadataId " + schemaMetadataId + " and version " + version);
            }
        }

        return schemaVersionInfo;
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
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);

        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        for (SchemaVersionInfo schemaVersionInfo : schemaVersionInfos) {
            CompatibilityResult compatibilityResult = checkCompatibility(schemaMetadata.getType(),
                                                                         toSchema,
                                                                         schemaVersionInfo.getSchemaText(),
                                                                         schemaMetadata.getCompatibility());
            if(!compatibilityResult.isCompatible()) {
                LOG.info("Received schema is not compatible with one of the schema versions [{}] with schema name [{}]",
                         schemaVersionInfo.getVersion(), schemaName);
                return compatibilityResult;
            }
        }

        return CompatibilityResult.createCompatibleResult(toSchema);
    }

    public CompatibilityResult checkCompatibility(SchemaVersionKey schemaVersionKey,
                                                  String toSchema) throws SchemaNotFoundException {
        String schemaName = schemaVersionKey.getSchemaName();

        SchemaVersionInfo existingSchemaVersionInfo = getSchemaVersionInfo(schemaVersionKey);
        String schemaText = existingSchemaVersionInfo.getSchemaText();
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);
        SchemaMetadata schemaMetadata = schemaMetadataInfo.getSchemaMetadata();
        return checkCompatibility(schemaMetadata.getType(), toSchema, schemaText, schemaMetadata.getCompatibility());
    }

    private CompatibilityResult checkCompatibility(String type,
                                                   String toSchema,
                                                   String existingSchema,
                                                   SchemaCompatibility compatibility) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.checkCompatibility(toSchema, existingSchema, compatibility);
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
    public Long addSerDesInfo(SerDesPair serDesInfo) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable(serDesInfo);
        Long nextId = storageManager.nextId(serDesInfoStorable.getNameSpace());
        serDesInfoStorable.setId(nextId);
        serDesInfoStorable.setTimestamp(System.currentTimeMillis());
        storageManager.add(serDesInfoStorable);

        return serDesInfoStorable.getId();
    }

    @Override
    public SerDesInfo getSerDesInfo(Long serDesId) {
        SerDesInfoStorable serDesInfoStorable = new SerDesInfoStorable();
        serDesInfoStorable.setId(serDesId);
        return ((SerDesInfoStorable) storageManager.get(serDesInfoStorable.getStorableKey())).toSerDesInfo();
    }

    @Override
    public Collection<SerDesInfo> getSchemaSerializers(Long schemaMetadataId) {
        return getSerDesInfos(schemaMetadataId);
    }

    private Collection<SchemaSerDesMapping> getSchemaSerDesMappings(Long schemaMetadataId) {
        List<QueryParam> queryParams =
                Collections.singletonList(new QueryParam(SchemaSerDesMapping.SCHEMA_METADATA_ID, schemaMetadataId.toString()));

        return storageManager.find(SchemaSerDesMapping.NAMESPACE, queryParams);
    }

    private List<SerDesInfo> getSerDesInfos(Long schemaMetadataId) {
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(schemaMetadataId);
        List<SerDesInfo> serDesInfos;
        if (schemaSerDesMappings == null || schemaSerDesMappings.isEmpty()) {
            serDesInfos = Collections.emptyList();
        } else {
            serDesInfos = new ArrayList<>();
            for (SchemaSerDesMapping schemaSerDesMapping : schemaSerDesMappings) {
                SerDesInfo serDesInfo = getSerDesInfo(schemaSerDesMapping.getSerDesId());
                serDesInfos.add(serDesInfo);
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
                inputStream = fileStorage.downloadFile(serDesInfoStorable.getSerDesPair().getFileId());
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

    @Override
    public Collection<SchemaMetadataInfo> searchSchemas(WhereClause whereClause, List<OrderBy> orderByFields) {
        SearchQuery searchQuery = SearchQuery.searchFrom(SchemaMetadataStorable.NAME_SPACE)
                .where(whereClause)
                .orderBy(orderByFields.toArray(new OrderBy[orderByFields.size()]));

        return storageManager.search(searchQuery)
                .stream()
                .map(y -> ((SchemaMetadataStorable) y).toSchemaMetadataInfo())
                .collect(Collectors.toList());

    }

    public AggregatedSchemaMetadataInfo getAggregatedSchemaMetadata(String schemaName) {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadata(schemaName);
        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
    }

    private AggregatedSchemaMetadataInfo buildAggregatedSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo) {
        List<SchemaVersionInfo> allVersions = findAllVersions(schemaMetadataInfo.getSchemaMetadata().getName());
        List<SerDesInfo> serDesInfos = getSerDesInfos(schemaMetadataInfo.getId());

        return new AggregatedSchemaMetadataInfo(schemaMetadataInfo.getSchemaMetadata(),
                                                schemaMetadataInfo.getId(),
                                                schemaMetadataInfo.getTimestamp(),
                                                allVersions,
                                                serDesInfos);
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