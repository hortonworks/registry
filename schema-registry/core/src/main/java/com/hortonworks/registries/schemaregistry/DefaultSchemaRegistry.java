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

import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.search.WhereClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default implementation for schema registry.
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    public static final String ORDER_BY_FIELDS_PARAM_NAME = "_orderByFields";

    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final Collection<Map<String, Object>> schemaProvidersConfig;

    private Map<String, SchemaProvider> schemaTypeWithProviders;
    private List<SchemaProviderInfo> schemaProviderInfos;
    private SchemaVersionLifecycleManager schemaVersionLifecycleManager;

    public DefaultSchemaRegistry(StorageManager storageManager,
                                 FileStorage fileStorage,
                                 Collection<Map<String, Object>> schemaProvidersConfig) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaProvidersConfig = schemaProvidersConfig;
    }

    @Override
    public void init(Map<String, Object> props) {

        storageManager.registerStorables(
                Arrays.asList(
                        SchemaMetadataStorable.class,
                        SchemaVersionStorable.class,
                        SchemaVersionStateStorable.class,
                        SchemaFieldInfoStorable.class,
                        SerDesInfoStorable.class,
                        SchemaSerDesMapping.class));

        SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        schemaVersionLifecycleManager = new SchemaVersionLifecycleManager(storageManager,
                                                                          props,
                                                                          schemaMetadataFetcher);

        Collection<? extends SchemaProvider> schemaProviders = initSchemaProviders(schemaProvidersConfig,
                                                                                   schemaVersionLifecycleManager.getSchemaVersionRetriever());

        this.schemaTypeWithProviders = schemaProviders.stream()
                                                      .collect(Collectors.toMap(SchemaProvider::getType,
                                                                                Function.identity()));

        schemaProviderInfos = Collections.unmodifiableList(
                schemaProviders.stream()
                               .map(schemaProvider
                                            -> new SchemaProviderInfo(schemaProvider
                                                                              .getType(),
                                                                      schemaProvider
                                                                              .getName(),
                                                                      schemaProvider
                                                                              .getDescription(),
                                                                      schemaProvider
                                                                              .getDefaultSerializerClassName(),
                                                                      schemaProvider
                                                                              .getDefaultDeserializerClassName()))
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

    private Collection<? extends SchemaProvider> initSchemaProviders(final Collection<Map<String, Object>> schemaProvidersConfig,
                                                                     final SchemaVersionRetriever schemaVersionRetriever) {
        if (schemaProvidersConfig == null || schemaProvidersConfig.isEmpty()) {
            throw new IllegalArgumentException("No [" + SCHEMA_PROVIDERS + "] property is configured in schema registry configuration file.");
        }

        return schemaProvidersConfig.stream()
                                    .map(schemaProviderConfig -> {
                                        String className = (String) schemaProviderConfig.get("providerClass");
                                        if (className == null || className.isEmpty()) {
                                            throw new IllegalArgumentException("Schema provider class name must be non empty, Invalid provider class name [" + className + "]");
                                        }

                                        try {
                                            SchemaProvider schemaProvider =
                                                    (SchemaProvider) Class.forName(className,
                                                                                   true,
                                                                                   Thread.currentThread()
                                                                                         .getContextClassLoader())
                                                                          .newInstance();
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
        SchemaMetadataStorable givenSchemaMetadataStorable = SchemaMetadataStorable.fromSchemaMetadataInfo(new SchemaMetadataInfo(schemaMetadata));
        String type = schemaMetadata.getType();
        if (schemaTypeWithProviders.get(type) == null) {
            throw new UnsupportedSchemaTypeException("Given schema type " + type + " not supported");
        }

        if (!throwErrorIfExists) {
            Storable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());
            if (schemaMetadataStorable != null) {
                return schemaMetadataStorable.getId();
            }
        }
        final Long nextId = storageManager.nextId(givenSchemaMetadataStorable.getNameSpace());
        givenSchemaMetadataStorable.setId(nextId);
        givenSchemaMetadataStorable.setTimestamp(System.currentTimeMillis());
        storageManager.addOrUpdate(givenSchemaMetadataStorable);
        return givenSchemaMetadataStorable.getId();
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
            LOG.info("SchemaMetadata entries with id [{}] is [{}]", schemaMetadataStorables);
        }

        return schemaMetadataInfo;
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setName(schemaName);

        SchemaMetadataStorable schemaMetadataStorable = storageManager.get(givenSchemaMetadataStorable.getStorableKey());

        return schemaMetadataStorable != null ? schemaMetadataStorable.toSchemaMetadataInfo() : null;
    }

    public Collection<AggregatedSchemaMetadataInfo> findAggregatedSchemaMetadata(Map<String, String> props) {
        return findSchemaMetadata(props)
                .stream()
                .map(schemaMetadataInfo -> {
                    try {
                        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
                    } catch (SchemaNotFoundException e) {
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
            schemaMetadataStorable = SchemaMetadataStorable.updateSchemaMetadata(schemaMetadataStorable, schemaMetadata);
            storageManager.addOrUpdate(schemaMetadataStorable);
            return schemaMetadataStorable.toSchemaMetadataInfo();
        } else {
            return null;
        }
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

    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion)
            throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException {
        return schemaVersionLifecycleManager.addSchemaVersion(schemaMetadata, schemaVersion, x -> registerSchemaMetadata(x));
    }

    public SchemaIdVersion addSchemaVersion(String schemaName,
                                            SchemaVersion schemaVersion)
            throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException {
        return schemaVersionLifecycleManager.addSchemaVersion(schemaName, schemaVersion);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(final String schemaName) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getAllVersions(schemaName);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(String schemaName,
                                                  String schemaText) throws SchemaNotFoundException, InvalidSchemaException {
        return schemaVersionLifecycleManager.getSchemaVersionInfo(schemaName, schemaText);
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
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        schemaVersionLifecycleManager.deleteSchemaVersion(schemaVersionKey);
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException {
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
    public void transitionState(Long schemaVersionId, Byte targetStateId) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionLifecycleManager.executeState(schemaVersionId, targetStateId);
    }

    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return schemaVersionLifecycleManager.getSchemaVersionLifecycleStateMachine().toConfig();
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaName);
    }

    public CompatibilityResult checkCompatibility(String schemaName, String toSchema) throws SchemaNotFoundException {
        return schemaVersionLifecycleManager.checkCompatibility(schemaName, toSchema);
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
        Collection<SchemaSerDesMapping> schemaSerDesMappings = getSchemaSerDesMappings(getSchemaMetadataInfo(schemaName)
                                                                                               .getId());
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

    public AggregatedSchemaMetadataInfo getAggregatedSchemaMetadataInfo(String schemaName) throws SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        return buildAggregatedSchemaMetadataInfo(schemaMetadataInfo);
    }

    private AggregatedSchemaMetadataInfo buildAggregatedSchemaMetadataInfo(SchemaMetadataInfo schemaMetadataInfo) throws SchemaNotFoundException {
        if (schemaMetadataInfo == null) {
            return null;
        }

        Collection<SchemaVersionInfo> allVersions = getAllVersions(schemaMetadataInfo.getSchemaMetadata().getName());
        List<SerDesInfo> serDesInfos = getSerDesInfos(schemaMetadataInfo.getSchemaMetadata().getName());

        return new AggregatedSchemaMetadataInfo(schemaMetadataInfo.getSchemaMetadata(),
                                                schemaMetadataInfo.getId(),
                                                schemaMetadataInfo.getTimestamp(),
                                                allVersions,
                                                serDesInfos);
    }


}