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
package com.cloudera.dim.atlas.impl;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.cloudera.dim.atlas.AtlasUncheckedException;
import com.cloudera.dim.atlas.translate.SchemaMetadataTranslator;
import com.cloudera.dim.atlas.translate.SchemaVersionInfoTranslator;
import com.cloudera.dim.atlas.types.MetadataEntityDef;
import com.cloudera.dim.atlas.types.Model;
import com.cloudera.dim.atlas.types.kafka.KafkaExtendedModel;
import com.cloudera.dim.atlas.types.kafka.KafkaTopicEntityDef;
import com.cloudera.dim.atlas.types.kafka.KafkaTopicSchemaRelationshipDef;
import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

// created by the AtlasPluginFactory
public class AtlasPluginImpl implements AtlasPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasPluginImpl.class);

    private static final String ERR_NO_URLS_PROVIDED = "List of Atlas host URLs is empty.";

    private final SchemaMetadataTranslator schemaMetadataTranslator = new SchemaMetadataTranslator();
    private final SchemaVersionInfoTranslator schemaVersionTranslator = new SchemaVersionInfoTranslator();
    AtlasClientV2 atlasClient;

    public void initialize(Map<String, Object> config) {
        LOG.debug("Initializing the Atlas plugin");
        try {
            String[] hostUrls = getAtlasHosts(config);
            String[] unamePwd = getBasicAuth(config);

            try {
                atlasClient = new AtlasClientV2(hostUrls, unamePwd);
            } catch (Throwable ex) {
                LOG.error("Failed to initialize Atlas client.", ex);
            }
        } catch (Throwable ex) {
            LOG.error("Error while initializing the Atlas plugin.", ex);
            throw new AtlasUncheckedException("Could not initialize the Atlas plugin implementation", ex);
        }
        LOG.debug("Successfully initialized the Atlas plugin");
    }

    @Override
    public void setupAtlasModel() {
        try {
            LOG.info("Creating Atlas model for Schema Registry");
            Model model = new Model();
            AtlasTypesDef createdTypeDefs = atlasClient.createAtlasTypeDefs(model);

            checkNotNull(createdTypeDefs, "No type defs have been created or Atlas failed to respond.");

            // The response contains the entity and relationship defs which have been created. We need
            // to compare the expected with the actual to see if everything was created.
            checkState(model.equals(createdTypeDefs), "Not all type definitions were created successfully.");
        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Error while creating the SchemaRegistry model in Atlas.", asex);
        }
    }

    @Override
    public boolean setupKafkaSchemaModel() {
        try {
            boolean kafkaTypeExists = findTypeDefByName(KafkaTopicEntityDef.KAFKA_TOPIC).isPresent();
            if (!kafkaTypeExists) {
                LOG.warn("Atlas type {} does not exist so we can't connect the schema to it.", KafkaTopicEntityDef.KAFKA_TOPIC);
                return false;
            }

            LOG.info("Creating model for connecting Schema with Kafka topic");
            KafkaExtendedModel model = new KafkaExtendedModel();
            AtlasTypesDef createdTypeDefs = atlasClient.createAtlasTypeDefs(model);

            return createdTypeDefs != null;
        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Error while creating the SchemaRegistry/Kafka model in Atlas.", asex);
        }
    }

    @Override
    public boolean isKafkaSchemaModelInitialized() {
        try {
            return findTypeDefByName(KafkaTopicEntityDef.KAFKA_TOPIC).isPresent() &&
                    findRelationshipDefByName(KafkaTopicSchemaRelationshipDef.RELATIONSHIP_NAME).isPresent();
        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Error while querying Atlas about the type model.", asex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void connectSchemaWithTopic(String metaGuid, SchemaMetadataInfo schemaMetadataInfo) {
        try {
            checkNotNull(schemaMetadataInfo, "Schema metadata info was null.");
            checkNotNull(schemaMetadataInfo.getSchemaMetadata(), "Schema metadata was null.");

            AtlasEntity.AtlasEntityWithExtInfo metaEntityInfo = atlasClient.getEntityByGuid(metaGuid);
            checkNotNull(metaEntityInfo, "Did not find schema_metadata_info with GUID " + metaGuid);
            checkNotNull(metaEntityInfo.getEntity(), "Did not find schema_metadata_info with GUID " + metaGuid);

            AtlasEntity metaEntity = metaEntityInfo.getEntity();
            boolean relationshipAlreadyExists = false;

            if (metaEntity.getRelationshipAttributes() != null) {
                Object kafkaTopicColl = metaEntity.getRelationshipAttributes().get(KafkaTopicSchemaRelationshipDef.REL_MANY);
                if (kafkaTopicColl instanceof Collection) {
                    Collection<Map<String, ?>> headers = (Collection<Map<String, ?>>) kafkaTopicColl;
                    if (!headers.isEmpty()) {
                        Map<String, ?> header = (Map<String, ?>) CollectionUtils.get(headers, 0);
                        if (null != header.get(AtlasObjectId.KEY_GUID)) {
                            relationshipAlreadyExists = true;
                        }
                    }
                }
            }

            if (relationshipAlreadyExists) {
                LOG.debug("Schema {} is already connected to a topic.", schemaMetadataInfo.getId());
                return;
            }

            final String schemaName = schemaMetadataInfo.getSchemaMetadata().getName();
            Optional<AtlasEntityHeader> kafkaTopicEntity = findKafkaTopicEntityByName(schemaName);
            if (!kafkaTopicEntity.isPresent()) {
                LOG.info("Did not find Kafka topic with the name \"{}\"", schemaName);
                return;
            }

            AtlasRelationship relationship = new AtlasRelationship(KafkaTopicSchemaRelationshipDef.RELATIONSHIP_NAME,
                    new AtlasObjectId(kafkaTopicEntity.get().getGuid()), new AtlasObjectId(metaEntity.getGuid()));

            if (null != atlasClient.createRelationship(relationship)) {
                LOG.info("Successfully connected schema [{}] with its Kafka topic.", schemaName);
            }

        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Error while querying Atlas about the type model.", asex);
        }
    }

    private Optional<AtlasEntityHeader> findKafkaTopicEntityByName(@Nonnull String topicName) throws AtlasServiceException {
        if (StringUtils.isBlank(topicName)) {
            throw new IllegalArgumentException("Kafka topic name was null.");
        }

        AtlasSearchResult result = atlasClient.dslSearch(String.format("from %s where %s = '%s'",
                KafkaTopicEntityDef.KAFKA_TOPIC, KafkaTopicEntityDef.NAME, topicName));
        if (result == null || CollectionUtils.isEmpty(result.getEntities())) {
            return Optional.empty();
        }

        for (AtlasEntityHeader aeh : result.getEntities()) {
            if (KafkaTopicEntityDef.KAFKA_TOPIC.equals(aeh.getTypeName()) && topicName.equals(aeh.getAttribute(KafkaTopicEntityDef.NAME))) {
                return Optional.of(aeh);
            }
        }

        return Optional.empty();
    }

    private Optional<AtlasEntityDef> findTypeDefByName(String name) throws AtlasServiceException {
        SearchFilter searchFilter = new SearchFilter();
        searchFilter.setParam("name", name);
        AtlasTypesDef existingKafkaTopicTypes = atlasClient.getAllTypeDefs(searchFilter);
        if (existingKafkaTopicTypes != null && CollectionUtils.isNotEmpty(existingKafkaTopicTypes.getEntityDefs())) {
            AtlasEntityDef entityDef = existingKafkaTopicTypes.getEntityDefs().get(0);
            if (name.equals(entityDef.getName())) {
                return Optional.of(entityDef);
            }
        }
        return Optional.empty();
    }

    private Optional<AtlasRelationshipDef> findRelationshipDefByName(String name) throws AtlasServiceException {
        // TODO querying relationship typedefs doesn't work, remove this code after CDPD-28408 is fixed
        AtlasBaseClient.API api = new AtlasBaseClient.API(AtlasClientV2.TYPES_API + "relationshipdef/name/" + name, HttpMethod.GET, Response.Status.OK);
        return Optional.ofNullable(atlasClient.callAPI(api, AtlasRelationshipDef.class, null));
    }

    /** AtlasClient requires an array of host names to connect to. */
    @SuppressWarnings("unchecked")
    private String[] getAtlasHosts(Map<String, Object> config) {
        Object o = checkNotNull(config.get(ATLAS_HOSTS_PARAM), "List of urls was empty.");
        String[] result;
        if (o instanceof String[]) {
            result = (String[]) o;
        } else if (o instanceof String) {
            String urlString = (String) o;
            result = urlString.split("\\,");
        } else if (o instanceof ArrayList) {
            ArrayList<String> urls = (ArrayList<String>) o;
            result = urls.toArray(new String[urls.size()]);
        } else {
            throw new IllegalArgumentException("The list of Atlas hosts cannot be read. Please provide a list in readable format.");
        }

        if (result.length <= 0) {
            throw new AtlasUncheckedException(ERR_NO_URLS_PROVIDED);
        }

        return result;
    }

    /** This method is only invoked if Kerberos is disabled and Atlas falls back to basic authentication. */
    @SuppressWarnings("unchecked")
    @Nullable
    private String[] getBasicAuth(Map<String, Object> config) {
        if (config.get(ATLAS_BASIC_AUTH) != null) {
            Map<String, String> basicAuth = (Map<String, String>) config.get(ATLAS_BASIC_AUTH);
            if (!basicAuth.containsKey("username")) {
                return null;
            }
            return new String[] { basicAuth.get("username"), basicAuth.get("password") };
        }
        return null;
    }

    @Override
    public String createMeta(SchemaMetadataInfo schemaMetadata) {
        checkNotNull(schemaMetadata);

        LOG.debug("Create new meta for {}", schemaMetadata.getSchemaMetadata().getName());
        final long metaId = schemaMetadata.getId();
        try {
            AtlasEntity metaEntity = schemaMetadataTranslator.toAtlas(schemaMetadata.getSchemaMetadata(), metaId);
            AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            entitiesWithExtInfo.addEntity(metaEntity);

            if (LOG.isTraceEnabled()) {
                LOG.trace("Schema metadata: {}", schemaMetadata);
                LOG.trace("Schema atlas entity: {}", metaEntity.getAttributes());
            }

            EntityMutationResponse entities = atlasClient.createEntities(entitiesWithExtInfo);
            checkNotNull(entities, "Atlas returned an empty response for schema %s", schemaMetadata.getSchemaMetadata().getName());

            String metaGuid = entities.getGuidAssignments().get(metaEntity.getGuid());
            LOG.debug("Created entities: meta with GUID: \"{}\"", metaGuid);

            // update guids with newly assigned values (needed for creating the relationship)
            metaEntity.setGuid(metaGuid);

            return metaGuid;
        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Error creating new schema meta with id " + metaId, asex);
        }
    }

    @Override
    public Optional<SchemaMetadataInfo> updateMeta(SchemaMetadata schemaMetadata) throws SchemaNotFoundException {
        checkNotNull(schemaMetadata, "SchemaMetadata was null");
        checkNotNull(schemaMetadata.getName(), "Please provide the name of the schema");
        try {
            AtlasEntity metaEntity = getSchemaMetadataInfoAtlasEntity(schemaMetadata.getName());
            if (LOG.isTraceEnabled()) {
                LOG.trace("Updating schema metadata from {} to {}", metaEntity.getAttributes(), schemaMetadata);
            }
            schemaMetadataTranslator.updateEntity(metaEntity, schemaMetadata);

            AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
            entityWithExtInfo.setEntity(metaEntity);

            EntityMutationResponse response = atlasClient.updateEntity(entityWithExtInfo);
            List<AtlasEntityHeader> responseHeaders = response.getMutatedEntities().get(EntityMutations.EntityOperation.UPDATE);
            if (responseHeaders != null && !responseHeaders.isEmpty()) {
                boolean updateSuccess = responseHeaders.stream().anyMatch(h -> metaEntity.getGuid().equals(h.getGuid()));
                if (updateSuccess) {
                    return Optional.ofNullable(schemaMetadataTranslator.fromAtlasIntoInfo(metaEntity));
                } else {
                    LOG.warn("Metadata may not have been properly updated: {}", schemaMetadata);
                }
            }
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw new AtlasUncheckedException(asex);
        }

        return Optional.empty();
    }

    @Override
    public void addSchemaVersion(String schemaName, SchemaVersionInfo schemaVersion) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schemaName can't be null");
        checkNotNull(schemaVersion, "schemaVersion can't be null");

        LOG.debug("Add schema version to schema {}", schemaName);

        try {
            final AtlasEntity metaAtlasEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            final AtlasEntity versionEntity = schemaVersionTranslator.toAtlas(schemaVersion);

            AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
            entitiesWithExtInfo.addEntity(versionEntity);
            EntityMutationResponse entities = atlasClient.createEntities(entitiesWithExtInfo);

            // get back the guid of the new entity
            String versionGuid = entities.getGuidAssignments().get(versionEntity.getGuid());
            versionEntity.setGuid(versionGuid);

            // set up a relation between the version and the meta
            // TODO why do we query the entity back from Atlas when we have already persisted it?
            AtlasEntity.AtlasEntityWithExtInfo versionInfo = atlasClient.getEntityByGuid(versionGuid);
            AtlasRelationship metaRel = schemaVersionTranslator.createRelationship(metaAtlasEntity, versionInfo.getEntity());
            metaRel.setVersion(schemaVersion.getId());
            AtlasRelationship relResponse = atlasClient.createRelationship(metaRel);

            LOG.debug("Meta relationship with version GUID: {}", relResponse.getGuid());
        } catch (Exception ex) {
            throw new AtlasUncheckedException(ex);
        }
    }

    @Nonnull
    private AtlasEntity getSchemaMetadataInfoAtlasEntity(@Nonnull String schemaName) throws AtlasServiceException, SchemaNotFoundException {
        AtlasEntity.AtlasEntityWithExtInfo metaEntityInfo = atlasClient.getEntityByAttribute(
                MetadataEntityDef.SCHEMA_METADATA_INFO, ImmutableMap.of(MetadataEntityDef.NAME, schemaName));
        if (metaEntityInfo == null || metaEntityInfo.getEntity() == null) {
            throw new SchemaNotFoundException("Schema meta entity for name \"" + schemaName + "\" was null");
        }
        return metaEntityInfo.getEntity();
    }

}
