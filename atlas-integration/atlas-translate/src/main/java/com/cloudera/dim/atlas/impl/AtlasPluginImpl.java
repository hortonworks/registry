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
import com.cloudera.dim.atlas.translate.BranchTranslator;
import com.cloudera.dim.atlas.translate.SchemaMetadataTranslator;
import com.cloudera.dim.atlas.translate.SchemaVersionInfoTranslator;
import com.cloudera.dim.atlas.translate.SchemaVersionTranslator;
import com.cloudera.dim.atlas.translate.SerdesInfoTranslator;
import com.cloudera.dim.atlas.types.BranchEntityDef;
import com.cloudera.dim.atlas.types.MetaBranchRelationshipDef;
import com.cloudera.dim.atlas.types.MetadataEntityDef;
import com.cloudera.dim.atlas.types.Model;
import com.cloudera.dim.atlas.types.SchemaVersionRelationshipDef;
import com.cloudera.dim.atlas.types.SerdesEntityDef;
import com.cloudera.dim.atlas.types.SerdesMappingRelationshipDef;
import com.cloudera.dim.atlas.types.VersionBranchRelationshipDef;
import com.cloudera.dim.atlas.types.VersionEntityDef;
import com.cloudera.dim.atlas.types.VersionStateEntityDef;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AtlasPluginImpl implements AtlasPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasPluginImpl.class);

    private static final String ERR_NO_URLS_PROVIDED = "List of Atlas host URLs is empty.";

    private final SchemaMetadataTranslator schemaMetadataTranslator = new SchemaMetadataTranslator();
    private final SchemaVersionTranslator schemaVersionTranslator = new SchemaVersionTranslator();
    private final SchemaVersionInfoTranslator schemaVersionInfoTranslator = new SchemaVersionInfoTranslator();
    private final BranchTranslator branchTranslator = new BranchTranslator();
    private final SerdesInfoTranslator serdesInfoTranslator = new SerdesInfoTranslator();
    private final Map<String, IdGenerator> idGenerators = new HashMap<>();

    AtlasClientV2 atlasClient;

    public void initialize(Map<String, Object> config) {
        LOG.debug("Initializing the Atlas plugin");
        try {
            atlasClient = new AtlasClientV2(getAtlasHosts(config));

            idGenerators.put(MetadataEntityDef.SCHEMA_METADATA_INFO, new IdGenerator());
            idGenerators.put(VersionEntityDef.SCHEMA_VERSION_INFO, new IdGenerator());
            idGenerators.put(VersionStateEntityDef.SCHEMA_VERSION_STATE, new IdGenerator());
            idGenerators.put(BranchEntityDef.SCHEMA_BRANCH, new IdGenerator());
            idGenerators.put(SerdesEntityDef.SCHEMA_SERDES_INFO, new IdGenerator());
        } catch (Throwable ex) {
            throw new AtlasUncheckedException("Could not initialize the Atlas plugin implementation", ex);
        }
    }

    @Override
    public void setupAtlasModel() {
        try {
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

    /** AtlasClient requires an array of host names to connect to. */
    @SuppressWarnings("unchecked")
    private String[] getAtlasHosts(Map<String, Object> config) {
        Object o = checkNotNull(config.get(ATLAS_HOSTS_PARAM), "List of urls was empty.");
        String[] result;
        if (o instanceof String[]) {
            result = (String[])o;
        } else if (o instanceof String) {
            String urlString = (String)o;
            result = urlString.split("\\,");
        } else if (o instanceof ArrayList) {
            ArrayList<String> urls = (ArrayList<String>)o;
            result = urls.toArray(new String[urls.size()]);
        } else {
            throw new IllegalArgumentException("The list of Atlas hosts cannot be read. Please provide a list in readable format.");
        }

        if (result.length <= 0) {
            throw new AtlasUncheckedException(ERR_NO_URLS_PROVIDED);
        }

        return result;
    }

    @Override
    public Long createMeta(SchemaMetadata schemaMetadata) {
        checkNotNull(schemaMetadata);

        LOG.debug("Create new meta for {}", schemaMetadata.getName());
        return withRetry(
            randomize -> Pair.of(
                    generateUniqueId(MetadataEntityDef.SCHEMA_METADATA_INFO, randomize),
                    generateUniqueId(BranchEntityDef.SCHEMA_BRANCH, randomize)),
            ids -> {
                final long metaId = ids.getLeft();
                final long branchId = ids.getRight();
                try {
                    AtlasEntity metaEntity = schemaMetadataTranslator.toAtlas(schemaMetadata, metaId);
                    AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
                    entitiesWithExtInfo.addEntity(metaEntity);

                    AtlasEntity branchEntity = branchTranslator.toAtlas(new SchemaBranch(branchId, SchemaBranch.MASTER_BRANCH, schemaMetadata.getName(), String.format(SchemaBranch.MASTER_BRANCH_DESC, schemaMetadata.getName()), System.currentTimeMillis()));
                    entitiesWithExtInfo.addEntity(branchEntity);

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Schema metadata: {}", schemaMetadata);
                        LOG.trace("Schema atlas entity: {}", metaEntity.getAttributes());
                        LOG.trace("Branch entity: {}", branchEntity.getAttributes());
                    }

                    EntityMutationResponse entities = atlasClient.createEntities(entitiesWithExtInfo);
                    checkNotNull(entities, "Atlas returned an empty response for schema %s", schemaMetadata.getName());

                    String metaGuid = entities.getGuidAssignments().get(metaEntity.getGuid());
                    String branchGuid = entities.getGuidAssignments().get(branchEntity.getGuid());
                    LOG.debug("Created entities: meta and branch, with GUIDs: \"{}\", \"{}\"", metaGuid, branchGuid);

                    // update guids with newly assigned values (needed for creating the relationship)
                    metaEntity.setGuid(metaGuid);
                    branchEntity.setGuid(branchGuid);

                    // TODO CDPD-18841 Creating the relationship should be done in the same request as the entities.

                    AtlasRelationship relationship = branchTranslator.createRelationshipWithMeta(metaEntity, branchEntity);
                    LOG.debug("Creating relationship {}", relationship.getTypeName());
                    relationship.setAttribute(BranchEntityDef.NAME, SchemaBranch.MASTER_BRANCH);
                    atlasClient.createRelationship(relationship);

                } catch (AtlasServiceException asex) {
                    throw new AtlasUncheckedException("Error creating new schema meta with id " + metaId, asex);
                }

                return ids;
            }).getLeft();
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
    public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion,
                                            String fingerprint, SchemaBranch schemaBranch) throws SchemaNotFoundException {
        checkNotNull(schemaBranch, "Schema branch can't be null");
        checkNotNull(schemaName, "schemaName can't be null");
        checkNotNull(schemaVersion, "schemaVersion can't be null");
        checkNotNull(fingerprint, "schema fingerprint can't be null");

        LOG.debug("Add schema version to schema {}", schemaName);

        try {
            final AtlasEntity metaAtlasEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            int existingCount = getExistingVersionsCount(metaAtlasEntity);
            LOG.debug("Found {} existing versions.", existingCount);

            SchemaMetadataInfo schemaMetadataInfo = schemaMetadataTranslator.fromAtlasIntoInfo(metaAtlasEntity);

            long vid = withRetry(
                    randomize -> generateUniqueId(VersionEntityDef.SCHEMA_VERSION_INFO, randomize),
                    versionId -> {
                        try {
                            AtlasEntity versionEntity = schemaVersionTranslator.toAtlas(versionId, schemaVersion, schemaMetadataInfo, schemaName, existingCount + 1, fingerprint);
                            AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
                            entitiesWithExtInfo.addEntity(versionEntity);
                            EntityMutationResponse entities = atlasClient.createEntities(entitiesWithExtInfo);

                            // get back the guid of the new entity
                            String versionGuid = entities.getGuidAssignments().get(versionEntity.getGuid());
                            versionEntity.setGuid(versionGuid);

                            // set up a relation between the version and the meta
                            AtlasEntity.AtlasEntityWithExtInfo versionInfo = atlasClient.getEntityByGuid(versionGuid);
                            AtlasRelationship metaRel = schemaVersionTranslator.createRelationship(metaAtlasEntity, versionInfo.getEntity());
                            metaRel.setVersion(versionId);
                            AtlasRelationship relResponse = atlasClient.createRelationship(metaRel);

                            LOG.debug("Meta relationship GUID: {}", relResponse.getGuid());

                            // set up a relation between the version and the branch
                            AtlasEntity.AtlasEntityWithExtInfo branchEntity = atlasClient.getEntityByAttribute(BranchEntityDef.SCHEMA_BRANCH,
                                    ImmutableMap.of(BranchEntityDef.ID, String.valueOf(schemaBranch.getId())));
                            AtlasRelationship branchRel = branchTranslator.createRelationshipWithVersion(versionEntity, branchEntity.getEntity());
                            AtlasRelationship branchResponse = atlasClient.createRelationship(branchRel);

                            LOG.debug("Branch relationship GUID: {}", branchResponse.getGuid());

                            return versionId;
                        } catch (AtlasServiceException asex) {
                            throw new AtlasUncheckedException(asex);
                        }
                    });

            return new SchemaIdVersion(schemaMetadataInfo.getId(), existingCount + 1, vid);
        } catch (SchemaNotFoundException snfex) {
            throw snfex;
        } catch (Exception ex) {
            throw new AtlasUncheckedException(ex);
        }
    }

    @Override
    public SchemaBranch createBranch(SchemaVersionInfo schemaVersion, String branchName) throws SchemaNotFoundException {
        checkNotNull(schemaVersion, "schemaVersion");
        checkNotNull(branchName, "branchName");

        final String metaName = checkNotNull(schemaVersion.getName(), "schema name");
        final AtlasEntity metaEntity, versionEntity;
        try {
            metaEntity = getSchemaMetadataInfoAtlasEntity(metaName);
        } catch (AtlasServiceException e) {
            throw new SchemaNotFoundException("Schema not found: " + metaName);
        }
        try {
            versionEntity = getVersionAtlasEntity(schemaVersion.getId());
        } catch (AtlasServiceException e) {
            throw new SchemaNotFoundException("Schema not found: " + metaName);
        }

        final Function<Long, SchemaBranch> makeBranchObject = branchId ->
                new SchemaBranch(branchId, branchName, metaName, String.format(branchName, metaName), System.currentTimeMillis());

        final Long persistedBranchId = withRetry(
            randomize -> generateUniqueId(BranchEntityDef.SCHEMA_BRANCH, randomize),
            branchId -> {
                SchemaBranch schemaBranch = makeBranchObject.apply(branchId);
                try {
                    AtlasEntity branchEntity = branchTranslator.toAtlas(schemaBranch);
                    AtlasEntity.AtlasEntityWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
                    entitiesWithExtInfo.setEntity(branchEntity);

                    EntityMutationResponse result = atlasClient.createEntity(entitiesWithExtInfo);
                    String guid = result.getGuidAssignments().get(branchEntity.getGuid());
                    branchEntity.setGuid(guid);

                    AtlasRelationship relationshipWithMeta = branchTranslator.createRelationshipWithMeta(metaEntity, branchEntity);
                    atlasClient.createRelationship(relationshipWithMeta);

                    AtlasRelationship relationshipWithVersion = branchTranslator.createRelationshipWithVersion(versionEntity, branchEntity);
                    atlasClient.createRelationship(relationshipWithVersion);
                } catch (AtlasServiceException asex) {
                    throw new AtlasUncheckedException("Could not create branch " + branchName + " for schema " + metaName, asex);
                }

                return branchId;
            });


        return makeBranchObject.apply(persistedBranchId);
    }

    @Override
    public Optional<SchemaBranch> getSchemaBranch(String schemaName, String branchName) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schema name");
        try {
            AtlasEntity metaEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            Object branches = metaEntity.getRelationshipAttribute(MetaBranchRelationshipDef.REL_ONE);
            if (branches instanceof Collection) {
                // relationship contains the branch name, so we can obtain the guid without having to query the full rel.
                Collection<SchemaBranch> schemaBranches = extractSchemaBranchesFromAtlas((Collection<?>) branches,
                        f -> branchName.equals(f.get(BranchEntityDef.NAME)));
                if (schemaBranches == null || schemaBranches.isEmpty()) {
                    LOG.debug("Schema \"{}\" does not have any branches with the name \"{}\"", schemaName, branchName);
                    return Optional.empty();
                }
                if (schemaBranches.size() > 1) {
                    LOG.warn("Found multiple branches with the name \"{}\" for schema \"{}\"", branchName, schemaName);
                }
                return Optional.ofNullable(Iterables.getFirst(schemaBranches, null));
            }
        } catch (AtlasServiceException e) {
            if (e.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw new AtlasUncheckedException(e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<SchemaBranch> getSchemaBranchById(Long branchId) {
        try {
            AtlasEntity.AtlasEntityWithExtInfo branchEntity = atlasClient.getEntityByAttribute(
                    BranchEntityDef.SCHEMA_BRANCH, ImmutableMap.of(BranchEntityDef.ID, String.valueOf(branchId)));
            if (branchEntity == null || branchEntity.getEntity() == null) {
                return Optional.empty();
            }

            return Optional.ofNullable(branchTranslator.fromAtlas(branchEntity.getEntity()));
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw new AtlasUncheckedException("Error while searching for schema branch with id " + branchId, asex);
        }
    }

    @Override
    public Long addSerdes(SerDesPair serdes) {
        return withRetry(
            randomize -> generateUniqueId(SerdesEntityDef.SCHEMA_SERDES_INFO, randomize),
            id -> {
                try {
                    AtlasEntity atlasEntity = serdesInfoTranslator.toAtlas(new SerDesInfo(id, System.currentTimeMillis(), serdes));
                    AtlasEntity.AtlasEntityWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
                    entitiesWithExtInfo.setEntity(atlasEntity);

                    EntityMutationResponse response = atlasClient.createEntity(entitiesWithExtInfo);
                    checkNotNull(response, "Atlas returned an empty response for serdes %s", serdes.getName());

                    Iterables.getOnlyElement(response.getCreatedEntities());
                } catch (AtlasServiceException asex) {
                    throw new AtlasUncheckedException("Error creating new serdes with id " + id, asex);
                }

                return id;
            });
    }

    @Override
    public Optional<SerDesInfo> getSerdesById(Long serDesId) {
        try {
            return Optional.of(serdesInfoTranslator.fromAtlas(getSerdesAtlasEntity(serDesId)));
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw new AtlasUncheckedException("Error getting serdes by ID " + serDesId, asex);
        }
    }

    @Override
    public void mapSchemaWithSerdes(String schemaName, Long serDesId) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schema name");
        checkNotNull(serDesId, "serdes ID");
        try {
            LOG.trace("Get schema \"{}\" and serdes {}", schemaName, serDesId);
            AtlasEntity metaEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            AtlasEntity serdesEntity = getSerdesAtlasEntity(serDesId);
            AtlasRelationship relationship = serdesInfoTranslator.createRelationship(metaEntity, serdesEntity);
            LOG.debug("Creating relationship {}", relationship.getTypeName());
            atlasClient.createRelationship(relationship);
        } catch (AtlasServiceException asex) {
            LOG.error("111", asex);
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                throw new SerDesException("Serializer with given ID " + serDesId + " does not exist");
            }
            throw new AtlasUncheckedException("Error creating relation between schema \"" + schemaName + "\" and serdes with ID " + serDesId, asex);
        } catch (SchemaNotFoundException snfex) {
            throw snfex;
        } catch (Exception ex) {
            LOG.error("222", ex);
            throw ex;
        }
    }

    // TODO refactor this, merge with getAllSchemaVersions
    @Override
    public Collection<SerDesInfo> getAllSchemaSerdes(String schemaName) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schemaName");

        LOG.info("Get all serdes for {}", schemaName);
        try {
            final AtlasEntity metaAtlasEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            if (metaAtlasEntity.getRelationshipAttributes() != null) {
                Object serdes = metaAtlasEntity.getRelationshipAttributes().get(SerdesMappingRelationshipDef.REL_ONE);
                if (serdes instanceof Collection) {
                    return extractSchemaSerdesFromAtlas((Collection<?>) serdes);
                }
            }

        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Could not retrieve all serdes for schema "+schemaName, asex);
        }

        // happens if relationship is missing in Atlas
        LOG.debug("No serdes have been found for schema \"{}\".", schemaName);
        return ImmutableList.of();
    }

    private long generateUniqueId(@Nonnull String typeName, boolean randomize) {
        IdGenerator idGenerator = idGenerators.get(typeName);
        if (idGenerator == null) {
            throw new Error("ID generator not implemented for " + typeName);
        }
        if (randomize) {
            idGenerator.randomize();
        }
        return idGenerator.nextId();
    }

    /** Get the number of existing versions of the schema meta. */
    private int getExistingVersionsCount(AtlasEntity metaAtlasEntity) {
        if (metaAtlasEntity.getRelationshipAttributes() != null) {
            Object versions = metaAtlasEntity.getRelationshipAttributes().get(SchemaVersionRelationshipDef.REL_ONE);
            if (versions instanceof Collection) {
                return ((Collection<?>) versions).size();
            }
        }

        return 0;
    }

    @Nonnull
    private AtlasEntity getSchemaMetadataInfoAtlasEntity(@Nonnull String schemaName) throws AtlasServiceException, SchemaNotFoundException {
        AtlasEntity.AtlasEntityWithExtInfo metaEntityInfo = atlasClient.getEntityByAttribute(
                MetadataEntityDef.SCHEMA_METADATA_INFO, ImmutableMap.of(MetadataEntityDef.NAME, schemaName));
        if (metaEntityInfo == null || metaEntityInfo.getEntity() == null) {
            throw new SchemaNotFoundException("Schema meta entity for name \""+schemaName+"\" was null");
        }
        return metaEntityInfo.getEntity();
    }

    @Nonnull
    private AtlasEntity getSchemaMetadataInfoAtlasEntity(Long id) throws AtlasServiceException, SchemaNotFoundException {
        AtlasEntity.AtlasEntityWithExtInfo metaEntityInfo = atlasClient.getEntityByAttribute(
                MetadataEntityDef.SCHEMA_METADATA_INFO, ImmutableMap.of(MetadataEntityDef.SCHEMA_METADATA_ID, String.valueOf(id)));
        if (metaEntityInfo == null || metaEntityInfo.getEntity() == null) {
            throw new SchemaNotFoundException("Schema meta entity for id \""+id+"\" was null");
        }
        return metaEntityInfo.getEntity();
    }

    @Nonnull
    private AtlasEntity getVersionAtlasEntity(Long versionId) throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo entityInfo = atlasClient.getEntityByAttribute(
                VersionEntityDef.SCHEMA_VERSION_INFO, ImmutableMap.of(VersionEntityDef.ID, String.valueOf(versionId)));
        if (entityInfo == null || entityInfo.getEntity() == null) {
            throw new SerDesException("Schema version with given ID " + versionId + " does not exist");
        }
        return entityInfo.getEntity();
    }

    @Nonnull
    private AtlasEntity getSerdesAtlasEntity(Long serdesId) throws AtlasServiceException {
        AtlasEntity.AtlasEntityWithExtInfo entityInfo = atlasClient.getEntityByAttribute(
                SerdesEntityDef.SCHEMA_SERDES_INFO, ImmutableMap.of(SerdesEntityDef.ID, String.valueOf(serdesId)));
        if (entityInfo == null || entityInfo.getEntity() == null) {
            throw new SerDesException("Serializer with given ID " + serdesId + " does not exist");
        }
        return entityInfo.getEntity();
    }

    @Override
    public Optional<SchemaMetadataInfo> getSchemaMetadataInfo(String schemaName) {
        return getSchemaMetadataInfo(schemaName, String.class);
    }

    @Override
    public Optional<SchemaMetadataInfo> getSchemaMetadataInfo(Long metaId) {
        return getSchemaMetadataInfo(metaId, Long.class);
    }

    private <T> Optional<SchemaMetadataInfo> getSchemaMetadataInfo(T id, Class<T> clazz) {
        checkNotNull(id, "id can't be null");

        try {
            AtlasEntity metaEntity;
            if (clazz == Long.class)
                metaEntity = getSchemaMetadataInfoAtlasEntity((Long)id);
            else if (clazz == String.class)
                metaEntity = getSchemaMetadataInfoAtlasEntity((String)id);
            else
                throw new Error("Unsupported id type: " + clazz);

            return Optional.ofNullable(schemaMetadataTranslator.fromAtlasIntoInfo(metaEntity));
        } catch (SchemaNotFoundException npex) {
            return Optional.empty();
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw new AtlasUncheckedException(asex);
        } catch (Exception ex) {
            throw new AtlasUncheckedException(ex);
        }
    }

    @Override
    public Optional<SchemaVersionInfo> getSchemaVersion(String schemaName, Integer version) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schema name");
        checkNotNull(version, "version");
        try {
            final AtlasEntity metaAtlasEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            if (metaAtlasEntity.getRelationshipAttributes() != null) {
                Object versions = metaAtlasEntity.getRelationshipAttributes().get(SchemaVersionRelationshipDef.REL_ONE);
                if (versions instanceof Collection) {
                    Collection<SchemaVersionInfo> versionsFound = extractSchemaVersionsFromAtlas((Collection<?>) versions,
                            map -> map != null && version.equals(map.get(VersionEntityDef.VERSION)));

                    if (versionsFound.size() > 1) {
                        LOG.warn("Found multiple versions {} for schema \"{}\"", version, schemaName);
                    }
                    return Optional.ofNullable(Iterables.getFirst(versionsFound, null));
                }
            }
        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Could not retrieve schema "+schemaName+" with version " + version, asex);
        }

        return Optional.empty();
    }

    @Override
    public Optional<SchemaVersionInfo> getSchemaVersionById(Long versionId) {
        checkNotNull(versionId, "versionId");
        try {
            AtlasEntity.AtlasEntityWithExtInfo atlasEntity = atlasClient.getEntityByAttribute(VersionEntityDef.SCHEMA_VERSION_INFO, ImmutableMap.of(VersionEntityDef.ID, String.valueOf(versionId)));
            if (atlasEntity == null || atlasEntity.getEntity() == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(schemaVersionInfoTranslator.fromAtlas(atlasEntity.getEntity()));
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return Optional.empty();
            }
            throw new AtlasUncheckedException("Could not get schema version by its ID " + versionId, asex);
        }
    }

    @Override
    public List<SchemaVersionInfo> getSchemaVersionsByBranchId(Long branchId) throws SchemaBranchNotFoundException {
        checkNotNull(branchId, "branchId");
        try {
            AtlasEntity.AtlasEntityWithExtInfo atlasEntity = atlasClient.getEntityByAttribute(BranchEntityDef.SCHEMA_BRANCH, ImmutableMap.of(BranchEntityDef.ID, String.valueOf(branchId)));
            if (atlasEntity == null || atlasEntity.getEntity() == null) {
                throw new SchemaBranchNotFoundException("Did not find branch with id " + branchId);
            }

            Object versions = atlasEntity.getEntity().getRelationshipAttributes().get(VersionBranchRelationshipDef.REL_MANY);
            if (versions instanceof Collection) {
                return new ArrayList<>(extractSchemaVersionsFromAtlas((Collection<?>) versions, null));
            }

        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                throw new SchemaBranchNotFoundException("Did not find branch with id " + branchId);
            }
            throw new AtlasUncheckedException("Could not get schema version by the branch ID " + branchId, asex);
        }

        return new ArrayList<>();
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranchesByVersionId(Long versionId) throws SchemaBranchNotFoundException {
        checkNotNull(versionId, "versionId");
        try {
            AtlasEntity.AtlasEntityWithExtInfo atlasEntity = atlasClient.getEntityByAttribute(VersionEntityDef.SCHEMA_VERSION_INFO, ImmutableMap.of(VersionEntityDef.ID, String.valueOf(versionId)));
            if (atlasEntity == null || atlasEntity.getEntity() == null) {
                throw new SchemaBranchNotFoundException("Did not find schema version with ID " + versionId);
            }

            Object branches = atlasEntity.getEntity().getRelationshipAttributes().get(VersionBranchRelationshipDef.REL_ONE);
            if (branches instanceof Collection) {
                return extractSchemaBranchesFromAtlas((Collection<?>) branches, null);
            }
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                throw new SchemaBranchNotFoundException("Did not find schema version with ID " + versionId);
            }
            throw new AtlasUncheckedException("Could not get schema branches by their version ID " + versionId, asex);
        }

        return ImmutableList.of();
    }

    @Override
    public Collection<SchemaVersionInfo> getAllSchemaVersions(String schemaName) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schemaName");

        LOG.info("Get all versions for {}", schemaName);
        try {
            final AtlasEntity metaAtlasEntity = getSchemaMetadataInfoAtlasEntity(schemaName);
            if (metaAtlasEntity.getRelationshipAttributes() != null) {
                Object versions = metaAtlasEntity.getRelationshipAttributes().get(SchemaVersionRelationshipDef.REL_ONE);
                if (versions instanceof Collection) {
                    return extractSchemaVersionsFromAtlas((Collection<?>) versions);
                }
            }

        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Could not retrieve all versions for schema "+schemaName, asex);
        }

        // happens if relationship is missing in Atlas
        LOG.debug("No versions have been found for schema \"{}\".", schemaName);
        return ImmutableList.of();
    }

    private Collection<SchemaVersionInfo> extractSchemaVersionsFromAtlas(Collection<?> versions) throws AtlasServiceException {
        return extractSchemaVersionsFromAtlas(versions, null);
    }

    private Collection<SchemaVersionInfo> extractSchemaVersionsFromAtlas(
            Collection<?> versions, @Nullable Predicate<Map<String, ?>> filter) throws AtlasServiceException {

        return extractRelatedEntitiesFromAtlas(versions, filter, schemaVersionInfoTranslator::fromAtlas, VersionEntityDef.SCHEMA_VERSION_INFO);
    }

    private Collection<SchemaBranch> extractSchemaBranchesFromAtlas(
            Collection<?> branches, @Nullable Predicate<Map<String, ?>> filter) throws AtlasServiceException {
        return extractRelatedEntitiesFromAtlas(branches, filter, branchTranslator::fromAtlas, BranchEntityDef.SCHEMA_BRANCH);
    }

    private Collection<SerDesInfo> extractSchemaSerdesFromAtlas(Collection<?> serdes) throws AtlasServiceException {
        return extractRelatedEntitiesFromAtlas(serdes, null, serdesInfoTranslator::fromAtlas, SerdesEntityDef.SCHEMA_SERDES_INFO);
    }

    private <T> Collection<T> extractRelatedEntitiesFromAtlas(
            Collection<?> relatedList, @Nullable Predicate<Map<String, ?>> filter,
            Function<AtlasEntity, T> converter, String requiredType) throws AtlasServiceException {
        List<String> guids = relatedList.stream().map(header -> {
            if (header instanceof Map) {
                String guid = (String) ((Map) header).get(AtlasObjectId.KEY_GUID);
                String type = (String) ((Map) header).get(AtlasObjectId.KEY_TYPENAME);
                // Relationships can have attributes; these are nested as map attributes inside
                // map relationshipAttributes inside map of headers
                Map attributes = (Map) ((Map) header).get(AtlasRelatedObjectId.KEY_RELATIONSHIP_ATTRIBUTES);
                if (attributes != null) {
                    attributes = (Map)attributes.get("attributes");
                }
                if (attributes != null && filter != null && !filter.apply(attributes)) {
                    return "";
                }
                if (requiredType.equals(type)) {
                    return guid;
                }
                return "";
            }
            return "";
        }).filter(s -> !"".equals(s)).collect(Collectors.toList());

        if (guids.isEmpty()) {
            LOG.info("No guids were found for {}", requiredType);
        } else {
            LOG.info("Guids for {}: {}", requiredType, String.join(", ", guids));
        }
        if (guids.isEmpty()) {
            return ImmutableList.of();
        }

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesByGuids = atlasClient.getEntitiesByGuids(guids);
        return entitiesByGuids.getEntities().stream().map(converter).collect(Collectors.toList());
    }

    @Override
    public Collection<SchemaMetadataInfo> search(Optional<String> nameOpt, Optional<String> descOpt, Optional<String> orderBy) {
        LOG.debug("Search for schemas, name=\"{}\", desc=\"{}\"", nameOpt, descOpt.orElse(null));

        ImmutableMap.Builder<String, String> paramBuild = ImmutableMap.builder();
        nameOpt.ifPresent(name -> { if (!name.trim().isEmpty()) { paramBuild.put(MetadataEntityDef.NAME, name.trim()); } });
        descOpt.ifPresent(description -> { if (!description.trim().isEmpty()) { paramBuild.put(MetadataEntityDef.DESCRIPTION, description.trim()); } });
        final Map<String, String> params = paramBuild.build();

        List<? extends AtlasStruct> entities;
        AtlasEntity.AtlasEntitiesWithExtInfo metaEntityInfo;
        try {
            // simple search
            // TODO this searches in all fields; look at DSL query to search by specific fields
            String query = params.get(MetadataEntityDef.NAME);
            AtlasSearchResult atlasSearchResult = basicSearch(MetadataEntityDef.SCHEMA_METADATA_INFO, query, orderBy);
            //
            if (atlasSearchResult == null || atlasSearchResult.getEntities() == null) {
                return ImmutableList.of();
            } else {
                List<AtlasEntityHeader> headers = atlasSearchResult.getEntities();
                metaEntityInfo = atlasClient.getEntitiesByGuids(headers.stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList()));
            }

            if (metaEntityInfo == null || metaEntityInfo.getEntities() == null) {
                entities = ImmutableList.of();
            } else {
                entities = metaEntityInfo.getEntities();
            }

            if (entities.isEmpty()) {
                LOG.debug("No schemas were found.");
                return ImmutableList.of();
            }

            // TODO orderBy
            return entities.stream()
                    .map(schemaMetadataTranslator::fromAtlasIntoInfo)
                    .collect(Collectors.toList());

        } catch (AtlasServiceException asex) {
            throw new AtlasUncheckedException("Exception while searching for schemas by name " + nameOpt, asex);
        }
    }

    @Override
    public Collection<SchemaVersionInfo> searchVersions(String schemaName, String fingerprint) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schemaName");
        checkNotNull(fingerprint, "fingerprint");

        try {
            AtlasEntity metaEntity = getSchemaMetadataInfoAtlasEntity(schemaName);

            Object versions = metaEntity.getRelationshipAttribute(SchemaVersionRelationshipDef.REL_ONE);
            if (versions instanceof Collection) {
                // relationship contains the fingerprint, so we can filter for only those versions which match
                return extractSchemaVersionsFromAtlas((Collection<?>) versions,
                        f -> fingerprint.equals(f.get(VersionEntityDef.FINGERPRINT)));
            }
        } catch (AtlasServiceException asex) {
            if (asex.getStatus() == ClientResponse.Status.NOT_FOUND) {
                return ImmutableList.of();
            } else {
                throw new AtlasUncheckedException("Could not search for schema " + schemaName + " and version fingerprint " + fingerprint, asex);
            }
        }

        return ImmutableList.of();
    }

    private AtlasSearchResult basicSearch(String type, String query, Optional<String> orderByStr) throws AtlasServiceException {
        List<String> sortBy = null;
        Boolean descending = null;
        if (orderByStr.isPresent()) {
            Pair<List<String>, Boolean> orderByFields = getOrderByFields(orderByStr.get());
            sortBy = orderByFields.getLeft();
            descending = orderByFields.getRight();
        }

        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("typeName", type);
        queryParams.add("classification", null);
        queryParams.add(AtlasBaseClient.QUERY, query);
        queryParams.add("excludeDeletedEntities", String.valueOf(true));
        queryParams.add(AtlasBaseClient.LIMIT, String.valueOf(0));
        queryParams.add(AtlasBaseClient.OFFSET, String.valueOf(0));
        queryParams.add("sortBy", sortBy == null ? null : String.join(",", sortBy));
        queryParams.add("sortOrder", descending == null ? null : descending ? "DESCENDING" : "ASCENDING");

        return atlasClient.callAPI(AtlasClientV2.API_V2.BASIC_SEARCH, AtlasSearchResult.class, queryParams);
    }

    private Pair<List<String>, Boolean> getOrderByFields(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }

        List<String> orderByList = new ArrayList<>();
        // _orderByFields=[<field-name>,<a/d>,]*
        // example can be : _orderByFields=foo,a,bar,d
        // order by foo with ascending then bar with descending
        Boolean descending = null;
        String[] splitStrings = value.split(",");
        for (int i = 0; i < splitStrings.length; i += 2) {
            String ascStr = splitStrings[i + 1];
            if ("a".equals(ascStr)) {
                descending = false;
            } else if ("d".equals(ascStr)) {
                descending = true;
            } else {
                throw new IllegalArgumentException("Ascending or Descending identifier can only be 'a' or 'd' respectively.");
            }

            String fieldName = splitStrings[i];
            orderByList.add(fieldName);
        }

        return Pair.of(orderByList, descending);
    }

    @Override
    public Collection<SerDesInfo> getSerDesMappingsForSchema(String schemaName) throws SchemaNotFoundException {
        checkNotNull(schemaName, "schema name");
        try {
            AtlasEntity atlasEntity = getSchemaMetadataInfoAtlasEntity(schemaName);

            if (atlasEntity.getRelationshipAttributes() != null) {
                Object serdes = atlasEntity.getRelationshipAttributes().get(SerdesMappingRelationshipDef.REL_MANY);
                if (serdes instanceof Collection) {
                    // TODO I don't know what will be in this collection
                    //((Collection<?>) serdes);
                    for (Object o : ((Collection)serdes)) {
                        LOG.info("rel = {}", o);
                    }
                } else {
                    LOG.error("NOT A COLLECTION");
                }
            }

        } catch (AtlasServiceException e) {
            e.printStackTrace();
        }

        return ImmutableList.of();
    }

    @VisibleForTesting
    // TODO CDPD-18826 Implement a smarter retry algorithm
    <T> T withRetry(Function<Boolean, T> idProvider, Function<T, T> func) {
        T result = null;
        boolean repeat = true;
        int counter = 0;
        while (repeat) {
            T id = idProvider.apply(counter > 0);
            try {
                LOG.trace("Attempting to insert entity with id {}", id);
                result = func.apply(id);
                repeat = false;
            } catch (Exception ex) {
                counter++;
                if (counter < 2) {
                    LOG.error("Error inserting new entity with id {}. Retrying with another id.", id);
                } else {
                    throw ex;
                }
            }
        }
        return result;
    }
}
