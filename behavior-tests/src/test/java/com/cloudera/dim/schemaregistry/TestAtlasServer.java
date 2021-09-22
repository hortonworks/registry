/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.cloudera.dim.schemaregistry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.mock.Expectation;
import org.mockserver.model.ExpectationId;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.MediaType;
import org.mockserver.model.NottableString;
import org.mockserver.model.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.cloudera.dim.schemaregistry.GlobalState.ATLAS_ENTITIES;
import static com.cloudera.dim.schemaregistry.GlobalState.ATLAS_ENTITY_UPDATE;
import static com.cloudera.dim.schemaregistry.GlobalState.ATLAS_RELATIONSHIPS;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;
import static org.mockserver.verify.VerificationTimes.exactly;

public class TestAtlasServer extends AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(TestAtlasServer.class);

    public static final String SCHEMA_META_TYPE_NAME = "schema_metadata_info";
    public static final String SCHEMA_VERSION_TYPE_NAME = "schema_version_info";
    public static final String META_VERSION_REL_TYPE_NAME = "schema_version";
    public static final String TOPIC_SCHEMA_REL_TYPE_NAME = "topic_schema";
    public static final String KAFKA_TOPIC_TYPEDEF_NAME = "kafka_topic";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final GlobalState sow;
    private ClientAndServer mockWebServer;

    private Expectation createEntityExpectation;
    private Expectation createRelationshipExpectation;
    private Expectation queryByGuidExpectation;
    private Expectation updateMetaExpectation;
    private Expectation createModelExpectation;

    public TestAtlasServer(GlobalState sow) {
        this.sow = sow;
    }

    public int getAtlasPort() {
        return mockWebServer.getLocalPort();
    }

    @Override
    public void start() throws Exception {
        boolean alreadyStarted = started.getAndSet(true);
        if (alreadyStarted) {
            return;
        }
        LOG.info("Starting mock Atlas server");

        mockWebServer = ClientAndServer.startClientAndServer();

        createEntityExpectation = createEntity();
        updateMetaExpectation = updateMeta();
        queryMetaByName();
        queryByGuidExpectation = queryByGuid();
        createRelationshipExpectation = createOneToManyRelationship();
        createModelExpectation = createAtlasModel();

        // we need to set the -Datlas.conf property for AtlasClient to work
        String atlasProps = configGenerator.generateAtlasProperties();
        File atlasPropFile = writeFile("atlas-application", ".properties", atlasProps);
        System.setProperty("atlas.conf", atlasPropFile.getParent());
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping mock Atlas server");
        mockWebServer.close();
        super.stop();
    }

    private HttpResponse jsonResponse(Object value) {
        return response().withBody(serializeToJson(value), MediaType.JSON_UTF_8);
    }

    private String serializeToJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize test data", e);
        }
    }

    @Nullable
    private String getParamValue(HttpRequest request, String name) {
        List<Parameter> parameters = request.getQueryStringParameterList();
        for (Parameter param : parameters) {
            if (name.equals(param.getName().getValue())) {
                List<NottableString> values = param.getValues();
                if (CollectionUtils.isEmpty(values)) {
                    return null;
                } else {
                    return values.get(0).getValue();
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void queryMetaByName() {
        AtlasEntity.AtlasEntityWithExtInfo response = new AtlasEntity.AtlasEntityWithExtInfo();

        HttpRequest expectedRequest = request()
            .withMethod("GET")
            .withPath("/api/atlas/v2/entity/uniqueAttribute/type/" + SCHEMA_META_TYPE_NAME);

        mockWebServer
            .when(expectedRequest)
            .respond(request -> {
                // client queries meta by its unique name
                String schemaName = getParamValue(request, "attr:name");

                // we check SOW if there are any entities with that name
                List<AtlasEntity> entities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITIES);
                if (entities != null) {
                    for (AtlasEntity entity : entities) {
                        if (SCHEMA_META_TYPE_NAME.equals(entity.getTypeName()) && entity.getAttribute("name").equals(schemaName)) {
                            LOG.debug("Found schema meta with name [{}]", schemaName);
                            response.setEntity(entity);
                            break;
                        }
                    }
                }

                return jsonResponse(response);
            });
    }

    @SuppressWarnings("unchecked")
    private Expectation createEntity() {
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        HttpRequest expectedRequest = request()
                .withMethod("POST")
                .withPath("/api/atlas/v2/entity/bulk/")
                .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                .withBody(json(serializeToJson(entitiesWithExtInfo), MatchType.ONLY_MATCHING_FIELDS));

        Expectation[] expectations = mockWebServer
                .when(expectedRequest)
                .respond(request -> {
                    AtlasEntity.AtlasEntitiesWithExtInfo body = objectMapper.readValue(request.getBodyAsString(), AtlasEntity.AtlasEntitiesWithExtInfo.class);
                    checkNotNull(body.getEntities(), "No entities in the request.");
                    checkState(!body.getEntities().isEmpty(), "List of entities was empty.");

                    final List<AtlasEntity> persistedEntities;
                    if (sow.getValue(ATLAS_ENTITIES) != null) {
                        persistedEntities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITIES);
                    } else {
                        persistedEntities = new CopyOnWriteArrayList<>();
                        sow.setValue(ATLAS_ENTITIES, persistedEntities);
                    }

                    ImmutableMap.Builder<String, String> guidAssignments = ImmutableMap.builder();
                    for (AtlasEntity entity : body.getEntities()) {
                        String oldGuid = entity.getGuid();
                        entity.setGuid(RandomStringUtils.randomAlphanumeric(8));
                        guidAssignments.put(oldGuid, entity.getGuid());
                        persistedEntities.add(entity);
                    }
                    EntityMutationResponse response = new EntityMutationResponse();
                    response.setGuidAssignments(guidAssignments.build());

                    return jsonResponse(response);
                });

        return expectations[0];
    }

    @SuppressWarnings("unchecked")
    private Expectation createOneToManyRelationship() {
        HttpRequest expectedRequest = request()
                .withMethod("POST")
                .withPath("/api/atlas/v2/relationship/")
                .withContentType(MediaType.APPLICATION_JSON_UTF_8);

        Expectation[] expectations = mockWebServer
                .when(expectedRequest)
                .respond(request -> {
                    AtlasRelationship body = objectMapper.readValue(request.getBodyAsString(), AtlasRelationship.class);

                    String relationshipType = body.getTypeName();
                    String relationshipName;
                    switch (relationshipType) {
                        case META_VERSION_REL_TYPE_NAME:
                            relationshipName = "versions";
                            break;
                        case TOPIC_SCHEMA_REL_TYPE_NAME:
                            return jsonResponse(new AtlasRelationship());
                        default:
                            LOG.warn("Untested relationship type: {}", body.getTypeName());
                            return null;
                    }

                    checkNotNull(body, "No entities in the request.");
                    AtlasObjectId end1 = body.getEnd1();
                    AtlasObjectId end2 = body.getEnd2();

                    AtlasEntity oneEntity = null;
                    AtlasEntity manyEntity = null;
                    List<AtlasEntity> entities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITIES);
                    for (AtlasEntity entity : entities) {
                        if (end1.getGuid().equals(entity.getGuid())) {
                            oneEntity = entity;
                        } else if (end2.getGuid().equals(entity.getGuid())) {
                            manyEntity = entity;
                        }
                    }
                    if (oneEntity != null && manyEntity != null) {
                        Collection<AtlasEntity> manyEntities = (Collection<AtlasEntity>) oneEntity.getRelationshipAttribute(relationshipName);
                        if (manyEntities == null) {
                            manyEntities = new CopyOnWriteArrayList<>();
                            oneEntity.setRelationshipAttribute(relationshipName, manyEntities);
                        }
                        manyEntities.add(manyEntity);
                    } else {
                        fail("Did not find the entities on both end of the relationship: " + end1 + ", " + end2);
                    }

                    AtlasRelationship response = new AtlasRelationship(relationshipType, end1, end2);
                    response.getEnd1().setTypeName(oneEntity.getTypeName());
                    response.getEnd2().setTypeName(manyEntity.getTypeName());

                    List<AtlasRelationship> persistedRelationships;
                    if (sow.getValue(ATLAS_RELATIONSHIPS) != null) {
                        persistedRelationships = (List<AtlasRelationship>) sow.getValue(ATLAS_RELATIONSHIPS);
                    } else {
                        persistedRelationships = new CopyOnWriteArrayList<>();
                        sow.setValue(ATLAS_RELATIONSHIPS, persistedRelationships);
                    }
                    persistedRelationships.add(response);

                    return jsonResponse(response);
                });

        return expectations[0];
    }

    @SuppressWarnings("unchecked")
    private Expectation updateMeta() {
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        HttpRequest expectedRequest = request()
                .withMethod("POST")
                .withPath("/api/atlas/v2/entity/")
                .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                .withBody(json(serializeToJson(entitiesWithExtInfo), MatchType.ONLY_MATCHING_FIELDS));

        Expectation[] expectations = mockWebServer
                .when(expectedRequest)
                .respond(request -> {
                    AtlasEntity.AtlasEntityWithExtInfo body = objectMapper.readValue(request.getBodyAsString(), AtlasEntity.AtlasEntityWithExtInfo.class);
                    checkNotNull(body.getEntity(), "No entities in the request.");

                    AtlasEntity metaEntity = body.getEntity();
                    assertEquals(SCHEMA_META_TYPE_NAME, metaEntity.getTypeName());

                    String oldMetaGuid = metaEntity.getGuid();
                    metaEntity.setGuid(RandomStringUtils.randomAlphanumeric(8));
                    EntityMutationResponse response = new EntityMutationResponse();
                    response.setGuidAssignments(ImmutableMap.of(
                            oldMetaGuid, metaEntity.getGuid()
                    ));

                    List<AtlasEntity> persistedEntities;
                    if (sow.getValue(ATLAS_ENTITY_UPDATE) != null) {
                        persistedEntities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITY_UPDATE);
                    } else {
                        persistedEntities = new ArrayList<>();
                        sow.setValue(ATLAS_ENTITY_UPDATE, persistedEntities);
                    }
                    persistedEntities.add(metaEntity);

                    return jsonResponse(response);
                });

        return expectations[0];
    }

    @SuppressWarnings("unchecked")
    private Expectation queryByGuid() {
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();

        HttpRequest expectedRequest = request()
                .withMethod("GET")
                .withPath("/api/atlas/v2/entity/guid/{guid}")
                .withPathParameter("guid")
                .withContentType(MediaType.APPLICATION_JSON_UTF_8);

        Expectation[] expectations = mockWebServer
                .when(expectedRequest)
                .respond(request -> {
                    String guid = checkNotNull(request.getFirstPathParameter("guid"), "GUID was not provided in the request");

                    final List<AtlasEntity> persistedEntities;
                    if (sow.getValue(ATLAS_ENTITIES) != null) {
                        persistedEntities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITIES);
                    } else {
                        persistedEntities = new CopyOnWriteArrayList<>();
                        sow.setValue(ATLAS_ENTITIES, persistedEntities);
                    }

                    for (AtlasEntity ae : persistedEntities) {
                        if (guid.equals(ae.getGuid())) {
                            entityWithExtInfo.setEntity(ae);
                            break;
                        }
                    }

                    return jsonResponse(entityWithExtInfo);
                });

        return expectations[0];
    }

    public void dslSearch(AtlasSearchResult searchResult) {
        dslSearch(serializeToJson(searchResult));
    }

    public void dslSearch(String response) {
        mockWebServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/api/atlas/v2/search/dsl"))
                .respond(request -> response().withBody(response, MediaType.JSON_UTF_8));
    }

    private Expectation createAtlasModel() {
        HttpRequest expectedRequest = request()
                .withMethod("POST")
                .withPath("/api/atlas/v2/types/typedefs/")
                .withContentType(MediaType.APPLICATION_JSON_UTF_8);

        Expectation[] expectations = mockWebServer
                .when(expectedRequest)
                .respond(request -> {
                    checkNotNull(request.getBodyAsString(), "Empty request.");

                    return response().withBody(request.getBodyAsString(), MediaType.JSON_UTF_8);
                });

        return expectations[0];
    }

    public void queryAtlasModel() {
        mockWebServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/api/atlas/v2/types/typedefs/")
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                        .withQueryStringParameter("name", KAFKA_TOPIC_TYPEDEF_NAME)
                )
                .respond(request ->
                        jsonResponse(
                            new AtlasTypesDef(emptyList(), emptyList(), emptyList(), singletonList(new AtlasEntityDef(KAFKA_TOPIC_TYPEDEF_NAME)))
                        )
                );

        mockWebServer
                .when(request()
                        .withMethod("GET")
                        .withPath("/api/atlas/v2/types/relationshipdef/name/" + TOPIC_SCHEMA_REL_TYPE_NAME)
                        .withContentType(MediaType.APPLICATION_JSON_UTF_8)
                )
                .respond(request -> response().withBody("{}", MediaType.JSON_UTF_8));
    }

    @SuppressWarnings("unchecked")
    public boolean verifyCreateMetaRequest(String schemaName) {
        try {
            mockWebServer.verify(new ExpectationId().withId(createEntityExpectation.getId()));

            final List<AtlasEntity> persistedEntities;
            if (sow.getValue(ATLAS_ENTITIES) != null) {
                persistedEntities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITIES);
            } else {
                return false;
            }

            for (AtlasEntity ae : persistedEntities) {
                if (ae.getTypeName().equals(SCHEMA_META_TYPE_NAME) && schemaName.equals(ae.getAttribute("name"))) {
                    return true;
                }
            }

            return false;
        } catch (AssertionError aex) {
            LOG.error("Mock expectation failed.", aex);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean verifyCreateVersionRequest(String schemaName) {
        try {
            mockWebServer.verify(new ExpectationId().withId(createEntityExpectation.getId()));
            mockWebServer.verify(new ExpectationId().withId(createRelationshipExpectation.getId()));

            final List<AtlasEntity> persistedEntities;
            if (sow.getValue(ATLAS_ENTITIES) != null) {
                persistedEntities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITIES);
            } else {
                return false;
            }

            boolean entityFound = false;
            for (AtlasEntity ae : persistedEntities) {
                if (ae.getTypeName().equals(SCHEMA_VERSION_TYPE_NAME) && schemaName.equals(ae.getAttribute("name"))) {
                    entityFound = true;
                }
            }

            if (entityFound) {
                List<AtlasRelationship> rels = (List<AtlasRelationship>) sow.getValue(ATLAS_RELATIONSHIPS);
                if (CollectionUtils.isEmpty(rels)) {
                    return false;
                }

                for (AtlasRelationship rel : rels) {
                    if (META_VERSION_REL_TYPE_NAME.equals(rel.getTypeName()) && rel.getEnd2().getTypeName().equals(SCHEMA_VERSION_TYPE_NAME)) {
                        return true;
                    }
                }
            }

            return false;
        } catch (AssertionError aex) {
            LOG.error("Mock expectation failed.", aex);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public boolean verifyUpdateMetaRequest(String schemaName) {
        try {
            mockWebServer.verify(new ExpectationId().withId(updateMetaExpectation.getId()));

            List<AtlasEntity> entities = (List<AtlasEntity>) sow.getValue(ATLAS_ENTITY_UPDATE);
            if (CollectionUtils.isEmpty(entities)) {
                return false;
            }

            for (AtlasEntity ent : entities) {
                if (schemaName.equals(ent.getAttribute("name"))) {
                    return true;
                }
            }

            return false;
        } catch (AssertionError aex) {
            LOG.error("Mock expectation failed.", aex);
            return false;
        }
    }

    public boolean verifyCreateModelRequest() {
        try {
            mockWebServer.verify(new ExpectationId().withId(createModelExpectation.getId()));
            return true;
        } catch (AssertionError aex) {
            LOG.error("Mock expectation failed.", aex);
            return false;
        }
    }

    public void verifyNoConnectionWasMadeWith(String topicName) {
        mockWebServer.verify(
                request().withMethod("POST").withPath("/api/atlas/v2/relationship/"),
                exactly(0)
        );
    }

    public void verifySchemaAndTopicGotConnected(String schemaName, String topicName) {
        mockWebServer.verify(new ExpectationId().withId(createRelationshipExpectation.getId()));
    }
}
