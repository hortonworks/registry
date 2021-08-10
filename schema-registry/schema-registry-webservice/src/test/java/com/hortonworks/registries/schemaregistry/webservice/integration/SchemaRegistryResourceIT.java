/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.webservice.integration;

import com.cloudera.dim.atlas.events.AtlasEventLogger;
import com.hortonworks.registries.common.GenericExceptionMapper;
import com.hortonworks.registries.common.util.HadoopPlugin;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.core.util.AuthorizationUtils;
import com.hortonworks.registries.schemaregistry.authorizer.exception.RangerException;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryResource;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.BeanParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(DropwizardExtensionsSupport.class)
public class SchemaRegistryResourceIT {

    private static ISchemaRegistry schemaRegistryMock;
    private static AuthorizationAgent authorizationAgentMock;
    private static AuthorizationUtils authorizationUtils;
    private static SchemaMetadataTypeValidator schemaMetadataTypeValidatorMock;
    private static AtlasEventLogger atlasEventLogger;
    private static ResourceExtension RESOURCE = beforeAll();
    private Client testClient = RESOURCE.client();
    
    private static ResourceExtension beforeAll() {
        schemaRegistryMock = mock(ISchemaRegistry.class);
        authorizationAgentMock = mock(AuthorizationAgent.class);
        authorizationUtils = new AuthorizationUtils(mock(HadoopPlugin.class));
        schemaMetadataTypeValidatorMock = mock(SchemaMetadataTypeValidator.class);
        atlasEventLogger = mock(AtlasEventLogger.class);
        return ResourceExtension.builder()
                .addResource(new SchemaRegistryResource(schemaRegistryMock, authorizationAgentMock, authorizationUtils, null, schemaMetadataTypeValidatorMock, null, atlasEventLogger, null))
                .addProvider(GenericExceptionMapper.class)
                .addProperty("jersey.config.server.provider.classnames", MultiPartFeature.class.getName())
                .build();
    }
    
    @BeforeEach
    public void setup() {
        reset(atlasEventLogger);
    }
    
    @BeanParam
    public InputStream getInputStream() {
        return new ByteArrayInputStream("test".getBytes());
    }

    @Test
    public void findSchemasByFieldsNameNamespace() throws Exception {
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("apple", 1));
        SchemaFieldQuery query = new SchemaFieldQuery("apple", "pear", null);
        when(schemaRegistryMock.findSchemasByFields(query)).thenReturn(schemaversions);
        when(authorizationAgentMock.authorizeFindSchemasByFields(any(), any(), eq(schemaversions))).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas/fields"))
                .queryParam("name", "apple")
                .queryParam("fieldNamespace", "pear")
                .request()
                .get();

        //then
        SchemaFieldQuery expectedQuery = new SchemaFieldQuery("apple", "pear", null);
        verify(schemaRegistryMock).findSchemasByFields(expectedQuery);
        verify(authorizationAgentMock).authorizeFindSchemasByFields(null, schemaRegistryMock, schemaversions);
        TestResponseForSchemaVersionKey actual = response.readEntity(TestResponseForSchemaVersionKey.class);
        assertEquals(schemaversions, actual.getEntities());
        assertEquals(200, response.getStatus());
    }

    @Test
    public void findSchemasByFieldsEmpty() throws Exception {
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("test", 1));
        when(schemaRegistryMock.findSchemasByFields(any())).thenReturn(schemaversions);
        when(authorizationAgentMock.authorizeFindSchemasByFields(any(), any(), any())).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas/fields"))
                .request()
                .get();

        //then
        SchemaFieldQuery expectedQuery = new SchemaFieldQuery(null, null, null);
        verify(schemaRegistryMock).findSchemasByFields(expectedQuery);
        verify(authorizationAgentMock).authorizeFindSchemasByFields(null, schemaRegistryMock, schemaversions);
        TestResponseForSchemaVersionKey actual = response.readEntity(TestResponseForSchemaVersionKey.class);
        assertEquals(schemaversions, actual.getEntities());
        assertEquals(200, response.getStatus());
    }
    
    @Test
    public void findSchemasCanDelegate() throws Exception {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        when(authorizationAgentMock.authorizeFindSchemas(any(), eq(schemaversions))).thenReturn(schemaversions);
        when(schemaRegistryMock.searchSchemas(any(), any())).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .queryParam("name", "apple")
                .queryParam("_orderByFields", "timestamp,d")
                .request()
                .get();

        //then
        verify(authorizationAgentMock).authorizeFindSchemas(null, schemaversions);
        verify(schemaRegistryMock, atLeastOnce()).searchSchemas(any(MultivaluedMap.class), any(Optional.class));
        TestResponseForSchemaMetadataInfo actual = response.readEntity(TestResponseForSchemaMetadataInfo.class);
        assertEquals(schemaversions, actual.getEntities());
        assertEquals(200, response.getStatus());
    }

    @Test
    public void findSchemasWithoutNameAndDescResultingBadRequest() {
        //given
        
        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .request()
                .get();

        //then
        TestResponseForSchemaMetadataInfo actual = response.readEntity(TestResponseForSchemaMetadataInfo.class);
        assertEquals(200, response.getStatus());
        assertNotNull(actual);
        assertNotNull(actual.getEntities());
        assertEquals(0, actual.getEntities().size());
    }

    @Test
    public void findAggregatedSchemasCanDelegate() throws Exception {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfos = new ArrayList<>();
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = new AggregatedSchemaMetadataInfo(schemaMetadata, null, null, Collections.emptyList(), Collections.emptyList());
        aggregatedSchemaMetadataInfos.add(aggregatedSchemaMetadataInfo);
        MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();
        queryParameters.add("name", "magnesium");
        queryParameters.add("_orderByFields", "timestamp,d");
        when(authorizationAgentMock.authorizeGetAggregatedSchemaList(any(), any())).thenReturn(aggregatedSchemaMetadataInfos);
        when(schemaRegistryMock.searchSchemas(queryParameters, Optional.of("timestamp,d"))).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas/aggregated"))
                .queryParam("name", "magnesium")
                .queryParam("_orderByFields", "timestamp,d")
                .request()
                .get();

        //then
        verify(authorizationAgentMock).authorizeGetAggregatedSchemaList(null, aggregatedSchemaMetadataInfos);
        verify(schemaRegistryMock).searchSchemas(queryParameters, Optional.of("timestamp,d"));
        TestResponseForAggregatedSchemaMetadataInfo actual = response.readEntity(TestResponseForAggregatedSchemaMetadataInfo.class);
        assertEquals(200, response.getStatus());
        assertNotNull(actual);
    }
    

    @Test
    public void findAggregatedSchemaWithoutNameAndOrderResultingBadRequest() {
        //given

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .request()
                .get();

        //then
        TestResponseForSchemaMetadataInfo actual = response.readEntity(TestResponseForSchemaMetadataInfo.class);
        assertEquals(200, response.getStatus());
        assertNotNull(actual);
        assertNotNull(actual.getEntities());
        assertEquals(0, actual.getEntities().size());
    }

    @Test
    public void updateSchemaInfoBadType() {
        //given
        SchemaMetadata updatedSchemaMetadata = new SchemaMetadata.Builder("updated").description("SchemaMetadata with bad type").type("cat").build();
        when(schemaMetadataTypeValidatorMock.isValid("cat")).thenReturn(false);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/schemas/dummy"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(updatedSchemaMetadata), Response.class);

        //then
        verifyNoInteractions(atlasEventLogger);
        assertEquals(400, response.getStatus());

    }

    @Test
    public void addSerDesValidSerDesPair() {
        //given
        SerDesPair serDesPair = new SerDesPair("name", "desc", "fileid", "ser", "des");
        when(schemaRegistryMock.addSerDes(serDesPair)).thenReturn(66L);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(serDesPair), Response.class);

        //then
        verifyNoInteractions(atlasEventLogger);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void addSerDesInvalidSerDesPair() {
        //given
        SerDesPair serDesPair = new SerDesPair(null, null, null, null, null);
        when(schemaRegistryMock.addSerDes(serDesPair)).thenReturn(66L);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(serDesPair), Response.class);

        //then
        verifyNoInteractions(atlasEventLogger);
        TestResponseForSerDesInfo responseFor = response.readEntity(TestResponseForSerDesInfo.class);

        assertTrue(responseFor.getErrors().contains("name must not be null"));
        assertTrue(responseFor.getErrors().contains("deserializerClassName must not be null"));
        assertTrue(responseFor.getErrors().contains("serializerClassName must not be null"));
        assertTrue(responseFor.getErrors().contains("fileId must not be null"));
    }

    @Test
    public void getSerDesInfoExistingId() {
        //given
        SerDesPair serDesPair = new SerDesPair("name", "desc", "fileid", "ser", "des");
        SerDesInfo serDesInfo = new SerDesInfo(1L, System.currentTimeMillis(), serDesPair);
        when(schemaRegistryMock.getSerDes(1L)).thenReturn(serDesInfo);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes/1"))
                .request()
                .get();

        //then
        SerDesInfo actual = response.readEntity(SerDesInfo.class);
        assertEquals(Long.valueOf(1L), actual.getId());
    }

    @Test
    public void getSerDesInfoNotExistingId() {
        //given
        when(schemaRegistryMock.getSerDes(8L)).thenReturn(null);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes/8"))
                .request()
                .get();

        //then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void getSerDesInfoThrowException() {
        //given
        when(schemaRegistryMock.getSerDes(6L)).thenThrow(new RuntimeException("Test"));

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes/6"))
                .request()
                .get();

        //then
        assertEquals(500, response.getStatus());
    }

    @Test
    public void querySchemasThrowRangerException() throws Exception {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("foo").type("avro").schemaGroup("foundation").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        MultivaluedMap<String, String> queryParameters = new MultivaluedHashMap<>();
        queryParameters.add("name", "foo");
        queryParameters.add("_orderByFields", "timestamp,d");
        when(authorizationAgentMock.authorizeFindSchemas(any(), eq(schemaversions))).thenThrow(new RangerException("Ranger Exception"));
        when(schemaRegistryMock.findSchemaMetadata(any())).thenReturn(Collections.emptyList());
        when(schemaRegistryMock.searchSchemas(queryParameters, Optional.ofNullable("timestamp,d"))).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .queryParam("name", "foo")
                .queryParam("_orderByFields", "timestamp,d")
                .request()
                .get();

        //then
        assertEquals(502, response.getStatus());
    }

    @Test
    public void postSchemaThrowRangerException() throws Exception {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        doThrow(new RangerException("Ranger Exception")).when(authorizationAgentMock).authorizeSchemaMetadata(any(), any(SchemaMetadata.class), any());
        when(schemaRegistryMock.addSchemaMetadata(schemaMetadata, true)).thenReturn(1L);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/schemas"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(schemaMetadata), Response.class);

        //then
        assertEquals(502, response.getStatus());
        verifyNoInteractions(atlasEventLogger);
    }
    
    @Test
    public void throwUnsupportedSchemaTypeExceptionAddSchemaInfo() {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        doThrow(new UnsupportedSchemaTypeException("UnsupportedSchemaTypeException")).when(schemaRegistryMock).addSchemaMetadata(schemaMetadata, false);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/schemas"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(schemaMetadata), Response.class);

        //then
        assertEquals(400, response.getStatus());
    }

    @Test
    public void bulkImportSchema() {
        // given
        doNothing().when(authorizationAgentMock).authorizeBulkImport(any());

        MultiPart multiPart = new MultiPart();
        multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);

        StreamDataBodyPart fileDataBodyPart = new StreamDataBodyPart("file",
                getClass().getResourceAsStream("/exportimport/confluent1.txt"),
                "bulk.txt");
        multiPart.bodyPart(fileDataBodyPart);

        // when
        Response response = testClient.target("/api/v1/schemaregistry/import")
                .register(MultiPartFeature.class)
                .queryParam("format", "CONFLUENT")
                .queryParam("failOnError", "true")
                .request()
                .post(Entity.entity(multiPart, multiPart.getMediaType()), Response.class);

        // then
        assertEquals(200, response.getStatus());
        assertNotNull(response.getEntity());
        verifyNoInteractions(atlasEventLogger);
    }

}

abstract class TestResponse<T> {
    private List<T> entities;
    private List<String> errors;

    public List<T> getEntities() {
        return entities;
    }

    public void setEntities(List<T> entities) {
        this.entities = entities;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
}
class TestResponseForSerDesInfo extends TestResponse<SerDesInfo> { }

class TestResponseForSchemaVersionKey extends TestResponse<SchemaVersionKey> { }

class TestResponseForSchemaMetadataInfo extends TestResponse<SchemaMetadataInfo> { }

class TestResponseForAggregatedSchemaMetadataInfo extends TestResponse<AggregatedSchemaMetadataInfo> { }
