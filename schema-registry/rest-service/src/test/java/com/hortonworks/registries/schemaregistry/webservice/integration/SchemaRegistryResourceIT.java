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
package com.hortonworks.registries.schemaregistry.webservice.integration;

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
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryResource;
import com.hortonworks.registries.storage.search.OrderBy;
import com.hortonworks.registries.storage.search.WhereClause;
import io.dropwizard.testing.junit.ResourceTestRule;
import org.hamcrest.core.Is;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.ws.rs.BeanParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaRegistryResourceIT {
    private static AtomicReference atomicReferenceMock = Mockito.mock(AtomicReference.class);
    private static ISchemaRegistry schemaRegistryMock = Mockito.mock(ISchemaRegistry.class);
    private static AuthorizationAgent authorizationAgentMock = Mockito.mock(AuthorizationAgent.class);
    private static SchemaMetadataTypeValidator schemaMetadataTypeValidatorMock = Mockito.mock(SchemaMetadataTypeValidator.class);
    private Client testClient = resource.client();

    @BeanParam
    public InputStream getInputStream() {
        return new ByteArrayInputStream("test".getBytes());
    }

    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder()
            .addResource(instantiateResource())
            .addProperty("jersey.config.server.provider.classnames", "org.glassfish.jersey.media.multipart.MultiPartFeature")
            .build();

    private static SchemaRegistryResource instantiateResource() {
        return new SchemaRegistryResource(schemaRegistryMock, atomicReferenceMock, null, authorizationAgentMock, null, schemaMetadataTypeValidatorMock) {
            @Override
            protected Response handleLeaderAction(UriInfo uriInfo, Supplier<Response> supplier) {
                return supplier.get();
            }
        };
    }

    @Test
    public void findSchemasByFields_NameNamespace() throws Exception {
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("apple", 1));
        when(schemaRegistryMock.findSchemasByFields(ArgumentMatchers.any())).thenReturn(schemaversions);
        when(authorizationAgentMock.authorizeFindSchemasByFields(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

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
        assertThat(actual.getEntities(), is(schemaversions));
        assertThat(response.getStatus(), is(200));
    }

    @Test
    public void findSchemasByFields_Empty() throws Exception {
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("test", 1));
        when(schemaRegistryMock.findSchemasByFields(ArgumentMatchers.any())).thenReturn(schemaversions);
        when(authorizationAgentMock.authorizeFindSchemasByFields(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

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
        assertThat(actual.getEntities(), is(schemaversions));
        assertThat(response.getStatus(), is(200));
    }
    
    @Test
    public void findSchemas_CanDelegate() throws Exception {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        when(authorizationAgentMock.authorizeFindSchemas(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);
        when(schemaRegistryMock.searchSchemas(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .queryParam("name", "apple")
                .queryParam("_orderByFields", "timestamp,d")
                .request()
                .get();

        //then
        WhereClause whereClause = WhereClause.begin().contains("name", "apple").combine();
        List<OrderBy> order = new ArrayList<>();
        order.add(OrderBy.desc("timestamp"));
        verify(authorizationAgentMock).authorizeFindSchemas(null, schemaversions);
        verify(schemaRegistryMock).searchSchemas(whereClause, order);
        TestResponseForSchemaMetadataInfo actual = response.readEntity(TestResponseForSchemaMetadataInfo.class);
        assertThat(actual.getEntities(), is(schemaversions));
        assertThat(response.getStatus(), is(200));
    }

    @Test
    public void findSchemasWithoutNameAndDesc_ResultingBadRequest() {
        //given
        
        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .request()
                .get();

        //then
        TestResponseForSchemaMetadataInfo actual = response.readEntity(TestResponseForSchemaMetadataInfo.class);
        String[] errors = {"query param name may not be null", "query param _orderByFields may not be null"};
        assertThat(actual.getErrors(), hasItems(errors));
        assertThat(response.getStatus(), is(400));
    }

    @Test
    public void findAggregatedSchemas_CanDelegate() throws Exception {
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfos = new ArrayList<>();
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = new AggregatedSchemaMetadataInfo(schemaMetadata, null, null, Collections.emptyList(), Collections.emptyList());
        aggregatedSchemaMetadataInfos.add(aggregatedSchemaMetadataInfo);
        when(authorizationAgentMock.authorizeGetAggregatedSchemaList(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(aggregatedSchemaMetadataInfos);
        when(schemaRegistryMock.searchSchemas(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas/aggregated"))
                .queryParam("name", "magnesium")
                .queryParam("_orderByFields", "timestamp,d")
                .request()
                .get();

        //then
        WhereClause whereClause = WhereClause.begin().contains("name", "magnesium").combine();
        List<OrderBy> order = new ArrayList<>();
        order.add(OrderBy.desc("timestamp"));
        verify(authorizationAgentMock).authorizeGetAggregatedSchemaList(null, aggregatedSchemaMetadataInfos);
        verify(schemaRegistryMock).searchSchemas(whereClause, order);
        TestResponseForAggregatedSchemaMetadataInfo actual = response.readEntity(TestResponseForAggregatedSchemaMetadataInfo.class);
        assertThat(response.getStatus(), is(200));
    }
    

    @Test
    public void findAggregatedSchemaWithoutNameAndOrder_ResultingBadRequest() {
        //given

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .request()
                .get();

        //then
        TestResponseForSchemaMetadataInfo actual = response.readEntity(TestResponseForSchemaMetadataInfo.class);
        String[] errors = {"query param name may not be null", "query param _orderByFields may not be null"};
        assertThat(actual.getErrors(), hasItems(errors));
        assertThat(response.getStatus(), is(400));
    }

    @Test
    public void updateSchemaInfo_BadType() {
        //given
        SchemaMetadata updatedSchemaMetadata = new SchemaMetadata.Builder("updated").description("SchemaMetadata with bad type").type("cat").build();
        when(schemaMetadataTypeValidatorMock.isValid("cat")).thenReturn(false);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/schemas/dummy"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(updatedSchemaMetadata), Response.class);

        //then
        assertThat(response.getStatus(), is(400));

    }

    @Test
    public void addSerDes_ValidSerDesPair() {
        //given
        SerDesPair serDesPair = new SerDesPair("name", "desc", "fileid", "ser", "des");
        when(schemaRegistryMock.addSerDes(serDesPair)).thenReturn(66L);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(serDesPair), Response.class);

        //then
        assertThat(response.getStatus(), is(200));
    }

    @Test
    public void addSerDes_InvalidSerDesPair() {
        //given
        SerDesPair serDesPair = new SerDesPair(null, null, null, null, null);
        when(schemaRegistryMock.addSerDes(serDesPair)).thenReturn(66L);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes"))
                .request(MediaType.APPLICATION_JSON)
                .post(Entity.json(serDesPair), Response.class);

        //then
        TestResponseForSerDesInfo responseFor = response.readEntity(TestResponseForSerDesInfo.class);
        String[] errors = {"name may not be null", "deserializerClassName may not be null", "serializerClassName may not be null", "fileId may not be null"};
        assertThat(responseFor.getErrors(), hasItems(errors));
    }

    @Test
    public void getSerDesInfo_ExistingId() {
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
        assertThat(actual.getId(), is(1L));
    }

    @Test
    public void getSerDesInfo_NotExistingId() {
        //given
        when(schemaRegistryMock.getSerDes(6L)).thenReturn(null);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes/6"))
                .request()
                .get();

        //then
        assertThat(response.getStatus(), is(404));
    }

    @Test
    public void getSerDesInfo_ThrowException() {
        //given
        when(schemaRegistryMock.getSerDes(6L)).thenThrow(new RuntimeException("Test"));

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/serdes/6"))
                .request()
                .get();

        //then
        assertThat(response.getStatus(), is(500));
    }

}

class TestResponseForSerDesInfo {
    private List<SerDesInfo> entities;
    private List<String> errors;

    public TestResponseForSerDesInfo() {
    }

    public List<SerDesInfo> getEntities() {
        return entities;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setEntities(List<SerDesInfo> entities) {
        this.entities = entities;
    }
}

class TestResponseForSchemaVersionKey {
    private List<SchemaVersionKey> entities;

    public TestResponseForSchemaVersionKey() {
    }

    public List<SchemaVersionKey> getEntities() {
        return entities;
    }

    public void setEntities(List<SchemaVersionKey> entities) {
        this.entities = entities;
    }
}

class TestResponseForSchemaMetadataInfo {
    private List<SchemaMetadataInfo> entities;
    private List<String> errors;

    public List<SchemaMetadataInfo> getEntities() {
        return entities;
    }

    public List<String> getErrors() {
        return errors;
    }
}

class TestResponseForAggregatedSchemaMetadataInfo {
    private List<AggregatedSchemaMetadataInfo> entities;
    private List<String> errors;

    public List<AggregatedSchemaMetadataInfo> getEntities() {
        return entities;
    }

    public List<String> getErrors() {
        return errors;
    }
}
