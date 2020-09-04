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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
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
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class SchemaRegistryResourceIT {
    private static AtomicReference atomicReferenceMock = Mockito.mock(AtomicReference.class);
    private static ISchemaRegistry schemaRegistryMock = Mockito.mock(ISchemaRegistry.class);
    private static AuthorizationAgent authorizationAgentMock = Mockito.mock(AuthorizationAgent.class);
    private ObjectMapper om = new ObjectMapper();
    private Client testClient = resource.client();
    
    @BeanParam
    public InputStream getInputStream(){
        return new ByteArrayInputStream("test".getBytes());
    }
    @ClassRule
    public static final ResourceTestRule resource = ResourceTestRule.builder()
            .addResource(new SchemaRegistryResource(schemaRegistryMock, atomicReferenceMock, null, authorizationAgentMock, null))
            .addProperty("jersey.config.server.provider.classnames", "org.glassfish.jersey.media.multipart.MultiPartFeature")
            .build();
    @Test
    public void findSchemasByFields_NameNamespace() throws Exception{
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("apple", 1));
        Mockito.when(schemaRegistryMock.findSchemasByFields(ArgumentMatchers.any())).thenReturn(schemaversions);
        Mockito.when(authorizationAgentMock.authorizeFindSchemasByFields(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas/fields"))
                .queryParam("name", "apple")
                .queryParam("fieldNamespace", "pear")
                .request()
                .get();

        //then
        SchemaFieldQuery expectedQuery = new SchemaFieldQuery("apple", "pear",null);
        Mockito.verify(schemaRegistryMock).findSchemasByFields(expectedQuery);
        Mockito.verify(authorizationAgentMock).authorizeFindSchemasByFields(null, schemaRegistryMock, schemaversions);
        TestResponseForSchemaVersionKey actual = om.readValue(response.readEntity(String.class), TestResponseForSchemaVersionKey.class);
        assertThat(actual.getEntities(), Is.is(schemaversions));
        assertThat(response.getStatus(), Is.is(200));
    }

    @Test
    public void findSchemasByFields_Empty() throws Exception{
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("test", 1));
        Mockito.when(schemaRegistryMock.findSchemasByFields(ArgumentMatchers.any())).thenReturn(schemaversions);
        Mockito.when(authorizationAgentMock.authorizeFindSchemasByFields(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas/fields"))
                .request()
                .get();

        //then
        SchemaFieldQuery expectedQuery = new SchemaFieldQuery(null, null,null);
        Mockito.verify(schemaRegistryMock).findSchemasByFields(expectedQuery);
        Mockito.verify(authorizationAgentMock).authorizeFindSchemasByFields(null, schemaRegistryMock, schemaversions);
        TestResponseForSchemaVersionKey actual = om.readValue(response.readEntity(String.class), TestResponseForSchemaVersionKey.class);
        assertThat(actual.getEntities(), Is.is(schemaversions));
        assertThat(response.getStatus(), Is.is(200));
    }
    
    @Test
    public void findSchemas_CanDelegate() throws Exception{
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        Mockito.when(authorizationAgentMock.authorizeFindSchemas(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);
        Mockito.when(schemaRegistryMock.searchSchemas(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

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
        Mockito.verify(authorizationAgentMock).authorizeFindSchemas(null, schemaversions);
        Mockito.verify(schemaRegistryMock).searchSchemas(whereClause, order);
        TestResponseForSchemaMetadataInfo actual = om.readValue(response.readEntity(String.class), TestResponseForSchemaMetadataInfo.class);
        assertThat(actual.getEntities(), Is.is(schemaversions));
        assertThat(response.getStatus(), Is.is(200));
    }

    @Test
    public void findSchemasWithoutNameAndDesc_ResultingBadRequest() throws Exception{
        //given
        
        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .request()
                .get();

        //then
        TestResponseForSchemaMetadataInfo actual = om.readValue(response.readEntity(String.class), TestResponseForSchemaMetadataInfo.class);
        String[] errors = {"query param name may not be null", "query param _orderByFields may not be null"};
        assertThat(actual.getErrors(), hasItems(errors));
        assertThat(response.getStatus(), Is.is(400));
    }

    @Test
    public void findAggregatedSchemas_CanDelegate() throws Exception{
        //given
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("magnesium").type("avro").schemaGroup("eyebrow").compatibility(SchemaCompatibility.BACKWARD).validationLevel(SchemaValidationLevel.LATEST).description("b6").build();
        Collection<SchemaMetadataInfo> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaMetadataInfo(schemaMetadata));
        Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfos = new ArrayList<>();
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = new AggregatedSchemaMetadataInfo(schemaMetadata, null, null, Collections.emptyList(), Collections.emptyList());
        aggregatedSchemaMetadataInfos.add(aggregatedSchemaMetadataInfo);
        Mockito.when(authorizationAgentMock.authorizeGetAggregatedSchemaList(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(aggregatedSchemaMetadataInfos);
        Mockito.when(schemaRegistryMock.searchSchemas(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);

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
        Mockito.verify(authorizationAgentMock).authorizeGetAggregatedSchemaList(null, aggregatedSchemaMetadataInfos);
        Mockito.verify(schemaRegistryMock).searchSchemas(whereClause, order);
        TestResponseForAggregatedSchemaMetadataInfo actual = om.readValue(response.readEntity(String.class), TestResponseForAggregatedSchemaMetadataInfo.class);
        assertThat(response.getStatus(), Is.is(200));
    }
    

    @Test
    public void findAggregatedSchemaWithoutNameAndOrder_ResultingBadRequest() throws Exception{
        //given

        //when
        Response response = testClient.target(
                String.format("/api/v1/schemaregistry/search/schemas"))
                .request()
                .get();

        //then
        TestResponseForSchemaMetadataInfo actual = om.readValue(response.readEntity(String.class), TestResponseForSchemaMetadataInfo.class);
        String[] errors = {"query param name may not be null", "query param _orderByFields may not be null"};
        assertThat(actual.getErrors(), hasItems(errors));
        assertThat(response.getStatus(), Is.is(400));
    }
    
}
class TestResponseForSchemaVersionKey {
    private List<SchemaVersionKey> entities;
    public TestResponseForSchemaVersionKey(){}

    public List<SchemaVersionKey> getEntities() {
        return entities;
    }

    public void setEntities(List<SchemaVersionKey> entities) {
        this.entities = entities;
    }
}

class TestResponseForSchemaMetadataInfo {
    private final List<SchemaMetadataInfo> entities;
    private final List<String> errors;

    public List<SchemaMetadataInfo> getEntities() {
        return entities;
    }

    public List<String> getErrors() {
        return errors;
    }

    @JsonCreator
    public TestResponseForSchemaMetadataInfo(@JsonProperty("entities") List<SchemaMetadataInfo> entities,
                                             @JsonProperty("errors") List<String> errors) {
        this.entities = entities;
        this.errors = errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestResponseForSchemaMetadataInfo that = (TestResponseForSchemaMetadataInfo) o;
        return Objects.equals(entities, that.entities) &&
                Objects.equals(errors, that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entities, errors);
    }

    @Override
    public String toString() {
        return "TestResponseForSchemaMetadataInfo{" +
                "entities=" + entities +
                ", errors=" + errors +
                '}';
    }
}
    class TestResponseForAggregatedSchemaMetadataInfo {
        private final List<AggregatedSchemaMetadataInfo> entities;
        private final List<String> errors;

        public List<AggregatedSchemaMetadataInfo> getEntities() {
            return entities;
        }

        public List<String> getErrors() {
            return errors;
        }

        @JsonCreator
        public TestResponseForAggregatedSchemaMetadataInfo(@JsonProperty("entities") List<AggregatedSchemaMetadataInfo> entities,
                                                           @JsonProperty("errors") List<String> errors) {
            this.entities = entities;
            this.errors = errors;
        }

        @Override
        public String toString() {
            return "TestResponseForAggregatedSchemaMetadataInfo{" +
                    "entities=" + entities +
                    ", errors=" + errors +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestResponseForAggregatedSchemaMetadataInfo that = (TestResponseForAggregatedSchemaMetadataInfo) o;
            return Objects.equals(entities, that.entities) &&
                    Objects.equals(errors, that.errors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entities, errors);
        }
    }
