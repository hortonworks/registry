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
package com.hortonworks.registries.schemaregistry.webservice;

import com.hortonworks.registries.common.CollectionResponse;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.storage.search.WhereClause;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class SchemaRegistryResourceTest {
    private MultivaluedMap<String, String> queryParametersWithNameAndDesc;
    private MultivaluedMap<String, String> queryParametersWithoutDesc;
    private SchemaRegistryResource underTest;
    private AtomicReference atomicReferenceMock = Mockito.mock(AtomicReference.class);
    private ISchemaRegistry schemaRegistryMock = Mockito.mock(ISchemaRegistry.class);
    private AuthorizationAgent authorizationAgentMock = Mockito.mock(AuthorizationAgent.class);
    private static final String NAME = "name";
    private static final String DESCRIPTION = "description";
    private static final String ORDER = "_orderByFields";
    private static final String FIELD_NAMESPACE = "fieldNamespace";
    private static final String TYPE = "type";
    

    @Before
    public void setup() {
        queryParametersWithNameAndDesc = new MultivaluedHashMap<>();
        queryParametersWithoutDesc = new MultivaluedHashMap<>();
        queryParametersWithNameAndDesc.putSingle(NAME, "some name");
        queryParametersWithNameAndDesc.putSingle(DESCRIPTION, "some desc");
        queryParametersWithNameAndDesc.putSingle(ORDER,"foo,a,bar,d");
        queryParametersWithoutDesc.putSingle(NAME, "only name");
        queryParametersWithoutDesc.putSingle(ORDER,"foo,a,bar,d");
        underTest = new SchemaRegistryResource(schemaRegistryMock, atomicReferenceMock, null, authorizationAgentMock, null);
    }
    
    @Test
    public void getWhereClauseTest_NamePresent() {
        //given
        WhereClause expected = WhereClause.begin().contains(NAME, "only name").combine();
        
        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutDesc);
        
        //then
        assertThat(actual, is(expected));

    }
    
    @Test
    public void getWhereClauseTest_NameDescriptionPresent() {
        //given
        WhereClause expected = WhereClause.begin().contains(NAME, "some name").or().contains(DESCRIPTION, "some desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithNameAndDesc);

        //then
        assertThat(actual, is(expected));

    }
    
    @Test
    public void createFilterForTest_NameDescOrder(){
        //given
        String name = "some name";
        String desc = "interesting desc";
        String order = "timestamp,a";
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);
        expected.put(DESCRIPTION, desc);
        expected.put(ORDER, order);
        
        //when
        Map<String, String> actual = underTest.createFilterForSchema(Optional.ofNullable(name), Optional.ofNullable(desc), Optional.ofNullable(order), Optional.ofNullable(null), Optional.ofNullable(null), Optional.ofNullable(null), Optional.ofNullable(null), Optional.ofNullable(null), Optional.ofNullable(null));
        
        //then
        assertThat(actual, is(expected));
    }

    @Test
    public void createFilterForTest_AllQueries(){
        //given
        String name = "some name";
        String desc = "interesting desc";
        String order = "timestamp,a";
        String id = "425";
        String type = "avro";
        String schemaGroup = "kafka";
        String validationLevel = "ALL";
        String compatibility = "BACKWARD";
        String evolve = "1";
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);
        expected.put(DESCRIPTION, desc);
        expected.put(ORDER, order);
        expected.put("id", id);
        expected.put(TYPE, type);
        expected.put("schemaGroup", schemaGroup);
        expected.put("validationLevel", validationLevel);
        expected.put("compatibility", compatibility);
        expected.put("evolve", evolve);

        //when
        Map<String, String> actual = underTest.createFilterForSchema(Optional.ofNullable(name), Optional.ofNullable(desc), Optional.ofNullable(order), Optional.ofNullable(id), Optional.ofNullable(type), Optional.ofNullable(schemaGroup), Optional.ofNullable(validationLevel), Optional.ofNullable(compatibility), Optional.ofNullable(evolve));

        //then
        assertThat(actual, is(expected));
    }

    @Test
    public void createFilterForTest_NameDescType(){
        //given
        String name = "some name";
        String desc = "interesting desc";
        String order = null;
        String id = null;
        String type = "avro";
        String schemaGroup = null;
        String validationLevel = null;
        String compatibility = null;
        String evolve = null;
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);
        expected.put(DESCRIPTION, desc);
        expected.put(TYPE, type);

        //when
        Map<String, String> actual = underTest.createFilterForSchema(Optional.ofNullable(name), Optional.ofNullable(desc), Optional.ofNullable(order), Optional.ofNullable(id), Optional.ofNullable(type), Optional.ofNullable(schemaGroup), Optional.ofNullable(validationLevel), Optional.ofNullable(compatibility), Optional.ofNullable(evolve));

        //then
        assertThat(actual, is(expected));
    }

    @Test
    public void createFilterForNamespaceTest_NameGroupType(){
        //given
        String name = "unique name";
        String namespace = "namespace";
        String type = "JSON";
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);
        expected.put(FIELD_NAMESPACE, namespace);
        expected.put(TYPE, type);
        
        //when
        Map<String, String> actual = underTest.createFilterForNamespace(Optional.ofNullable(name), Optional.ofNullable(namespace), Optional.ofNullable(type));
        
        //then
        assertThat(actual, is(expected));

    }

    @Test
    public void createFilterForNamespaceTest_NameGroup(){
        //given
        String name = "unique name";
        String namespace = "namespace";
        String type = null;
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);
        expected.put(FIELD_NAMESPACE, namespace);

        //when
        Map<String, String> actual = underTest.createFilterForNamespace(Optional.ofNullable(name), Optional.ofNullable(namespace), Optional.ofNullable(type));

        //then
        assertThat(actual, is(expected));

    }

    @Test
    public void createFilterForNamespaceTest_NameType(){
        //given
        String name = "unique name";
        String namespace = null;
        String type = "JSON";
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);
        expected.put(TYPE, type);

        //when
        Map<String, String> actual = underTest.createFilterForNamespace(Optional.ofNullable(name), Optional.ofNullable(namespace), Optional.ofNullable(type));

        //then
        assertThat(actual, is(expected));

    }

    @Test
    public void createFilterForNamespaceTest_GroupType(){
        //given
        String name = null;
        String namespace = "namespace";
        String type = "JSON";
        Map<String, String> expected = new HashMap<>();
        expected.put(FIELD_NAMESPACE, namespace);
        expected.put(TYPE, type);

        //when
        Map<String, String> actual = underTest.createFilterForNamespace(Optional.ofNullable(name), Optional.ofNullable(namespace), Optional.ofNullable(type));

        //then
        assertThat(actual, is(expected));

    }

    @Test
    public void createFilterForNamespaceTest_Name(){
        //given
        String name = "unique name";
        String namespace = null;
        String type = null;
        Map<String, String> expected = new HashMap<>();
        expected.put(NAME, name);

        //when
        Map<String, String> actual = underTest.createFilterForNamespace(Optional.ofNullable(name), Optional.ofNullable(namespace), Optional.ofNullable(type));

        //then
        assertThat(actual, is(expected));

    }
    
    @Test
    public void findSchemaByFields_AllParametersProvided() throws Exception{
        //given
        Collection<SchemaVersionKey> schemaversions = new ArrayList<>();
        schemaversions.add(new SchemaVersionKey("test", 1));
        Mockito.when(authorizationAgentMock.authorizeFindSchemasByFields(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(schemaversions);
        String name = "name";
        String namespace = "namespace";
        String type = "type";
        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        SchemaFieldQuery schemaFieldQuery = new SchemaFieldQuery("test", "testnamespace", TYPE);
        underTest = new SchemaRegistryResource(schemaRegistryMock, atomicReferenceMock, null, authorizationAgentMock, null){
            @Override
            SchemaFieldQuery buildSchemaFieldQuery(MultivaluedMap<String, String> queryParameters) {
                MultivaluedMap<String, String> expectedParameters = new MultivaluedHashMap<>();
                expectedParameters.add(NAME, "name");
                expectedParameters.add(FIELD_NAMESPACE, "namespace");
                expectedParameters.add(TYPE, "type");
                assertThat(queryParameters, Is.is(expectedParameters));
                return schemaFieldQuery;
            }
        };
        Mockito.when(schemaRegistryMock.findSchemasByFields(ArgumentMatchers.any())).thenReturn(schemaversions);
        
        //when
        Response actual = underTest.findSchemasByFields(name, namespace, type, securityContext);
        
        //then
        Mockito.verify(schemaRegistryMock).findSchemasByFields(schemaFieldQuery);
        Mockito.verify(authorizationAgentMock).authorizeFindSchemasByFields(null, schemaRegistryMock, schemaversions);
        CollectionResponse expectedEntity = CollectionResponse.newResponse().entities(schemaversions).build();
        assertThat(actual.getStatus(), is(200));
        assertThat(((CollectionResponse)(actual.getEntity())).getEntities(), is(expectedEntity.getEntities()));

    }
    
    @Test
    public void buildSchemaFieldQueryTest_NameNamespaceType(){
        //given
        String name = "apple";
        String namespace = "pear";
        String type = "strawberry";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(NAME, name);
        queryparameters.add(FIELD_NAMESPACE, namespace);
        queryparameters.add(TYPE, type);
        SchemaFieldQuery expected = new SchemaFieldQuery(name, namespace, type);
        
        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);
        
        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }

    @Test
    public void buildSchemaFieldQueryTest_NameType(){
        //given
        String name = "apple";
        String namespace = "pear";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(NAME, name);
        queryparameters.add(FIELD_NAMESPACE, namespace);
        SchemaFieldQuery expected = new SchemaFieldQuery(name, namespace, null);

        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);

        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }

    @Test
    public void buildSchemaFieldQueryTest_NamespaceType(){
        //given
        String namespace = "pear";
        String type = "strawberry";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(FIELD_NAMESPACE, namespace);
        queryparameters.add(TYPE, type);
        SchemaFieldQuery expected = new SchemaFieldQuery(null, namespace, type);

        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);

        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }

    @Test
    public void buildSchemaFieldQueryTest_NameNamespace(){
        //given
        String name = "apple";
        String namespace = "pear";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(NAME, name);
        queryparameters.add(FIELD_NAMESPACE, namespace);
        SchemaFieldQuery expected = new SchemaFieldQuery(name, namespace, null);

        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);

        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }

    @Test
    public void buildSchemaFieldQueryTest_Type(){
        //given
        String type = "strawberry";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(TYPE, type);
        SchemaFieldQuery expected = new SchemaFieldQuery(null, null, type);

        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);

        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }

    @Test
    public void buildSchemaFieldQueryTest_Namespace(){
        //given
        String namespace = "pear";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(FIELD_NAMESPACE, namespace);
        SchemaFieldQuery expected = new SchemaFieldQuery(null, namespace, null);

        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);

        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }

    @Test
    public void buildSchemaFieldQueryTest_Name(){
        //given
        String name = "apple";
        MultivaluedMap<String, String> queryparameters = new MultivaluedHashMap<>();
        queryparameters.add(NAME, name);
        SchemaFieldQuery expected = new SchemaFieldQuery(name, null, null);

        //when
        SchemaFieldQuery actual = underTest.buildSchemaFieldQuery(queryparameters);

        //then
        assertThat(actual.toQueryMap(), is(expected.toQueryMap()));
    }
}