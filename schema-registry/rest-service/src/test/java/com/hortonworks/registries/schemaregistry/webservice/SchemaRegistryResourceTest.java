package com.hortonworks.registries.schemaregistry.webservice;

import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.storage.search.WhereClause;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


public class SchemaRegistryResourceTest {
    private MultivaluedMap<String, String> queryParametersWithNameAndDesc;
    private MultivaluedMap<String, String> queryParametersWithoutDesc;
    private SchemaRegistryResource underTest;
    

    @Before
    public void setup() {
        queryParametersWithNameAndDesc = new MultivaluedHashMap<>();
        queryParametersWithoutDesc = new MultivaluedHashMap<>();
        queryParametersWithNameAndDesc.putSingle("name", "some name");
        queryParametersWithNameAndDesc.putSingle("description", "some desc");
        queryParametersWithNameAndDesc.putSingle("_orderByFields","foo,a,bar,d");
        queryParametersWithoutDesc.putSingle("name", "only name");
        queryParametersWithoutDesc.putSingle("_orderByFields","foo,a,bar,d");
        AtomicReference atomicReferenceMock = Mockito.mock(AtomicReference.class);
        ISchemaRegistry schemaRegistryMock = Mockito.mock(ISchemaRegistry.class);
        underTest = new SchemaRegistryResource(schemaRegistryMock, atomicReferenceMock, null, null, null);
    }
    
    @Test
    public void getWhereClauseTest_NamePresent() {
        //given
        WhereClause expected = WhereClause.begin().contains("name", "only name").combine();
        
        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutDesc);
        
        //then
        assertThat(actual, is(expected));

    }
    
    @Test
    public void getWhereClauseTest_NameDescriptionPresent() {
        //given
        WhereClause expected = WhereClause.begin().contains("name", "some name").or().contains("description", "some desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithNameAndDesc);

        //then
        assertThat(actual, is(expected));

    }
}