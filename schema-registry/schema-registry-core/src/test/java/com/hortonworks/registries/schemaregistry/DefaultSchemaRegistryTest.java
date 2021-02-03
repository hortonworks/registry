package com.hortonworks.registries.schemaregistry;

import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import com.hortonworks.registries.storage.search.WhereClause;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DefaultSchemaRegistryTest {

    private static final String NAME = "name";
    private static final String DESCRIPTION = "description";
    private static final String ORDER = "_orderByFields";

    private MultivaluedMap<String, String> queryParametersWithNameAndDesc;
    private MultivaluedMap<String, String> queryParametersWithoutDesc;
    private MultivaluedMap<String, String> queryParametersWithoutName;
    private DefaultSchemaRegistry underTest;

    @Before
    public void setup() {
        queryParametersWithNameAndDesc = new MultivaluedHashMap<>();
        queryParametersWithoutDesc = new MultivaluedHashMap<>();
        queryParametersWithoutName = new MultivaluedHashMap<>();

        queryParametersWithNameAndDesc.putSingle(NAME, "some name");
        queryParametersWithNameAndDesc.putSingle(DESCRIPTION, "some desc");
        queryParametersWithNameAndDesc.putSingle(ORDER, "foo,a,bar,d");

        queryParametersWithoutDesc.putSingle(NAME, "only name");
        queryParametersWithoutDesc.putSingle(ORDER, "foo,a,bar,d");

        queryParametersWithoutName.putSingle(DESCRIPTION, "only desc");
        queryParametersWithoutDesc.putSingle(ORDER, "foo,a,bar,d");

        StorageManager storageManager = new InMemoryStorageManager();
        Collection<Map<String, Object>> schemaProvidersConfig =
                Collections.singleton(Collections.singletonMap("providerClass", AvroSchemaProvider.class.getName()));
        underTest = new DefaultSchemaRegistry(storageManager, null, schemaProvidersConfig, new SchemaLockManager(new NOOPTransactionManager()));
    }

    @Test
    public void getWhereClauseTestNamePresent() {
        //given
        WhereClause expected = WhereClause.begin().contains(NAME, "only name").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutDesc);

        //then
        assertThat(actual, is(expected));
    }

    @Test
    public void getWhereClauseTestNameDescriptionPresent() {
        //given
        WhereClause expected = WhereClause.begin().contains(NAME, "some name").and().contains(DESCRIPTION, "some desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithNameAndDesc);

        //then
        assertThat(actual, is(expected));
    }

    @Test
    public void getWhereClauseTest_DescriptionPresent() {
        //given
        WhereClause expected = WhereClause.begin().contains(DESCRIPTION, "only desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutName);

        //then
        assertThat(actual, is(expected));
    }
}
