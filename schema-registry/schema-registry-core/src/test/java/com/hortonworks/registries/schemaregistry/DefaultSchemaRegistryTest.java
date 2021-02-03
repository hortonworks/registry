/**
 * Copyright 2017-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    public void getWhereClauseTestDescriptionPresent() {
        //given
        WhereClause expected = WhereClause.begin().contains(DESCRIPTION, "only desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutName);

        //then
        assertThat(actual, is(expected));
    }
}
