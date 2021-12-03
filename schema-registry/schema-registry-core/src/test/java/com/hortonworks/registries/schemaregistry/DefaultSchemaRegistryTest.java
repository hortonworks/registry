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

import com.google.common.collect.ImmutableList;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.json.JsonSchemaProvider;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import com.hortonworks.registries.storage.search.WhereClause;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultSchemaRegistryTest {

    private static final String NAME = "name";
    private static final String DESCRIPTION = "description";
    private static final String ORDER = "_orderByFields";

    private MultivaluedMap<String, String> queryParametersWithNameAndDesc;
    private MultivaluedMap<String, String> queryParametersWithoutDesc;
    private MultivaluedMap<String, String> queryParametersWithoutName;
    private DefaultSchemaRegistry underTest;
    StorageManager storageManagerMock;
    
    private void setup() {
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
                ImmutableList.of(
                        Collections.singletonMap("providerClass", AvroSchemaProvider.class.getName()),
                        Collections.singletonMap("providerClass", JsonSchemaProvider.class.getName()));
        ModuleDetailsConfiguration configuration = new ModuleDetailsConfiguration();
        underTest = new DefaultSchemaRegistry(configuration, storageManager, null, schemaProvidersConfig, new SchemaLockManager(new NOOPTransactionManager()));
    }
    
    private void setupForIdCheck() {
        storageManagerMock = mock(StorageManager.class);
        Collection<Map<String, Object>> schemaProvidersConfig =
            ImmutableList.of(
                Collections.singletonMap("providerClass", AvroSchemaProvider.class.getName()),
                Collections.singletonMap("providerClass", JsonSchemaProvider.class.getName()));
        ModuleDetailsConfiguration configuration = new ModuleDetailsConfiguration();
        underTest = new DefaultSchemaRegistry(configuration, storageManagerMock, null, schemaProvidersConfig, new SchemaLockManager(new NOOPTransactionManager()));
    }

    @Test
    public void getWhereClauseTestNamePresent() {
        //given
        setup();
        WhereClause expected = WhereClause.begin().contains(NAME, "only name").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutDesc);

        //then
        assertEquals(expected, actual);
    }

    @Test
    public void getWhereClauseTestNameDescriptionPresent() {
        //given
        setup();
        WhereClause expected = WhereClause.begin().contains(NAME, "some name").and().contains(DESCRIPTION, "some desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithNameAndDesc);

        //then
        assertEquals(expected, actual);
    }

    @Test
    public void getWhereClauseTestDescriptionPresent() {
        //given
        setup();
        WhereClause expected = WhereClause.begin().contains(DESCRIPTION, "only desc").combine();

        //when
        WhereClause actual = underTest.getWhereClause(queryParametersWithoutName);

        //then
        assertEquals(expected, actual);
    }

    @Test
    public void testJsonSchemaCompatibility() {
        setup();
        SchemaMetadata meta = new SchemaMetadata.Builder("hello-world")
                .type("json")
                .description("json schema")
                .evolve(true)
                .compatibility(SchemaCompatibility.BACKWARD)
                .validationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL)
                .schemaGroup("kafka")
                .build();
        Long id = underTest.addSchemaMetadata(meta);

        SchemaMetadataInfo persisted = underTest.getSchemaMetadataInfo(id);
        assertNotNull(persisted);
        assertEquals(SchemaCompatibility.NONE, persisted.getSchemaMetadata().getCompatibility());

        SchemaMetadata meta2 = new SchemaMetadata.Builder(meta.getName())
                .type("json")
                .description("second version")
                .evolve(true)
                .compatibility(SchemaCompatibility.BACKWARD)
                .validationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL)
                .schemaGroup("kafka")
                .build();

        SchemaMetadataInfo info = underTest.updateSchemaMetadata(meta.getName(), meta2);
        assertNotNull(info);

        SchemaMetadataInfo updated = underTest.getSchemaMetadataInfo(id);
        assertNotNull(updated);
        assertEquals(SchemaCompatibility.NONE, updated.getSchemaMetadata().getCompatibility());
        assertEquals("second version", updated.getSchemaMetadata().getDescription());
    }

    @Test
    public void schemaBranchNotExisting() {
        //given
        setupForIdCheck();
        Long id = 3L;
        when(storageManagerMock.find(eq(SchemaBranchStorable.NAME_SPACE), eq(getQueryParams(SchemaBranchStorable.ID, id)))).thenReturn(Collections.emptyList());

        //when
        Long actual = underTest.checkIfIdIsTaken(SchemaBranchStorable.class, id);

        //then
        assertEquals(3L, actual);
    }

    @Test
    public void schemaBranchExistingNextId() {
        //given
        setupForIdCheck();
        Long id = 3L;
        SchemaBranchStorable branchStorable = new SchemaBranchStorable(id);
        when(storageManagerMock.find(eq(SchemaBranchStorable.NAME_SPACE), eq(getQueryParams(SchemaBranchStorable.ID, id)))).thenReturn(Collections.singletonList(branchStorable));
        when(storageManagerMock.nextId(eq(SchemaBranchStorable.NAME_SPACE))).thenReturn(4L);

        //when
        Long actual = underTest.checkIfIdIsTaken(SchemaBranchStorable.class, id);

        //then
        verify(storageManagerMock, times(1)).nextId(eq(SchemaBranchStorable.NAME_SPACE));
        assertEquals(4L, actual);
    }

    @Test
    public void schemaBranchExistingNextNextId() {
        //given
        AtomicInteger callCounter = new AtomicInteger();
        setupForIdCheck();
        Long id = 3L;
        SchemaBranchStorable branchStorable3 = new SchemaBranchStorable(id);
        SchemaBranchStorable branchStorable4 = new SchemaBranchStorable(id + 1);
        when(storageManagerMock.find(eq(SchemaBranchStorable.NAME_SPACE), eq(getQueryParams(SchemaBranchStorable.ID, id)))).thenReturn(Collections.singletonList(branchStorable3));
        when(storageManagerMock.find(eq(SchemaBranchStorable.NAME_SPACE), eq(getQueryParams(SchemaBranchStorable.ID, id + 1)))).thenReturn(Collections.singletonList(branchStorable4));
        when(storageManagerMock.nextId(eq(SchemaBranchStorable.NAME_SPACE))).thenAnswer(ans -> {
            if (callCounter.get() == 0) {
                callCounter.getAndIncrement();
                return 4L;
            } else {
                return 5L;
            }
        });

        //when
        Long actual = underTest.checkIfIdIsTaken(SchemaBranchStorable.class, id);

        //then
        verify(storageManagerMock, times(2)).nextId(eq(SchemaBranchStorable.NAME_SPACE));
        assertEquals(5L, actual);
    }

    @Test
    public void schemaMetadataNotExisting() {
        //given
        setupForIdCheck();
        Long id = 4L;
        when(storageManagerMock.find(eq(SchemaMetadataStorable.NAME_SPACE), eq(getQueryParams(SchemaMetadataStorable.ID, id)))).thenReturn(Collections.emptyList());

        //when
        Long actual = underTest.checkIfIdIsTaken(SchemaMetadataStorable.class, id);

        //then
        assertEquals(4L, actual);
    }

    @Test
    public void schemaMetaExistingNextId() {
        //given
        setupForIdCheck();
        Long id = 3L;
        SchemaMetadataStorable metaStorable = new SchemaMetadataStorable();
        when(storageManagerMock.find(eq(SchemaMetadataStorable.NAME_SPACE), eq(getQueryParams(SchemaMetadataStorable.ID, id)))).thenReturn(Collections.singletonList(metaStorable));
        when(storageManagerMock.nextId(eq(SchemaMetadataStorable.NAME_SPACE))).thenReturn(4L);

        //when
        Long actual = underTest.checkIfIdIsTaken(SchemaMetadataStorable.class, id);

        //then
        verify(storageManagerMock, times(1)).nextId(eq(SchemaMetadataStorable.NAME_SPACE));
        assertEquals(4L, actual);
    }

    @Test
    public void schemaMetaExistingNextNextId() {
        //given
        AtomicInteger counter = new AtomicInteger();
        setupForIdCheck();
        Long id = 3L;
        SchemaMetadataStorable metadataStorable3 = new SchemaMetadataStorable();
        SchemaMetadataStorable metadataStorable4 = new SchemaMetadataStorable();
        when(storageManagerMock.find(eq(SchemaMetadataStorable.NAME_SPACE), eq(getQueryParams(SchemaMetadataStorable.ID, id)))).thenReturn(Collections.singletonList(metadataStorable3));
        when(storageManagerMock.find(eq(SchemaMetadataStorable.NAME_SPACE), eq(getQueryParams(SchemaMetadataStorable.ID, id + 1)))).thenReturn(Collections.singletonList(metadataStorable4));
        when(storageManagerMock.nextId(eq(SchemaMetadataStorable.NAME_SPACE))).thenAnswer(ans -> {
            if (counter.get() == 0) {
                counter.getAndIncrement();
                return 4L;
            } else {
                return 5L;
            }
        });

        //when
        Long actual = underTest.checkIfIdIsTaken(SchemaMetadataStorable.class, id);

        //then
        verify(storageManagerMock, times(2)).nextId(eq(SchemaMetadataStorable.NAME_SPACE));
        assertEquals(5L, actual);
    }

    @Test
    public void invalidClassCheck() {
        //given
        setupForIdCheck();
        Long id = 4L;

        //when
        Long actual = underTest.checkIfIdIsTaken(AtlasEventStorable.class, id);

        //then
        assertNull(actual);
    }
    
    @Test
    public void getAllSchemaBranchesNoMetadata() {
        //given
        setupForIdCheck();
        when(storageManagerMock.get(any())).thenReturn(null);
        String schemaName = "not-existing-meta";
        
        //when
        assertThrows(SchemaNotFoundException.class, () -> underTest.getSchemaBranches(schemaName));
    }

    @Test
    public void getAllSchemaBranchesExistingMetadata() throws Exception {
        //given
        setupForIdCheck();
        String schemaName = "existing-meta";
        String branchName1 = "first-branch";
        String branchName2 = "second-branch";
        SchemaMetadataStorable metadataStorable = createSchemaMetadataStorable(schemaName);
        QueryParam queryParam = new QueryParam(SchemaBranchStorable.SCHEMA_METADATA_NAME, schemaName);
        SchemaBranchStorable schemaBranchStorable1 = new SchemaBranchStorable(branchName1, schemaName, SchemaBranch.MASTER_BRANCH_DESC);
        SchemaBranchStorable schemaBranchStorable2 = new SchemaBranchStorable(branchName2, schemaName, SchemaBranch.MASTER_BRANCH_DESC);
        List<Storable> storableCollection = new ArrayList<>();
        storableCollection.add(schemaBranchStorable1);
        storableCollection.add(schemaBranchStorable2);
        List<SchemaBranch> expected = new ArrayList<>();
        expected.add(schemaBranchStorable1.toSchemaBranch());
        expected.add(schemaBranchStorable2.toSchemaBranch());
        when(storageManagerMock.find(eq(SchemaBranchStorable.NAME_SPACE), eq(Collections.singletonList(queryParam)))).thenReturn(storableCollection);
        when(storageManagerMock.get(any())).thenReturn(metadataStorable);

        //when
        Collection<SchemaBranch> actual = underTest.getSchemaBranches(schemaName);
        
        //then
        assertEquals(actual, expected);
    }

    private SchemaMetadataStorable createSchemaMetadataStorable(String schemaName) {
        SchemaMetadataStorable storable = new SchemaMetadataStorable();
        storable.setId(1L);
        storable.setType("avro");
        storable.setSchemaGroup("kafka");
        storable.setName(schemaName);
        storable.setDescription("desc");
        storable.setValidationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL);
        storable.setCompatibility(SchemaCompatibility.DEFAULT_COMPATIBILITY);
        storable.setEvolve(true);
        return storable;
    }


    private List<QueryParam> getQueryParams(String key, Long value) {
        return Collections.singletonList(new QueryParam(key, String.valueOf(value)));
    }
}
