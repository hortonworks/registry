/*
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
package com.hortonworks.registries.schemaregistry.exportimport;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkUploadServiceTest {

    private ISchemaRegistry schemaRegistry;
    private BulkUploadService bulkUploadService;
    private static final SchemaIdVersion SCHEMA_ID_VERSION_1 = new SchemaIdVersion(1L);
    private static final SchemaIdVersion SCHEMA_ID_VERSION_2 = new SchemaIdVersion(2L);
    private static final SchemaIdVersion SCHEMA_ID_VERSION_3 = new SchemaIdVersion(3L);
    private static final SchemaIdVersion SCHEMA_ID_VERSION_4 = new SchemaIdVersion(4L);

    @BeforeEach
    public void setUp() {
        schemaRegistry = mock(ISchemaRegistry.class);
        bulkUploadService = new BulkUploadService(schemaRegistry);
    }

    @Test
    public void testValidateSchemaMetadata() {
        final String existingSchema = "schema1";
        final Long existingSchemaId = 1L;
        final String notFoundSchema = "schema2";
        final Long notFoundSchemaId = 2L;

        when(schemaRegistry.getSchemaMetadataInfo(existingSchema)).thenReturn(createMetadata(existingSchemaId, existingSchema));

        // if a schema exists in the database, then it's considered not valid - because we can't insert a new one

        boolean valid1 = bulkUploadService.validateSchemaMetadata(createMetadata(existingSchemaId, existingSchema));
        boolean valid2 = bulkUploadService.validateSchemaMetadata(createMetadata(notFoundSchemaId, notFoundSchema));

        assertFalse(valid1, "Expected schema to be valid.");
        assertTrue(valid2, "Expected schema to be not valid.");
    }

    @Test
    public void testValidateSchemaVersion() {
        assertTrue(bulkUploadService.validateSchemaVersion(createVersion(1L, "schema1", 1, "{ }")));
        assertFalse(bulkUploadService.validateSchemaVersion(createVersion(2L, "schema2", 1, "")));
    }

    @Test
    public void testUploadValidSchemas() throws Exception {
        Map<Long, SchemaMetadata> mockDatabase = new HashMap<>();
        SchemaMetadataInfo car = createMetadata(1L, "Car");
        SchemaMetadataInfo fruit = createMetadata(2L, "Fruit");
        Map<String, SchemaMetadataInfo> nameToMeta = ImmutableMap.of(
                car.getSchemaMetadata().getName(), car,
                fruit.getSchemaMetadata().getName(), fruit
        );
        mockDatabase.put(car.getId(), car.getSchemaMetadata());
        mockDatabase.put(fruit.getId(), fruit.getSchemaMetadata());

        SchemaVersionInfo carV1 = createVersion(1L, "Car", 1, "avro1");
        SchemaVersionInfo carV2 = createVersion(2L, "Car", 2, "avro2");
        SchemaVersionInfo carV3 = createVersion(4L, "Car", 3, "avro3");
        SchemaVersionInfo fruitVersion = createVersion(3L, "Fruit", 1, "avro4");

        final Map<String, Integer> versionCount = new HashMap<>();
        when(schemaRegistry.addSchemaMetadata(any(SchemaMetadata.class), anyBoolean())).thenAnswer((Answer<Long>) invocation -> {
            SchemaMetadata meta = invocation.getArgument(0, SchemaMetadata.class);
            return nameToMeta.get(meta.getName()).getId();
        });
        when(schemaRegistry.getSchemaMetadataInfo(any(Long.class))).thenAnswer((Answer<SchemaMetadataInfo>) invocation -> {
            Long id = invocation.getArgument(0, Long.class);
            return new SchemaMetadataInfo(mockDatabase.get(id), id, System.currentTimeMillis());
        });
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(SchemaMetadata.class), any(Long.class), any(SchemaVersionInfo.class)))
                .thenAnswer((Answer<SchemaIdVersion>) invocation -> {
                    SchemaMetadata schemaMetadata = invocation.getArgument(1, SchemaMetadata.class);
                    int count = versionCount.getOrDefault(schemaMetadata.getName(), 0);
                    versionCount.put(schemaMetadata.getName(), count + 1);
                    return new SchemaIdVersion(invocation.getArgument(2, Long.class), count + 1);
                });

        // test
        Multimap<SchemaMetadataInfo, SchemaVersionInfo> schemasToUpload = ArrayListMultimap.create();
        schemasToUpload.put(car, carV1);
        schemasToUpload.put(car, carV2);
        schemasToUpload.put(car, carV3);
        schemasToUpload.put(fruit, fruitVersion);
        List<Long> failedIds = Lists.newArrayList(77L);

        UploadResult result = bulkUploadService.uploadValidSchemas(schemasToUpload, failedIds);

        // validate - 2 schemas and 4 versions
        verify(schemaRegistry, times(2)).addSchemaMetadata(any(SchemaMetadata.class), eq(false));
        verify(schemaRegistry, times(2)).getSchemaMetadataInfo(any(Long.class));
        verify(schemaRegistry, times(4)).addSchemaVersionWithBranchName(anyString(), any(SchemaMetadata.class), any(Long.class), any(SchemaVersionInfo.class));

        assertNotNull(result);
        assertEquals(1, result.getFailedCount());  // 77L
        assertEquals(4, result.getSuccessCount());
        assertTrue(result.getFailedIds().contains(77L));
    }
    
    @Test
    public void testUploadValidClouderaSchemaOneMetadataMasterBranchTwoVersions() throws Exception {
        //given
        InputStream is = getClass().getResourceAsStream("/exportimport/simplecloudera.json");
        SchemaBranch masterBranch = createBranch(1L, "MASTER", "rain", "'MASTER' branch for schema metadata 'rain'");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch);
        AtomicInteger callCount1L = new AtomicInteger();
        when(schemaRegistry.getSchemaMetadataInfo(1L)).thenAnswer(ans -> {
            if (callCount1L.get() == 0) {
                callCount1L.getAndIncrement();
                return null;
            } else {
                callCount1L.getAndIncrement();
                return createMetadata(1L, "Rain");
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(any(SchemaIdVersion.class))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.addSchemaMetadata(any(), any())).thenReturn(1L);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(1L), any())).thenReturn(SCHEMA_ID_VERSION_1);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(2L), any())).thenReturn(SCHEMA_ID_VERSION_2);
        UploadResult expected = new UploadResult(2, 0, Collections.emptyList());
        
        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);
        
        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());
    }

    @Test
    public void testUploadValidClouderaSchemaTwoMetadataTwoMasterBranchTwoVersions() throws Exception {
        //given
        InputStream is = getClass().getResourceAsStream("/exportimport/twometas.json");
        SchemaBranch masterBranch1 = createBranch(1L, "MASTER", "rain", "'MASTER' branch for schema metadata 'rain'");
        SchemaBranch masterBranch2 = createBranch(2L, "MASTER", "Food", "'MASTER' branch for schema metadata 'Food'");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch1);
        when(schemaRegistry.getSchemaBranch(eq(2L))).thenReturn(masterBranch2);
        AtomicInteger callCount1L = new AtomicInteger();
        AtomicInteger callCount2L = new AtomicInteger();
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenAnswer(ans -> {
            if (callCount1L.get() == 0) {
                callCount1L.getAndIncrement();
                return null;
            }  else {
                callCount1L.getAndIncrement();
                return createMetadata(1L, "Rain");
            }
        });
        when(schemaRegistry.getSchemaMetadataInfo(eq(2L))).thenAnswer(ans -> {
            if (callCount2L.get() == 0) {
                callCount2L.getAndIncrement();
                return null;
            } else {
                callCount2L.getAndIncrement();
                return createMetadata(2L, "Food");
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(any(SchemaIdVersion.class))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.addSchemaMetadata(eq(1L), any())).thenReturn(1L);
        when(schemaRegistry.addSchemaMetadata(eq(2L), any())).thenReturn(2L);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(1L), any())).thenReturn(SCHEMA_ID_VERSION_1);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(2L), any())).thenReturn(SCHEMA_ID_VERSION_2);
        UploadResult expected = new UploadResult(4, 0, Collections.emptyList());

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());

    }

    @Test
    public void testUploadValidClouderaSchemaOneMetadataOneMasterOneAnotherBranchTwoVersions() throws Exception {
        //given
        String schemaText = "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Tea\",\n     \"fields\": " +
        "[\n       { \"name\": \"flavour\", \"type\": \"string\" },\n       { \"name\": \"color\", \"type\": \"string\" },\n       { \"name\": \"sweeteners\", \"type\": \"boolean\", \"default\": false }\n     ]\n} \n";
        InputStream is = getClass().getResourceAsStream("/exportimport/twobranch.json");
        SchemaBranch masterBranch = createBranch(1L, "MASTER", "Tea", "'MASTER' branch for schema metadata 'Tea'");
        SchemaBranch anotherBranch = createBranch(2L, "rooibos", "Tea", "tea with bad taste");
        AtomicInteger callCount1L = new AtomicInteger();
        AtomicInteger callCount2L = new AtomicInteger();
        AtomicInteger callCount2LBranch = new AtomicInteger();
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch);
        when(schemaRegistry.createSchemaBranch(eq(2L), eq(anotherBranch))).thenReturn(anotherBranch);
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenAnswer(ans -> {
            if (callCount1L.get() == 0) {
                callCount1L.getAndIncrement();
                return null;
            } else {
                callCount1L.getAndIncrement();
                return createMetadata(1L, "Rain");
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_2))).thenAnswer(ans -> {
            if (callCount2L.get() == 0) {
                callCount2L.getAndIncrement();
                throw new SchemaNotFoundException("schema not found");
            } else {
                callCount2L.getAndIncrement();
                return createVersion(2L, "Tea", 2, schemaText);
            }
        });
        when(schemaRegistry.getSchemaBranch(eq(2L))).thenAnswer(ans -> {
            if (callCount2LBranch.get() == 0) {
                callCount2LBranch.getAndIncrement();
                throw new SchemaBranchNotFoundException("schema branch not found");
            } else {
                callCount2LBranch.getAndIncrement();
                return anotherBranch;
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_1))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_3))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.addSchemaMetadata(eq(1L), any())).thenReturn(1L);
        when(schemaRegistry.addSchemaMetadata(eq(2L), any())).thenReturn(2L);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(1L), any())).thenReturn(SCHEMA_ID_VERSION_1);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(2L), any())).thenReturn(SCHEMA_ID_VERSION_2);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(3L), any())).thenReturn(SCHEMA_ID_VERSION_3);
        UploadResult expected = new UploadResult(3, 0, Collections.emptyList());

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());

    }

    @Test
    public void testUploadValidClouderaSchemaTwoMetadataOneMasterOneAnotherBranchTwoVersions() throws Exception {
        //given
        String schemaText = "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Rain\",\n     \"fields\": [\n       " +
            "{ \"name\": \"day\", \"type\": \"string\" },\n       { \"name\": \"temperature\", \"type\": \"int\" }\n     ]\n}";
        InputStream is = getClass().getResourceAsStream("/exportimport/thwobranches.json");
        SchemaBranch masterBranch1 = createBranch(1L, "MASTER", "rain", "'MASTER' branch for schema metadata 'rain'");
        SchemaBranch masterBranch2 = createBranch(2L, "MASTER", "Food", "'MASTER' branch for schema metadata 'Food'");
        SchemaBranch anotherBranch = createBranch(3L, "another", "rain", "another branch");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch1);
        when(schemaRegistry.getSchemaBranch(eq(2L))).thenReturn(masterBranch2);
        when(schemaRegistry.createSchemaBranch(eq(1L), eq(anotherBranch))).thenReturn(anotherBranch);
        AtomicInteger callCount1L = new AtomicInteger();
        AtomicInteger callCount1LVersion = new AtomicInteger();
        AtomicInteger callCount2L = new AtomicInteger();
        AtomicInteger callCount4L = new AtomicInteger();
        AtomicInteger callCount3L = new AtomicInteger();
        AtomicInteger callCount2LVersion = new AtomicInteger();
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenAnswer(ans -> {
            if (callCount1L.get() == 0) {
                callCount1L.getAndIncrement();
                return null;
            } else {
                callCount1L.getAndIncrement();
                return createMetadata(1L, "Rain");
            }
        });
        when(schemaRegistry.getSchemaMetadataInfo(eq(2L))).thenAnswer(ans -> {
            if (callCount2L.get() == 0) {
                callCount2L.getAndIncrement();
                return null;
            } else {
                callCount2L.getAndIncrement();
                return createMetadata(1L, "Rain");
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_1))).thenAnswer(ans -> {
            if (callCount1LVersion.get() == 0) {
                callCount1LVersion.getAndIncrement();
                throw new SchemaNotFoundException("schema not found");
            } else {
                callCount1LVersion.getAndIncrement();
                return createVersion(1L, "Tea", 2, schemaText);
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_4))).thenAnswer(ans -> {
            if (callCount4L.get() == 0) {
                callCount4L.getAndIncrement();
                throw new SchemaNotFoundException("schema not found");
            } else {
                callCount4L.getAndIncrement();
                return createVersion(4L, "Tea", 2, "text");
            }
        });
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_2))).thenAnswer(ans -> {
            if (callCount2LVersion.get() == 0) {
                callCount2LVersion.getAndIncrement();
                throw new SchemaNotFoundException("schema not found");
            } else {
                callCount2LVersion.getAndIncrement();
                return createVersion(2L, "Tea", 2, "text");
            }
        });
        when(schemaRegistry.getSchemaBranch(3L)).thenThrow(SchemaBranchNotFoundException.class);
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_3))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.addSchemaMetadata(eq(1L), any())).thenReturn(1L);
        when(schemaRegistry.addSchemaMetadata(eq(2L), any())).thenReturn(2L);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(1L), any())).thenReturn(SCHEMA_ID_VERSION_1);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(2L), any())).thenReturn(SCHEMA_ID_VERSION_2);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(3L), any())).thenReturn(SCHEMA_ID_VERSION_3);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(4L), any())).thenReturn(SCHEMA_ID_VERSION_4);
        UploadResult expected = new UploadResult(4, 0, Collections.emptyList());

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());
    }
    
    @Test
    public void testUploadInvalidVersionValidVersion() throws Exception {
        //given
        InputStream is = getClass().getResourceAsStream("/exportimport/simplecloudera.json");
        SchemaBranch masterBranch = createBranch(1L, "MASTER", "rain", "'MASTER' branch for schema metadata 'rain'");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch);
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenReturn(createMetadata(1L, "rain"));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_1))).thenReturn(createVersion(1L, "invalid", 1, "text"));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_2))).thenThrow(SchemaNotFoundException.class);
        UploadResult expected = new UploadResult(1, 1, Lists.newArrayList(1L));

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());
    }

    @Test
    public void testUploadValidMetadataInvalidVersion() throws Exception {
        //given
        String schemaText = "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Rain\",\n     \"fields\": [\n       " +
            "{ \"name\": \"day\", \"type\": \"string\" },\n       { \"name\": \"temperature\", \"type\": \"int\" }\n     ]\n}";
        InputStream is = getClass().getResourceAsStream("/exportimport/simplecloudera.json");
        SchemaBranch masterBranch = createBranch(1L, "MASTER", "rain", "'MASTER' branch for schema metadata 'rain'");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch);
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenReturn(createMetadata(1L, "rain"));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_1))).thenReturn(createVersion(1L, "rain", 1, schemaText));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_2))).thenThrow(SchemaNotFoundException.class);
        UploadResult expected = new UploadResult(1, 0, Collections.emptyList());

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());
    }

    @Test
    public void testUploadValidMetadataInvalidJsonMetadata() throws Exception {
        //given
        InputStream is = getClass().getResourceAsStream("/exportimport/jsonschema.json");
        AtomicInteger callCount1L = new AtomicInteger();
        SchemaBranch masterBranch = createBranch(1L, "MASTER", "Avro", "'MASTER' branch for schema metadata 'Avro'");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch);
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenAnswer(ans -> {
            if (callCount1L.get() == 0) {
                callCount1L.getAndIncrement();
                return null;
            } else {
                callCount1L.getAndIncrement();
                return createMetadata(1L, "Rain");
            }
        });
        when(schemaRegistry.addSchemaMetadata(eq(1L), any())).thenReturn(1L);
        when(schemaRegistry.getSchemaMetadataInfo(eq(2L))).thenReturn(createMetadata(2L, "not-json"));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_1))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_2))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(1L), any())).thenReturn(SCHEMA_ID_VERSION_1);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(2L), any())).thenReturn(SCHEMA_ID_VERSION_2);


        UploadResult expected = new UploadResult(2, 2, Lists.newArrayList(3L, 4L));

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());
    }

    @Test
    public void testUploadValidMetadataValidVersionsValidMetadataInvalidVersion() throws Exception {
        //given
        InputStream is = getClass().getResourceAsStream("/exportimport/twometas.json");
        SchemaBranch masterBranch1 = createBranch(1L, "MASTER", "rain", "'MASTER' branch for schema metadata 'rain'");
        SchemaBranch masterBranch2 = createBranch(2L, "MASTER", "Food", "'MASTER' branch for schema metadata 'Food'");
        when(schemaRegistry.getSchemaBranch(eq(1L))).thenReturn(masterBranch1);
        when(schemaRegistry.getSchemaBranch(eq(2L))).thenReturn(masterBranch2);
        when(schemaRegistry.getSchemaMetadataInfo(eq(1L))).thenReturn(createMetadata(1L, "rain"));
        when(schemaRegistry.getSchemaMetadataInfo(eq(2L))).thenReturn(createMetadata(2L, "Food"));
        String schemaText1 = "{\n     \"type\": \"record\",\n     \"namespace\": \"com.example\",\n     \"name\": \"Rain\",\n     \"fields\": " +
        "[\n       { \"name\": \"day\", \"type\": \"string\" },\n       { \"name\": \"temperature\", \"type\": \"int\" }\n     ]\n}";
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_1))).thenReturn(createVersion(1L, "rain", 1, schemaText1));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_2))).thenThrow(SchemaNotFoundException.class);
        String schemaText3 = "{\n \"type\": \"record\",\n \"namespace\": \"com.example\",\n \"name\": \"Food\",\n \"fields\": [\n  " +
            "{\n   \"name\": \"color\",\n   \"type\": \"string\"\n  },\n  {\n   \"name\": \"kg\",\n   \"type\": \"int\"\n  }\n ]\n}";
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_3))).thenReturn(createVersion(3L, "rain", 1, schemaText3));
        when(schemaRegistry.getSchemaVersionInfo(eq(SCHEMA_ID_VERSION_4))).thenThrow(SchemaNotFoundException.class);
        when(schemaRegistry.addSchemaMetadata(eq(2L), any())).thenReturn(2L);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(3L), any())).thenReturn(SCHEMA_ID_VERSION_3);
        when(schemaRegistry.addSchemaVersionWithBranchName(anyString(), any(), eq(4L), any())).thenReturn(SCHEMA_ID_VERSION_4);
        UploadResult expected = new UploadResult(2, 0, Collections.emptyList());

        //when
        UploadResult actual = bulkUploadService.bulkUploadSchemas(is, false, BulkUploadInputFormat.CLOUDERA);

        //then
        assertEquals(expected.getSuccessCount(), actual.getSuccessCount());
        assertEquals(expected.getFailedCount(), actual.getFailedCount());
        assertIterableEquals(expected.getFailedIds(), actual.getFailedIds());
    }

    private SchemaVersionInfo createVersion(Long id, String schemaName, int version, String schemaText) {
        return new SchemaVersionInfo(id, schemaName, version, schemaText, System.currentTimeMillis(), "");
    }

    private SchemaMetadataInfo createMetadata(Long id, String name) {
        SchemaMetadata meta = new SchemaMetadata.Builder(name)
                .evolve(true)
                .compatibility(SchemaCompatibility.DEFAULT_COMPATIBILITY)
                .validationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL)
                .schemaGroup("Kafka")
                .type("avro")
                .build();
        return new SchemaMetadataInfo(meta, id, System.currentTimeMillis());
    }
    
    private SchemaBranch createBranch(Long id, String name, String metadataName, String desc) {
        return new SchemaBranch(id, name, metadataName, desc, System.currentTimeMillis());
    }
}
