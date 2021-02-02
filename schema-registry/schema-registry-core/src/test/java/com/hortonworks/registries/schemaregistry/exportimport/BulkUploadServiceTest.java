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
package com.hortonworks.registries.schemaregistry.exportimport;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkUploadServiceTest {

    private ISchemaRegistry schemaRegistry;
    private BulkUploadService bulkUploadService;

    @Before
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

        assertFalse("Expected schema to be valid.", valid1);
        assertTrue("Expected schema to be not valid.", valid2);
    }

    @Test
    public void testValidateSchemaVersion() {
        assertTrue(bulkUploadService.validateSchemaVersion(createVersion(1L, "schema1", 1, "{ }")));
        assertFalse(bulkUploadService.validateSchemaVersion(createVersion(2L, "schema2", 1, "")));
    }

    @Test
    public void testUploadValidSchemas() throws Exception {
        SchemaMetadataInfo car = createMetadata(1L, "Car");
        SchemaMetadataInfo fruit = createMetadata(2L, "Fruit");
        SchemaVersionInfo carV1 = createVersion(1L, "Car", 1, "avro1");
        SchemaVersionInfo carV2 = createVersion(2L, "Car", 2, "avro2");
        SchemaVersionInfo carV3 = createVersion(4L, "Car", 3, "avro3");
        SchemaVersionInfo fruitVersion = createVersion(3L, "Fruit", 1, "avro4");

        final Map<Long, SchemaMetadata> mockDatabase = new HashMap<>();
        final Map<String, Integer> versionCount = new HashMap<>();
        when(schemaRegistry.addSchemaMetadata(any(Long.class), any(SchemaMetadata.class))).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                Long id = invocation.getArgument(0, Long.class);
                SchemaMetadata meta = invocation.getArgument(1, SchemaMetadata.class);
                mockDatabase.put(id, meta);
                return id;
            }
        });
        when(schemaRegistry.getSchemaMetadataInfo(any(Long.class))).thenAnswer(new Answer<SchemaMetadataInfo>() {
            @Override
            public SchemaMetadataInfo answer(InvocationOnMock invocation) throws Throwable {
                return new SchemaMetadataInfo(mockDatabase.get(invocation.getArgument(0, Long.class)),
                        invocation.getArgument(0, Long.class), System.currentTimeMillis());
            }
        });
        when(schemaRegistry.addSchemaVersion(any(SchemaMetadata.class), any(Long.class), any(SchemaVersion.class)))
                .thenAnswer(new Answer<SchemaIdVersion>() {
                    @Override
                    public SchemaIdVersion answer(InvocationOnMock invocation) throws Throwable {
                        SchemaMetadata schemaMetadata = invocation.getArgument(0, SchemaMetadata.class);
                        int count = versionCount.getOrDefault(schemaMetadata.getName(), 0);
                        versionCount.put(schemaMetadata.getName(), count + 1);
                        return new SchemaIdVersion(invocation.getArgument(1, Long.class), count + 1);
                    }
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
        verify(schemaRegistry, times(2)).addSchemaMetadata(any(Long.class), any(SchemaMetadata.class));
        verify(schemaRegistry, times(2)).getSchemaMetadataInfo(any(Long.class));
        verify(schemaRegistry, times(4)).addSchemaVersion(any(SchemaMetadata.class), any(Long.class), any(SchemaVersion.class));

        assertNotNull(result);
        assertEquals(1, result.getFailedCount());  // 77L
        assertEquals(4, result.getSuccessCount());
        assertTrue(result.getFailedIds().contains(77L));
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
}
