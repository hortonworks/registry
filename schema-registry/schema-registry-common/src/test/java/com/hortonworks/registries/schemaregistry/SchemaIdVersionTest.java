/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SchemaIdVersionTest {

    @Test
    public void testEqualityWithCache() throws Exception {
        Cache<SchemaIdVersion, String> cache = CacheBuilder.newBuilder().maximumSize(10).build();
        ConcurrentMap<SchemaIdVersion, String> map = cache.asMap();
        doTestSchemaVersionIdEquality(map);
    }

    private void doTestSchemaVersionIdEquality(ConcurrentMap<SchemaIdVersion, String> map) {
        String value = "value";
        Integer version = 2;
        Long metadataId = 1L;
        Long schemaVersionId = 1L;

        // SchemaIdVersion objects containing same schemaVersionId
        SchemaIdVersion schemaIdVersion1 = new SchemaIdVersion(metadataId, version, schemaVersionId);
        SchemaIdVersion schemaIdVersion2 = new SchemaIdVersion(schemaVersionId);
        map.put(schemaIdVersion1, value);

        String storedValue = map.get(schemaIdVersion2);
        assertEquals(value, storedValue);

        // invalidating schemaIdVersion2 should remove value
        map.remove(schemaIdVersion2);
        assertFalse(map.containsKey(schemaIdVersion1));
        assertFalse(map.containsKey(schemaIdVersion2));


    }

    @Test
    public void testEqualityWithMap() throws Exception {
        doTestSchemaVersionIdEquality(new ConcurrentHashMap<>());
    }
}
