/*
 * Copyright 2016 Hortonworks.
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
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
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
        SchemaIdVersion schemaIdVersion_1 = new SchemaIdVersion(metadataId, version, schemaVersionId);
        SchemaIdVersion schemaIdVersion_2 = new SchemaIdVersion(schemaVersionId);
        map.put(schemaIdVersion_1, value);

        String storedValue = map.get(schemaIdVersion_2);
        Assert.assertEquals(value, storedValue);

        // invalidating schemaIdVersion_2 should remove value
        map.remove(schemaIdVersion_2);
        Assert.assertFalse(map.containsKey(schemaIdVersion_1));
        Assert.assertFalse(map.containsKey(schemaIdVersion_2));


    }

    @Test
    public void testEqualityWithMap() throws Exception {
        doTestSchemaVersionIdEquality(new ConcurrentHashMap<>());
    }
}
