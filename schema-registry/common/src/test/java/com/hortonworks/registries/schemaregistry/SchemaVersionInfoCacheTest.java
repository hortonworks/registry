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

import com.google.common.collect.Sets;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 */
public class SchemaVersionInfoCacheTest {

    @Test
    public void testSchemaVersionCache() throws Exception {

        final Map<SchemaIdVersion, SchemaVersionInfo> schemaIdWithVersionInfo = new HashMap<>();
        final Map<SchemaVersionKey, SchemaVersionInfo> schemaKeyWithVersionInfo = new HashMap<>();

        long schemaMetadataId = 1L;
        String schemaName = "schema-1";
        Integer version = 2;
        long schemaVersionId = 3L;
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(schemaVersionId, schemaName, version, schemaMetadataId, "schema-text", System
                .currentTimeMillis(), "schema-description", SchemaVersionLifecycleStates.ENABLED.getId());
        SchemaIdVersion withVersionId = new SchemaIdVersion(schemaVersionId);
        SchemaIdVersion withMetaIdAndVersion = new SchemaIdVersion(schemaMetadataId, version);
        SchemaIdVersion withBoth = new SchemaIdVersion(schemaMetadataId, version, schemaVersionId);
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, version);
        schemaKeyWithVersionInfo.put(schemaVersionKey, schemaVersionInfo);
        HashSet<SchemaIdVersion> allIdVersions = Sets.newHashSet(withVersionId, withMetaIdAndVersion, withBoth);
        allIdVersions.stream().forEach(x -> schemaIdWithVersionInfo.put(x, schemaVersionInfo));

        SchemaVersionInfo otherSchemaVersionInfo = new SchemaVersionInfo(schemaVersionId + 1, "other-" + schemaName, version, schemaMetadataId+1L, "other-schema-text", System
                .currentTimeMillis(), "schema-description", SchemaVersionLifecycleStates.ENABLED.getId());
        SchemaIdVersion otherIdVersion = new SchemaIdVersion(otherSchemaVersionInfo.getId());
        SchemaVersionKey otherSchemaVersionKey = new SchemaVersionKey(otherSchemaVersionInfo.getName(), otherSchemaVersionInfo
                .getVersion());
        schemaIdWithVersionInfo.put(otherIdVersion, otherSchemaVersionInfo);
        schemaKeyWithVersionInfo.put(otherSchemaVersionKey, otherSchemaVersionInfo);


        SchemaVersionRetriever schemaRetriever = new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return schemaKeyWithVersionInfo.get(key);
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                return schemaIdWithVersionInfo.get(key);
            }
        };

        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(schemaRetriever, 32, 60 * 1000L);

        // invalidate key without accessing earlier.
        for (SchemaVersionKey versionKey : schemaKeyWithVersionInfo.keySet()) {
            schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(versionKey));
        }

        for (SchemaIdVersion schemaIdVersion : schemaIdWithVersionInfo.keySet()) {
            schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
        }

        // access with all version id keys
        for (SchemaIdVersion key : allIdVersions) {
            SchemaVersionInfo recvdSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(key));
            Assert.assertEquals(schemaVersionInfo, recvdSchemaVersionInfo);
        }

        // access with version key
        SchemaVersionInfo recvdSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
        Assert.assertEquals(schemaVersionInfo, recvdSchemaVersionInfo);

        // invalidate one kind of schemaIdVersion, and all other combinations should return null
        schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(withVersionId));
        for (SchemaIdVersion idVersion : allIdVersions) {
            recvdSchemaVersionInfo = schemaVersionInfoCache.getSchemaIfPresent(SchemaVersionInfoCache.Key.of(idVersion));
            Assert.assertNull(recvdSchemaVersionInfo);
        }

        SchemaVersionInfo recvdOtherSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(otherSchemaVersionKey));
        Assert.assertEquals(otherSchemaVersionInfo, recvdOtherSchemaVersionInfo);
        recvdOtherSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(otherIdVersion));
        Assert.assertEquals(otherSchemaVersionInfo, recvdOtherSchemaVersionInfo);

        // all values for these keys should be non existent in cache
        for (SchemaIdVersion idVersion : allIdVersions) {
            Assert.assertNull(schemaVersionInfoCache.getSchemaIfPresent(SchemaVersionInfoCache.Key.of(idVersion)));
        }

        // all values for these keys should be loaded
        for (SchemaIdVersion idVersion : allIdVersions) {
            Assert.assertEquals(schemaVersionInfo, schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(idVersion)));
        }

        // all values for these keys should exist locally without loading from target
        for (SchemaIdVersion idVersion : allIdVersions) {
            Assert.assertEquals(schemaVersionInfo, schemaVersionInfoCache.getSchemaIfPresent(SchemaVersionInfoCache.Key.of(idVersion)));
        }

    }
}
