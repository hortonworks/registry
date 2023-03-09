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
package com.hortonworks.registries.schemaregistry.client;

import com.google.common.collect.Sets;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.shaded.javax.ws.rs.NotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        SchemaVersionInfo schemaVersionInfo =
            new SchemaVersionInfo(schemaVersionId, schemaName, version, schemaMetadataId, "schema-text",
                System.currentTimeMillis(), "schema-description", SchemaVersionLifecycleStates.ENABLED.getId());
        SchemaIdVersion withVersionId = new SchemaIdVersion(schemaVersionId);
        SchemaIdVersion withMetaIdAndVersion = new SchemaIdVersion(schemaMetadataId, version);
        SchemaIdVersion withBoth = new SchemaIdVersion(schemaMetadataId, version, schemaVersionId);
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, version);
        schemaKeyWithVersionInfo.put(schemaVersionKey, schemaVersionInfo);
        HashSet<SchemaIdVersion> allIdVersions = Sets.newHashSet(withVersionId, withMetaIdAndVersion, withBoth);
        allIdVersions.stream().forEach(x -> schemaIdWithVersionInfo.put(x, schemaVersionInfo));

        SchemaVersionInfo otherSchemaVersionInfo = new SchemaVersionInfo(schemaVersionId + 1, "other-" + schemaName, version, schemaMetadataId + 1L,
            "other-schema-text", System.currentTimeMillis(), "schema-description", SchemaVersionLifecycleStates.ENABLED.getId());
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
        SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher = new SchemaMetadataCache.SchemaMetadataFetcher() {
            @Override
            public SchemaMetadataInfo fetch(String name) throws SchemaNotFoundException {
                return null;
            }

            @Override
            public SchemaMetadataInfo fetch(Long id) throws SchemaNotFoundException {
                return null;
            }
        };

        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(schemaRetriever, schemaMetadataFetcher, 32, 60 * 60 * 1000L);

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
            Assertions.assertEquals(schemaVersionInfo, recvdSchemaVersionInfo);
        }

        // access with version key
        SchemaVersionInfo recvdSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
        Assertions.assertEquals(schemaVersionInfo, recvdSchemaVersionInfo);

        // invalidate one kind of schemaIdVersion, and all other combinations should return null
        schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(withVersionId));
        for (SchemaIdVersion idVersion : allIdVersions) {
            recvdSchemaVersionInfo = schemaVersionInfoCache.getSchemaIfPresent(SchemaVersionInfoCache.Key.of(idVersion));
            Assertions.assertNull(recvdSchemaVersionInfo);
        }

        SchemaVersionInfo recvdOtherSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(otherSchemaVersionKey));
        Assertions.assertEquals(otherSchemaVersionInfo, recvdOtherSchemaVersionInfo);
        recvdOtherSchemaVersionInfo = schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(otherIdVersion));
        Assertions.assertEquals(otherSchemaVersionInfo, recvdOtherSchemaVersionInfo);

        // all values for these keys should be non existent in cache
        for (SchemaIdVersion idVersion : allIdVersions) {
            Assertions.assertNull(schemaVersionInfoCache.getSchemaIfPresent(SchemaVersionInfoCache.Key.of(idVersion)));
        }

        // all values for these keys should be loaded
        for (SchemaIdVersion idVersion : allIdVersions) {
            Assertions.assertEquals(schemaVersionInfo, schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(idVersion)));
        }

        // all values for these keys should exist locally without loading from target
        for (SchemaIdVersion idVersion : allIdVersions) {
            Assertions.assertEquals(schemaVersionInfo, schemaVersionInfoCache.getSchemaIfPresent(SchemaVersionInfoCache.Key.of(idVersion)));
        }

    }

    @Test
    public void getSchemaBySchemaIdVersion() throws Exception {
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        when(mySchemaRetriever.retrieveSchemaVersion(any(SchemaIdVersion.class))).thenReturn(createSchemaVersionInfo());
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L);

        SchemaVersionInfo result = schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaIdVersion(15L, 6)));

        verify(mySchemaRetriever).retrieveSchemaVersion(any(SchemaIdVersion.class));
        verifyNoMoreInteractions(mySchemaRetriever);
        assertSchemaVersionInfo(result);
    }

    @Test
    public void getSchemaBySchemaVersionKey() throws Exception {
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        when(mySchemaRetriever.retrieveSchemaVersion(any(SchemaVersionKey.class))).thenReturn(createSchemaVersionInfo());
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L);

        SchemaVersionInfo result = schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name", 6)));

        verify(mySchemaRetriever).retrieveSchemaVersion(any(SchemaVersionKey.class));
        verifyNoMoreInteractions(mySchemaRetriever);
        assertSchemaVersionInfo(result);
    }

    @Test
    public void getSchemaVersionInfoFromCache() throws Exception {
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        when(mySchemaRetriever.retrieveSchemaVersion(any(SchemaVersionKey.class))).thenReturn(createSchemaVersionInfo());
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L);

        // fill up the cache from remote
        schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name", 6)));
        // get schemaversioninfo from the cache using different inputs
        SchemaVersionInfo result = schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name", 6)));
        SchemaVersionInfo resultFromSchemaVersionId = schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaIdVersion(15L)));
        SchemaVersionInfo resultFromSchemaMetadataVersion = schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaIdVersion(5L, 6)));

        verify(mySchemaRetriever).retrieveSchemaVersion(any(SchemaVersionKey.class));
        verifyNoMoreInteractions(mySchemaRetriever);
        assertSchemaVersionInfo(result);
        assertSchemaVersionInfo(resultFromSchemaVersionId);
        assertSchemaVersionInfo(resultFromSchemaMetadataVersion);
    }

    @Test
    public void getSchemaVersionInfoWithoutMetadataId() throws Exception {
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher = Mockito.mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
        when(mySchemaRetriever.retrieveSchemaVersion(any(SchemaIdVersion.class))).thenReturn(createSchemaVersionInfoWithoutMetadataId());
        when(schemaMetadataFetcher.fetch("schema-name")).thenReturn(new SchemaMetadataInfo(
             new SchemaMetadata.Builder("schema-metadata-name").type("type1").build(),
            5L, 0L));
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, schemaMetadataFetcher, 32,
            60 * 1000L);

        SchemaVersionInfo result = schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaIdVersion(15L)));

        verify(mySchemaRetriever).retrieveSchemaVersion(any(SchemaIdVersion.class));
        verify(schemaMetadataFetcher).fetch("schema-name");
        verifyNoMoreInteractions(mySchemaRetriever);
        assertSchemaVersionInfo(result);
    }
    @Test
    public void getSchemaThrowsSchemaNotFoundException() throws Exception {
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        SchemaVersionKey versionKey = new SchemaVersionKey("schema-1", 1);
        when(mySchemaRetriever.retrieveSchemaVersion(versionKey)).thenThrow(new SchemaNotFoundException("Schema not found.", "entity"));
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L);

        SchemaNotFoundException thrown = Assertions
            .assertThrows(SchemaNotFoundException.class, () -> {
                schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(versionKey));
            }, "NumberFormatException error was expected");

        Assertions.assertEquals(versionKey.toString(), thrown.getMessage());
    }

    @Test
    public void getSchemaThrowsNotFoundException() throws Exception {
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        SchemaVersionKey versionKey = new SchemaVersionKey("schema-1", 1);
        when(mySchemaRetriever.retrieveSchemaVersion(versionKey)).thenThrow(new NotFoundException("HTTP 404."));
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L);

        SchemaNotFoundException thrown = Assertions
            .assertThrows(SchemaNotFoundException.class, () -> {
                schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(versionKey));
            }, "NumberFormatException error was expected");

        Assertions.assertEquals(SchemaVersionInfoCache.Key.of(versionKey).toString(), thrown.getMessage());
    }

    private static SchemaVersionInfo createSchemaVersionInfo() {
        return new SchemaVersionInfo(15L,
            "schema-name", 6, 5L, "text", 0L, "description", null);
    }
    private static SchemaVersionInfo createSchemaVersionInfoWithoutMetadataId() {
        return new SchemaVersionInfo(15L,
            "schema-name", 6, null, "text", 0L, "description", null);
    }

    private static void assertSchemaVersionInfo(SchemaVersionInfo result) {
        Assertions.assertEquals(15L, result.getId());
        Assertions.assertEquals("schema-name", result.getName());
        Assertions.assertEquals(6, result.getVersion());
        Assertions.assertEquals(5L, result.getSchemaMetadataId());
        Assertions.assertEquals("text", result.getSchemaText());
        Assertions.assertEquals(0L, result.getTimestamp());
        Assertions.assertEquals("description", result.getDescription());
    }

    @Test
    public void itInvalidatesCaches() throws Exception {
        ConcurrentMap<SchemaVersionKey, Long> metadataNameVersion2Id = new ConcurrentHashMap<>();
        ConcurrentMap<SchemaIdVersion, Long> metadataIdVersion2Id = new ConcurrentHashMap<>();
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        when(mySchemaRetriever.retrieveSchemaVersion(any(SchemaVersionKey.class))).thenReturn(createSchemaVersionInfo());
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L, metadataNameVersion2Id, metadataIdVersion2Id);


        schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name", 6)));

        Assertions.assertEquals(1, metadataNameVersion2Id.size());
        Assertions.assertEquals(1, metadataIdVersion2Id.size());
        schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name", 6)));
        Assertions.assertEquals(0, metadataNameVersion2Id.size());
        Assertions.assertEquals(0, metadataIdVersion2Id.size());
    }

    @Test
    public void itInvalidatesAll() throws Exception {
        ConcurrentMap<SchemaVersionKey, Long> metadataNameVersion2Id = new ConcurrentHashMap<>();
        ConcurrentMap<SchemaIdVersion, Long> metadataIdVersion2Id = new ConcurrentHashMap<>();
        SchemaVersionRetriever mySchemaRetriever = Mockito.mock(SchemaVersionRetriever.class);
        when(mySchemaRetriever.retrieveSchemaVersion(new SchemaVersionKey("schema-name", 6))).thenReturn(createSchemaVersionInfo());
        when(mySchemaRetriever.retrieveSchemaVersion(new SchemaVersionKey("schema-name2", 7))).thenReturn(new SchemaVersionInfo(16L,
            "schema-name2", 7, 6L, "text2", 2L, "description2", null));
        SchemaVersionInfoCache schemaVersionInfoCache = new SchemaVersionInfoCache(mySchemaRetriever, null, 32,
            60 * 1000L, metadataNameVersion2Id, metadataIdVersion2Id);

        schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name", 6)));
        schemaVersionInfoCache.getSchema(
            SchemaVersionInfoCache.Key.of(new SchemaVersionKey("schema-name2", 7)));

        Assertions.assertEquals(2, metadataNameVersion2Id.size());
        Assertions.assertEquals(2, metadataIdVersion2Id.size());
        schemaVersionInfoCache.invalidateAll();
        Assertions.assertEquals(0, metadataNameVersion2Id.size());
        Assertions.assertEquals(0, metadataIdVersion2Id.size());
    }
}
