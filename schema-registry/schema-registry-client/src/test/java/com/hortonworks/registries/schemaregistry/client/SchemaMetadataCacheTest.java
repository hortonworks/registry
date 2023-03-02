/**
 * Copyright 2016-2023 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadataCache.Key;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryRetryableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaMetadataCacheTest {
  private SchemaMetadataCache testObj;

  @Test
  public void itInvalidatesCache() throws SchemaNotFoundException {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    SchemaMetadataInfo expected = new SchemaMetadataInfo(new SchemaMetadata.Builder("metadata1")
        .type("type").build(), 12L, 34L);
    when(metadataFetcher.fetch("metadata1")).thenReturn(expected);
    when(metadataFetcher.fetch(12L)).thenReturn(expected);
    Map<String, Long> metadataName2Id = new ConcurrentHashMap<>();
    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher, metadataName2Id);
    assertEquals(expected,
        testObj.get(Key.of("metadata1")));
    assertEquals(expected, testObj.getIfPresent(Key.of(12L)));

    testObj.invalidateSchemaMetadata(Key.of(12L));
    assertNull(testObj.getIfPresent(Key.of(12L)));
    assertNull(testObj.getIfPresent(Key.of("metadata1")));
    assertTrue(metadataName2Id.isEmpty());
  }

  @Test
  public void itWrapsSchemaNotFoundExceptionIntoRegistryException() throws SchemaNotFoundException {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    when(metadataFetcher.fetch("metadata1")).thenThrow(new SchemaNotFoundException("Not found.", "metadata1"));

    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher);

    Assertions.assertNull(testObj.get(Key.of("metadata1")));
  }

  @Test
  public void itWrapsRuntimeExceptionIntoRegistryRetryableException() throws SchemaNotFoundException {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    when(metadataFetcher.fetch("metadata1")).thenThrow(new RuntimeException(new IOException("metadata1 IOException")));

    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher);
    RegistryRetryableException thrown = Assertions
        .assertThrows(RegistryRetryableException.class, () -> {
          testObj.get(Key.of("metadata1"));
        }, "RegistryRetryableException was expected");
    Assertions.assertEquals("java.io.IOException: metadata1 IOException", thrown.getMessage());
  }

  @Test
  public void itWrapsRuntimeExceptionIntoRegistryException() throws SchemaNotFoundException {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    when(metadataFetcher.fetch("metadata1")).thenThrow(new RuntimeException(new NullPointerException("metadata1 NPE")));
    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher);
    RegistryException thrown = Assertions
        .assertThrows(RegistryException.class, () -> {
          testObj.get(Key.of("metadata1"));
        }, "RegistryException was expected");
    Assertions.assertEquals("java.lang.NullPointerException: metadata1 NPE", thrown.getMessage());
  }

  @Test
  public void testPut() {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher);

    testObj.put(Key.of(1L),
        new SchemaMetadataInfo(new SchemaMetadata.Builder("name1").type("type").build(), 1L, 0L));
    Assertions.assertEquals("name1", testObj.getIfPresent(Key.of(1L)).getSchemaMetadata().getName());
    Assertions.assertEquals(1L, testObj.getIfPresent(Key.of(1L)).getId());

    testObj.put(Key.of("name2"),
        new SchemaMetadataInfo(new SchemaMetadata.Builder("name2").type("type").build(), 2L, 0L));
    Assertions.assertEquals("name2", testObj.getIfPresent(Key.of(2L)).getSchemaMetadata().getName());
    Assertions.assertEquals(2L, testObj.getIfPresent(Key.of(2L)).getId());

  }

  @Test
  public void testGet() throws SchemaNotFoundException {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    SchemaMetadataInfo expected = new SchemaMetadataInfo(new SchemaMetadata.Builder("metadata1")
        .type("type").build(), 12L, 34L);
    when(metadataFetcher.fetch("metadata1")).thenReturn(expected);
    when(metadataFetcher.fetch(12L)).thenReturn(expected);
    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher);

    Assertions.assertNull(testObj.get(null));
    Assertions.assertEquals(expected, testObj.get(Key.of(12L)));
    Assertions.assertEquals(expected, testObj.get(Key.of("metadata1")));
  }

  @Test
  public void testGetIfPresent() throws SchemaNotFoundException {
    SchemaMetadataCache.SchemaMetadataFetcher metadataFetcher = mock(SchemaMetadataCache.SchemaMetadataFetcher.class);
    SchemaMetadataInfo expected = new SchemaMetadataInfo(new SchemaMetadata.Builder("metadata1")
        .type("type").build(), 12L, 34L);
    when(metadataFetcher.fetch("metadata1")).thenReturn(expected);
    when(metadataFetcher.fetch(12L)).thenReturn(expected);
    testObj = new SchemaMetadataCache(100L, 60 * 60L, metadataFetcher);

    Assertions.assertNull(testObj.getIfPresent(null));
    Assertions.assertNull(testObj.getIfPresent(Key.of(12L)));
    Assertions.assertNull(testObj.getIfPresent(Key.of("metadata1")));

    Assertions.assertEquals(expected, testObj.get(Key.of(12L)));
    Assertions.assertEquals(expected, testObj.getIfPresent(Key.of(12L)));
    Assertions.assertEquals(expected, testObj.getIfPresent(Key.of("metadata1")));
  }
}