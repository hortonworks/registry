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
 */
package com.cloudera.dim.atlas.events;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable.EventType;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class AtlasEventsProcessorTest {

    private AtlasPlugin atlasPlugin;
    private StorageManager storageManager;
    private TransactionManager transactionManager;
    private AtlasEventsProcessor atlasEventsProcessor;
    private Collection<AtlasEventStorable> persistedEvents;

    @BeforeEach
    public void setUp() {
        atlasPlugin = mock(AtlasPlugin.class);
        storageManager = mock(StorageManager.class);
        transactionManager = mock(TransactionManager.class);
        atlasEventsProcessor = new AtlasEventsProcessor(atlasPlugin, storageManager, transactionManager, 1000L, true);

        persistedEvents = new ArrayList<>();
        when(storageManager.<AtlasEventStorable>search(any(SearchQuery.class))).thenReturn(persistedEvents);
        when(atlasPlugin.isKafkaSchemaModelInitialized()).thenReturn(true);
        when(atlasPlugin.createMeta(any())).thenReturn(RandomStringUtils.randomAlphabetic(10));
    }

    @Test
    public void testNothing() throws Exception {
        // given
        persistedEvents.clear();

        // when
        atlasEventsProcessor.processAtlasEvents();

        // then
        verifyNoInteractions(atlasPlugin);
    }

    @Test
    public void testSearchesForAtlasEventsWithLocking() throws Exception {
        persistedEvents.add(new AtlasEventStorable());

        // when
        atlasEventsProcessor.processAtlasEvents();

        verify(storageManager).search(argThat(query ->
                        AtlasEventStorable.NAME_SPACE.equals(query.getNameSpace()) &&
                                query.isForUpdate()
                )
        );
    }

    @Test
    public void testCreateMeta() throws Exception {
        // given
        final Long metaId = RandomUtils.nextLong();
        final String schemaName = RandomStringUtils.randomAlphabetic(8);

        SchemaMetadataStorable metadataStorable = new SchemaMetadataStorable();
        metadataStorable.setId(metaId);
        metadataStorable.setName(schemaName);
        metadataStorable.setDescription("desc");
        metadataStorable.setEvolve(true);
        metadataStorable.setCompatibility(SchemaCompatibility.BACKWARD);
        metadataStorable.setType("avro");
        metadataStorable.setSchemaGroup("kafka");
        metadataStorable.setValidationLevel(SchemaValidationLevel.DEFAULT_VALIDATION_LEVEL);

        List<Storable> result = Lists.newArrayList();
        result.add(metadataStorable);

        when(storageManager.find(eq(SchemaMetadataStorable.NAME_SPACE), anyList())).thenReturn(result);

        AtlasEventStorable atlasEvent = new AtlasEventStorable();
        atlasEvent.setType(EventType.CREATE_META);
        atlasEvent.setProcessedId(metaId);
        persistedEvents.add(atlasEvent);

        // when
        atlasEventsProcessor.processAtlasEvents();

        // then
        verify(transactionManager).beginTransaction(TransactionIsolation.READ_COMMITTED);
        verify(transactionManager).commitTransaction();
        ArgumentCaptor<SchemaMetadataInfo> captor = ArgumentCaptor.forClass(SchemaMetadataInfo.class);
        verify(atlasPlugin).createMeta(captor.capture());
        SchemaMetadataInfo actualValue = captor.getValue();

        assertNotNull(actualValue);
        assertEquals(metadataStorable.getName(), actualValue.getSchemaMetadata().getName());
        assertEquals(metadataStorable.getDescription(), actualValue.getSchemaMetadata().getDescription());

        verify(storageManager).update(any(AtlasEventStorable.class));
        verify(atlasPlugin).connectSchemaWithTopic(anyString(), any());
    }

    @Test
    public void testEmptyAuditList() throws Exception {
        //given
        persistedEvents.clear();

        //when
        atlasEventsProcessor.processAtlasEvents();

        //then
        verify(transactionManager).commitTransaction();
    }

    @Test
    public void testProcessAuditEntryException() throws Exception {
        //given
        AtlasEventStorable atlasEvent = new AtlasEventStorable();
        atlasEvent.setType(EventType.CREATE_META);
        atlasEvent.setProcessedId(-1L);
        persistedEvents.add(atlasEvent);

        //when
        atlasEventsProcessor.processAtlasEvents();

        //then
        verify(transactionManager).commitTransaction();
        verify(storageManager).update(atlasEvent);
    }
}
