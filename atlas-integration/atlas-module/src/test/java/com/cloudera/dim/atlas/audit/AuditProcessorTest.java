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
package com.cloudera.dim.atlas.audit;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.RegistryAuditStorable;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class AuditProcessorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ISchemaRegistry schemaRegistry;
    private StorageManager storageManager;
    private TransactionManager transactionManager;
    private AuditProcessor auditProcessor;
    private Collection<RegistryAuditStorable> audits;

    @BeforeEach
    public void setUp() {
        schemaRegistry = mock(ISchemaRegistry.class);
        storageManager = mock(StorageManager.class);
        transactionManager = mock(TransactionManager.class);
        auditProcessor = new AuditProcessor(schemaRegistry, storageManager, transactionManager, 1000L);

        audits = new ArrayList<>();
        when(storageManager.<RegistryAuditStorable>find(eq(RegistryAuditStorable.NAME_SPACE), anyList())).thenReturn(audits);
    }

    @Test
    public void testNothing() throws Exception {
        // given
        audits.clear();

        // when
        auditProcessor.processAuditEntries();

        // then
        verify(storageManager).find(eq(RegistryAuditStorable.NAME_SPACE), anyList());
        verifyNoInteractions(schemaRegistry);
    }

    @Test
    public void testMethodInvocation() throws Exception {
        // given we log an entry for schemaRegistry.addSerDes(serdes)
        RegistryAuditStorable auditStorable = createAuditStorableWithId(1L);
        AuditEntry auditEntry = new AuditEntry();
        auditEntry.setMethodName("addSerDes");
        auditEntry.setParamTypes(Arrays.asList(SerDesPair.class.getName()));
        SerDesPair serDesPair = new SerDesPair("name", "description", "fileId", "serializer", "deserializer");
        auditEntry.setParamValues(Arrays.asList(objectMapper.writeValueAsString(serDesPair)));
        auditStorable.setProcessedData(objectMapper.writeValueAsString(auditEntry));

        audits.add(auditStorable);

        // when
        auditProcessor.processAuditEntries();

        // then
        verify(transactionManager).beginTransaction(TransactionIsolation.SERIALIZABLE);
        verify(transactionManager).commitTransaction();
        verify(transactionManager).lockTable(RegistryAuditStorable.NAME_SPACE);
        verify(transactionManager).unlockTable(RegistryAuditStorable.NAME_SPACE);
        verify(storageManager).find(eq(RegistryAuditStorable.NAME_SPACE), anyList());
        ArgumentCaptor<SerDesPair> captor = ArgumentCaptor.forClass(SerDesPair.class);
        verify(schemaRegistry).addSerDes(captor.capture());
        SerDesPair actualValue = captor.getValue();

        assertNotNull(actualValue);
        assertEquals(serDesPair.getName(), actualValue.getName());
        assertEquals(serDesPair.getFileId(), actualValue.getFileId());

        verify(storageManager).update(any(RegistryAuditStorable.class));
    }
    
    @Test
    public void testEmptyAuditList() throws Exception {
        //given
        audits = Collections.EMPTY_LIST;
        
        //when
        auditProcessor.processAuditEntries();
        
        //then
        verify(transactionManager).beginTransaction(TransactionIsolation.SERIALIZABLE);
        verify(transactionManager).commitTransaction();
        verify(transactionManager).lockTable(RegistryAuditStorable.NAME_SPACE);
        verify(transactionManager).unlockTable(RegistryAuditStorable.NAME_SPACE);
        verify(storageManager).find(eq(RegistryAuditStorable.NAME_SPACE), anyList());
    }

    @Test
    public void testInvalidJson() {
        //given
        RegistryAuditStorable auditStorable = createAuditStorableWithId(2L);
        auditStorable.setProcessedData("invalid json data");
        
        //when
        assertThrows(JsonParseException.class, () -> auditProcessor.processAuditEntry(auditStorable));

    }

    @Test
    public void testProcessAuditEntryException() throws Exception {
        //given
        RegistryAuditStorable auditStorable = createAuditStorableWithId(4L);
        AuditEntry auditEntry = new AuditEntry();
        auditEntry.setMethodName("checkCompatibility");
        auditEntry.setParamTypes(Arrays.asList(String.class.getName(), String.class.getName(), String.class.getName()));
        String name = "test";
        auditEntry.setParamValues(Arrays.asList(name, name, name));
        auditStorable.setProcessedData(objectMapper.writeValueAsString(auditEntry));

        audits.add(auditStorable);
        
        //when
        auditProcessor.processAuditEntries();

        //then
        verify(transactionManager).beginTransaction(TransactionIsolation.SERIALIZABLE);
        verify(transactionManager).commitTransaction();
        verify(transactionManager).lockTable(RegistryAuditStorable.NAME_SPACE);
        verify(transactionManager).unlockTable(RegistryAuditStorable.NAME_SPACE);
    }
    
    private RegistryAuditStorable createAuditStorableWithId(Long id) {
        RegistryAuditStorable auditStorable = new RegistryAuditStorable();
        auditStorable.setId(id);
        auditStorable.setFailed(false);
        auditStorable.setProcessed(false);
        auditStorable.setTimestamp(System.currentTimeMillis());
        return auditStorable;
    }
}
