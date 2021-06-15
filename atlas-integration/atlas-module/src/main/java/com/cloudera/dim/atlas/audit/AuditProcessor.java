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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.RegistryAuditStorable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.transaction.ManagedTransaction;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import com.hortonworks.registries.storage.transaction.functional.ManagedTransactionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * The processor should be run in its own thread. It reads audit entries from the database. The audit
 * entries contain the method that was invoked and also its parameters. AuditProcessor will ensure
 * this method will also get invoked on the SchemaRegistry instance injected in its constructor.
 * By injecting an AtlasSchemaRegistry, we can ensure the methods invoked in the RDBMS-SchemaRegistry
 * will also be invoked on the Atlas model.
 */
public class AuditProcessor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AuditProcessor.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ISchemaRegistry schemaRegistry;
    private final StorageManager storageManager;
    private final TransactionManager transactionManager;
    private final long waitBetweenProcessing;
    private final ManagedTransaction managedTransaction;

    public AuditProcessor(ISchemaRegistry schemaRegistry, StorageManager storageManager, TransactionManager transactionManager,
                          long waitBetweenProcessing) {
        this.schemaRegistry = checkNotNull(schemaRegistry, "schemaRegistry");
        this.storageManager = checkNotNull(storageManager, "storageManager");
        this.transactionManager = checkNotNull(transactionManager, "transactionManager");
        this.waitBetweenProcessing = waitBetweenProcessing;
        checkState(waitBetweenProcessing > 0L, "Wait period must be greater than 0");
        this.managedTransaction = new ManagedTransaction(transactionManager, TransactionIsolation.SERIALIZABLE);
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                processAuditEntries();
                wait(waitBetweenProcessing);
            } catch (InterruptedException iex) {
                LOG.info("Audit processor was interrupted.", iex);
                return;
            } catch (Exception ex) {
                LOG.error("An error occurred while processing audit entries. The AuditProcessor thread will terminate.");
                throw new UndeclaredThrowableException(ex);
            }
        }
    }

    @VisibleForTesting
    void processAuditEntries() throws Exception {
        List<QueryParam> queryParams = Arrays.asList(
                new QueryParam(RegistryAuditStorable.PROCESSED, "false"),
                new QueryParam(RegistryAuditStorable.FAILED, "false"));

        managedTransaction.executeFunction((ManagedTransactionFunction.Arg0<Object>) () -> {

            // TODO CDPD-27215 lock table before running find()

            Collection<RegistryAuditStorable> audits = storageManager.find(RegistryAuditStorable.NAME_SPACE, queryParams);
            if (audits == null || audits.isEmpty()) {
                return false;
            }

            LOG.info("Processing {} audit entries.", audits.size());

            for (RegistryAuditStorable auditStorable : audits) {
                try {
                    processAuditEntry(auditStorable);
                } catch (Exception ex) {
                    LOG.error("Could not process audit entry. Setting it to failed state: {}", auditStorable, ex);
                    setAuditEntryToFailed(auditStorable);
                }
            }

            return true;
        });
    }

    @VisibleForTesting
    void processAuditEntry(RegistryAuditStorable auditStorable) throws Exception {
        AuditEntry auditEntry = objectMapper.readValue(auditStorable.getProcessedData(), AuditEntry.class);
        checkNotNull(auditEntry, "Audit has an empty processed data.");

        String methodName = checkNotNull(auditEntry.getMethodName(), "Method name was empty.");
        List<String> paramTypeNames = auditEntry.getParamTypes();
        List<String> paramValues = auditEntry.getParamValues();

        Class<?>[] parameterTypes = null;
        Object[] args = null;
        if (paramTypeNames != null && !paramTypeNames.isEmpty()) {
            parameterTypes = new Class<?>[paramTypeNames.size()];
            args = new Object[paramTypeNames.size()];
            for (int i = 0; i < paramTypeNames.size(); i++) {
                Class<?> paramType = Class.forName(paramTypeNames.get(i));
                parameterTypes[i] = paramType;
                String value = paramValues.get(i);
                if (value != null) {
                    args[i] = objectMapper.readValue(value, paramType);
                }
            }
        }

        Method method = ISchemaRegistry.class.getMethod(methodName, parameterTypes);
        try {
            method.invoke(schemaRegistry, args);
        } catch (InvocationTargetException inex) {
            throw (Exception) inex.getTargetException();
        }

        auditStorable.setProcessed(true);
        storageManager.update(auditStorable);
    }

    private void setAuditEntryToFailed(RegistryAuditStorable auditStorable) {
        if (auditStorable == null) {
            return;
        }

        try {
            auditStorable.setFailed(true);
            storageManager.update(auditStorable);
        } catch (Exception ex) {
            LOG.error("Failed to set state to 'failed' for audit entry {}", auditStorable, ex);
        }
    }

}
