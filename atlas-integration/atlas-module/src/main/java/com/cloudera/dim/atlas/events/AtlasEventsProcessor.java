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
package com.cloudera.dim.atlas.events;

import com.cloudera.dim.atlas.AtlasPlugin;
import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable.EventType;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionStorable;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.transaction.ManagedTransaction;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import com.hortonworks.registries.storage.transaction.functional.ManagedTransactionFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
public class AtlasEventsProcessor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasEventsProcessor.class);

    private final AtlasPlugin atlasPlugin;
    private final StorageManager storageManager;
    private final long waitBetweenProcessing;
    private final boolean connectWithKafka;
    private final ManagedTransaction managedTransaction;

    public AtlasEventsProcessor(AtlasPlugin atlasPlugin, StorageManager storageManager, TransactionManager transactionManager,
                                long waitBetweenProcessing, boolean connectWithKafka) {
        this.atlasPlugin = checkNotNull(atlasPlugin, "atlasPlugin");
        this.storageManager = checkNotNull(storageManager, "storageManager");
        checkNotNull(transactionManager, "transactionManager");
        this.connectWithKafka = connectWithKafka;
        this.waitBetweenProcessing = waitBetweenProcessing;
        checkState(waitBetweenProcessing > 0L, "Wait period must be greater than 0");
        this.managedTransaction = new ManagedTransaction(transactionManager, TransactionIsolation.SERIALIZABLE);

        LOG.info("Connecting schemas with kafka topics is {}", connectWithKafka ? "ENABLED" : "DISABLED");
    }

    @Override
    public synchronized void run() {
        LOG.trace("Pre-sleep of AtlasEvents processor");
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException iex) {
            LOG.warn("Interrupted while performing initial sleep.", iex);
        }
        LOG.debug("Starting Atlas events processor.");
        while (!Thread.interrupted()) {
            try {
                processAtlasEvents();
                wait(waitBetweenProcessing);
            } catch (InterruptedException iex) {
                LOG.warn("Atlas events processor was interrupted.", iex);
                return;
            } catch (Exception ex) {
                LOG.error("An error occurred while processing Atlas events. The AtlasEventsProcessor thread will terminate.", ex);
                throw new UndeclaredThrowableException(ex);
            }
        }
    }

    @VisibleForTesting
    void processAtlasEvents() throws Exception {
        List<QueryParam> queryParams = Arrays.asList(
                new QueryParam(AtlasEventStorable.PROCESSED, "false"),
                new QueryParam(AtlasEventStorable.FAILED, "false"));

        managedTransaction.executeFunction((ManagedTransactionFunction.Arg0<Object>) () -> {

            Collection<AtlasEventStorable> atlasEvents = storageManager.find(AtlasEventStorable.NAME_SPACE, queryParams);

            if (atlasEvents == null || atlasEvents.isEmpty()) {
                return false;
            }

            LOG.info("Processing {} Atlas events.", atlasEvents.size());

            for (AtlasEventStorable atlasEvent : atlasEvents) {
                try {
                    processAtlasEvent(atlasEvent);
                } catch (Exception ex) {
                    LOG.error("Could not process Atlas event. Setting it to failed state: {}", atlasEvent, ex);
                    setAtlasEntryToFailed(atlasEvent);
                }
            }

            return true;
        });
    }

    @VisibleForTesting
    void processAtlasEvent(AtlasEventStorable atlasEventStorable) throws Exception {
        checkNotNull(atlasEventStorable.getProcessedId(), "Atlas event ID is null.");
        checkNotNull(atlasEventStorable.getType(), "Atlas event type is null.");

        switch (EventType.forNumValue(atlasEventStorable.getType())) {
            case CREATE_META:
                createMeta(atlasEventStorable.getProcessedId());
                break;
            case UPDATE_META:
                updateMeta(atlasEventStorable.getProcessedId());
                break;
            case CREATE_VERSION:
                createVersion(atlasEventStorable.getProcessedId());
                break;
            default:
                throw new IllegalArgumentException("Unsupported event type: " + atlasEventStorable);
        }

        atlasEventStorable.setProcessed(true);
        storageManager.update(atlasEventStorable);
    }

    private void createMeta(Long schemaMetadataId) throws SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getMetaById(schemaMetadataId);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Did not find schema with ID " + schemaMetadataId);
        }

        String metaGuid = atlasPlugin.createMeta(schemaMetadataInfo);
        if (metaGuid != null && isConnectWithKafka()) {
            connectSchemaWithTopic(metaGuid, schemaMetadataInfo);
        }
    }

    private void connectSchemaWithTopic(String metaGuid, SchemaMetadataInfo schemaMetadataInfo) {
        LOG.debug("Connect schema with Kafka topic");
        if (atlasPlugin.isKafkaSchemaModelInitialized()) {
            atlasPlugin.connectSchemaWithTopic(metaGuid, schemaMetadataInfo);
        }
    }

    private void updateMeta(Long schemaMetadataId) throws SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getMetaById(schemaMetadataId);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Did not find schema with ID " + schemaMetadataId);
        }

        atlasPlugin.updateMeta(schemaMetadataInfo.getSchemaMetadata());
    }

    private void createVersion(Long versionId) throws SchemaNotFoundException {
        SchemaVersionInfo persistedVersion = getVersionById(versionId);
        if (persistedVersion == null) {
            throw new SchemaNotFoundException("Did not find schema version with ID " + versionId);
        }

        SchemaMetadataInfo persistedMeta = getMetaById(persistedVersion.getSchemaMetadataId());
        if (persistedMeta == null) {
            throw new SchemaNotFoundException("Did not find schema with ID " + persistedVersion.getSchemaMetadataId() + " for version with ID " + versionId);
        }

        atlasPlugin.addSchemaVersion(persistedMeta.getSchemaMetadata().getName(), persistedVersion);
    }

    private void setAtlasEntryToFailed(AtlasEventStorable atlasEventStorable) {
        if (atlasEventStorable == null) {
            return;
        }

        try {
            atlasEventStorable.setFailed(true);
            storageManager.update(atlasEventStorable);
        } catch (Exception ex) {
            LOG.error("Failed to set state to 'failed' for Atlas event entry {}", atlasEventStorable, ex);
        }
    }

    @Nullable
    private SchemaMetadataInfo getMetaById(@Nonnull Long schemaMetadataId) {
        SchemaMetadataStorable givenSchemaMetadataStorable = new SchemaMetadataStorable();
        givenSchemaMetadataStorable.setId(schemaMetadataId);

        List<QueryParam> params = Collections.singletonList(new QueryParam(SchemaMetadataStorable.ID, schemaMetadataId.toString()));
        Collection<SchemaMetadataStorable> schemaMetadataStorables = storageManager.find(SchemaMetadataStorable.NAME_SPACE, params);
        SchemaMetadataInfo schemaMetadataInfo = null;
        if (schemaMetadataStorables != null && !schemaMetadataStorables.isEmpty()) {
            schemaMetadataInfo = schemaMetadataStorables.iterator().next().toSchemaMetadataInfo();
            if (schemaMetadataStorables.size() > 1) {
                LOG.warn("No unique entry with schemaMetatadataId: [{}]", schemaMetadataId);
            }
        }
        return schemaMetadataInfo;
    }

    @Nullable
    private SchemaVersionInfo getVersionById(@Nonnull Long versionId) {
        SchemaVersionStorable givenVersionStorable = new SchemaVersionStorable();
        givenVersionStorable.setId(versionId);

        SchemaVersionStorable storable = storageManager.get(givenVersionStorable.getStorableKey());
        if (storable != null) {
            return storable.toSchemaVersionInfo();
        }

        return null;
    }

    private boolean isConnectWithKafka() {
        return connectWithKafka;
    }
}
