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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable;
import com.hortonworks.registries.schemaregistry.AtlasEventStorable.EventType;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AtlasEventLogger implements AutoCloseable {

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("audit-processor-%d")
                    .build()
    );
    private final StorageManager storageManager;
    private String username = System.getProperty("user.name");

    @Inject
    public AtlasEventLogger(@Nullable AtlasConfiguration atlasConfiguration, AtlasPlugin atlasPlugin,
                            StorageManager storageManager, TransactionManager transactionManager) {

        this.storageManager = storageManager;

        if (atlasConfiguration != null && atlasConfiguration.isEnabled()) {
            threadPool.submit(new AtlasEventsProcessor(atlasPlugin, storageManager, transactionManager,
                    atlasConfiguration.getWaitBetweenAuditProcessing(), atlasConfiguration.isConnectWithKafka()));
        }
    }

    public void createMeta(Long schemaId) {
        createAudit(schemaId, EventType.CREATE_META);
    }

    public void updateMeta(Long id) {
        createAudit(id, EventType.UPDATE_META);
    }

    public void createVersion(Long schemaVersionId) {
        createAudit(schemaVersionId, EventType.CREATE_VERSION);
    }

    private void createAudit(Long id, EventType eventType) {
        AtlasEventStorable audit = new AtlasEventStorable();
        audit.setId(storageManager.nextId(AtlasEventStorable.NAME_SPACE));
        audit.setProcessedId(id);
        audit.setType(eventType);
        audit.setProcessed(false);
        audit.setFailed(false);
        audit.setUsername(username);
        audit.setTimestamp(System.currentTimeMillis());
        storageManager.add(audit);
    }

    public AtlasEventLogger withAuth(@Nullable Authorizer.UserAndGroups auth) {
        if (auth == null || auth.getUser() == null) {
            this.username = System.getProperty("user.name");
        } else {
            this.username = auth.getUser();
        }
        return this;
    }

    @Override
    public void close() {
        threadPool.shutdownNow();
    }
}
