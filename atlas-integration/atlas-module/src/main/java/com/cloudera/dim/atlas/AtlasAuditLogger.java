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
package com.cloudera.dim.atlas;

import com.cloudera.dim.atlas.audit.AuditProcessor;
import com.cloudera.dim.atlas.audit.DbLoggingSchemaRegistry;
import com.cloudera.dim.atlas.conf.AtlasConfiguration;
import com.cloudera.dim.atlas.conf.IAtlasSchemaRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.authorizer.audit.AuditLogger;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * After a successful operation with SchemaRegistry, we also created an audit entry. The AtlasAuditLogger writes
 * the invoked method and its parameters to a database. Another process is running in parallel, reads from the
 * database and sends the requests to Atlas. This way the model found in RDBMS is also logged in Atlas.
 */
public class AtlasAuditLogger implements AuditLogger {

    private final ExecutorService threadPool = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("audit-processor-%d").build());
    private final IAtlasSchemaRegistry schemaRegistry;
    private final DbLoggingSchemaRegistry dbLoggingSchemaRegistry;

    @Inject
    public AtlasAuditLogger(AtlasConfiguration atlasConfiguration, IAtlasSchemaRegistry schemaRegistry, StorageManager storageManager,
                            TransactionManager transactionManager) {
        this.schemaRegistry = checkNotNull(schemaRegistry, "schemaRegistry");
        this.dbLoggingSchemaRegistry = new DbLoggingSchemaRegistry(checkNotNull(storageManager, "storageManager"));

        if (atlasConfiguration.isEnabled()) {
            threadPool.submit(new AuditProcessor(schemaRegistry, storageManager, transactionManager, atlasConfiguration.getWaitBetweenAuditProcessing()));
        }
    }

    @Override
    public ISchemaRegistry withAuth(Authorizer.UserAndGroups auth) {
        if (auth == null) {
            dbLoggingSchemaRegistry.setUsername(System.getProperty("user.name"));
        } else {
            dbLoggingSchemaRegistry.setUsername(auth.getUser());
        }
        return dbLoggingSchemaRegistry;
    }

}
