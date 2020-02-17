/**
 * Copyright 2018-2019 Cloudera, Inc.
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

package com.hortonworks.registries.cron;

import com.hortonworks.registries.common.RegistryHAConfiguration;
import com.hortonworks.registries.schemaregistry.HAServerNotificationManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

public class RefreshHAServerManagedTask implements Managed {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshHAServerManagedTask.class);

    private RegistryHAConfiguration registryHAConfiguration;
    private RefreshHAServerListTask refreshHAServerListTask;
    private Timer timer = new Timer();

    public RefreshHAServerManagedTask(StorageManager storageManager,
                                      TransactionManager transactionManager,
                                      RegistryHAConfiguration registryHAConfiguration,
                                      HAServerNotificationManager haServerNotificationManager) {
        this.registryHAConfiguration = registryHAConfiguration;
        this.refreshHAServerListTask = new RefreshHAServerListTask(storageManager,
                                                                   transactionManager,
                                                                   registryHAConfiguration,
                                                                   haServerNotificationManager);
    }


    @Override
    public void start() {
        LOG.debug("Kick start timer task to send heartbeat to the database and sync up the latest nodes in HA mode");
        timer.scheduleAtFixedRate(refreshHAServerListTask, 0, registryHAConfiguration.getPeerListRefreshIntervalMs());
    }

    @Override
    public void stop() throws Exception {
        LOG.debug("Shutdown timer task for sending heartbeat to the database");
        timer.cancel();
    }

}
