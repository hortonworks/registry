/**
 * Copyright 2018-2019 Cloudera, Inc.
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

package com.hortonworks.registries.cron;

import com.hortonworks.registries.common.RegistryHAConfiguration;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import com.hortonworks.registries.schemaregistry.HAServerNotificationManager;
import com.hortonworks.registries.schemaregistry.HostConfigStorable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.stream.Collectors;

public class RefreshHAServerListTask extends TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshHAServerListTask.class);

    private StorageManager storageManager;
    private TransactionManager transactionManager;
    private HAServerNotificationManager haServerNotificationManager;

    private static final long DEFAULT_PEER_ENTRY_EXPIRY_TIME_MS = 600000L;
    private long peerEntryExpiryTimeMs;

    public RefreshHAServerListTask(StorageManager storageManager,
                                   TransactionManager transactionManager,
                                   RegistryHAConfiguration registryHAConfiguration,
                                   HAServerNotificationManager haServerNotificationManager) {
        this.storageManager = storageManager;
        this.transactionManager = transactionManager;
        this.peerEntryExpiryTimeMs = registryHAConfiguration == null ? DEFAULT_PEER_ENTRY_EXPIRY_TIME_MS : registryHAConfiguration.getPeerEntryExpiryTimeMs();
        this.haServerNotificationManager = haServerNotificationManager;
    }

    @Override
    public void run() {
        try {
            transactionManager.beginTransaction(TransactionIsolation.READ_COMMITTED);
            Collection<HostConfigStorable> hostConfigStorables = storageManager.<HostConfigStorable>list(HostConfigStorable.NAME_SPACE);
            List<HostConfigStorable> hostConfigWithoutStaleEntries = hostConfigStorables.stream().filter(hostConfigStorable -> {
                if ((System.currentTimeMillis() - hostConfigStorable.getTimestamp()) > peerEntryExpiryTimeMs) {
                    LOG.debug("Removing : {} from list of available servers", hostConfigStorable.getHostUrl());
                    storageManager.remove(hostConfigStorable.getStorableKey());
                    return false;
                } else {
                    return true;
                }
            }).collect(Collectors.toList());
            LOG.debug("Fetched : {} as the latest available servers", Arrays.toString(hostConfigWithoutStaleEntries.toArray()));
            haServerNotificationManager.updatePeerServerURLs(hostConfigWithoutStaleEntries);
            HostConfigStorable hostConfWithUpdatedTimeStamp = storageManager.get(new HostConfigStorable(haServerNotificationManager.getServerURL()).getStorableKey());
            if (hostConfWithUpdatedTimeStamp == null) {
                hostConfWithUpdatedTimeStamp = new HostConfigStorable(storageManager.nextId(HostConfigStorable.NAME_SPACE),
                        haServerNotificationManager.getServerURL(),
                        System.currentTimeMillis());
            } else {
                hostConfWithUpdatedTimeStamp.setTimestamp(System.currentTimeMillis());
            }
            storageManager.addOrUpdate(hostConfWithUpdatedTimeStamp);
            transactionManager.commitTransaction();
        } catch (Exception e) {
            transactionManager.rollbackTransaction();
            throw e;
        }
    }
}
