/**
 * Copyright 2018 Hortonworks.
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

import com.hortonworks.registries.common.transaction.TransactionIsolation;
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

    private StorageManager storageManager;
    private TransactionManager transactionManager;
    private HAServerNotificationManager haServerNotificationManager;
    private static final Logger LOG = LoggerFactory.getLogger(RefreshHAServerListTask.class);

    public RefreshHAServerListTask(StorageManager storageManager, TransactionManager transactionManager, HAServerNotificationManager haServerNotificationManager) {
        this.storageManager = storageManager;
        this.transactionManager = transactionManager;
        this.haServerNotificationManager = haServerNotificationManager;
    }

    @Override
    public void run() {
        try {
            transactionManager.beginTransaction(TransactionIsolation.SERIALIZABLE);
            Collection<HostConfigStorable> hostConfigStorables = storageManager.<HostConfigStorable>list(HostConfigStorable.NAME_SPACE);
            List<HostConfigStorable> hostConfigWithoutStaleEntries = hostConfigStorables.stream().filter(hostConfigStorable -> {
                if( (System.currentTimeMillis() - hostConfigStorable.getTimestamp()) > 600000) {
                    LOG.debug("Removing : {} from list of available servers", hostConfigStorable.getHostUrl());
                    storageManager.remove(hostConfigStorable.getStorableKey());
                    return false;
                } else {
                    return true;
                }
            }).collect(Collectors.toList());
            LOG.debug("Fetched : {} as the latest available servers", Arrays.toString(hostConfigWithoutStaleEntries.toArray()));
            haServerNotificationManager.refreshServerInfo(hostConfigWithoutStaleEntries);
            HostConfigStorable hostConfWithUpdatedTimeStamp = storageManager.get(new HostConfigStorable(haServerNotificationManager.getHomeNodeURL()).getStorableKey());
            if (hostConfWithUpdatedTimeStamp == null) {
                hostConfWithUpdatedTimeStamp = new HostConfigStorable(storageManager.nextId(HostConfigStorable.NAME_SPACE),
                        haServerNotificationManager.getHomeNodeURL(),
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
