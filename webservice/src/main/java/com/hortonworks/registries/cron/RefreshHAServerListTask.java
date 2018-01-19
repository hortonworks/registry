package com.hortonworks.registries.cron;

import com.hortonworks.registries.common.transaction.TransactionIsolation;
import com.hortonworks.registries.schemaregistry.HAServerConfigManager;
import com.hortonworks.registries.schemaregistry.HostConfigStorable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;

import java.util.Collection;
import java.util.TimerTask;

public class RefreshHAServerListTask extends TimerTask {

    private StorageManager storageManager;
    private TransactionManager transactionManager;
    private HAServerConfigManager haServerConfigManager;

    /**
     * Create a new task with the given name.
     *
     */
    public RefreshHAServerListTask(StorageManager storageManager, TransactionManager transactionManager, HAServerConfigManager haServerConfigManager) {
        this.storageManager = storageManager;
        this.transactionManager = transactionManager;
        this.haServerConfigManager = haServerConfigManager;
    }

    @Override
    public void run() {
        try {
            transactionManager.beginTransaction(TransactionIsolation.SERIALIZABLE);
            Collection<HostConfigStorable> hostConfigStorables = storageManager.<HostConfigStorable>list(HostConfigStorable.NAME_SPACE);
            haServerConfigManager.refresh(hostConfigStorables);
            transactionManager.commitTransaction();
        } catch (Exception e) {
            transactionManager.rollbackTransaction();
            throw e;
        }
    }
}
