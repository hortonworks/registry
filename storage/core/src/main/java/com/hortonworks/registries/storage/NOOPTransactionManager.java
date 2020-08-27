/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.storage;

import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import com.hortonworks.registries.storage.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class NOOPTransactionManager implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger(NOOPTransactionManager.class);

    @Override
    public void beginTransaction(TransactionIsolation transactionIsolationLevel) {
        LOG.debug("--- Ignore call to begin transaction for thread id : {} ---", Thread.currentThread().getId());
    }

    @Override
    public void rollbackTransaction() {
        LOG.debug("--- Ignore call to rollback transaction for thread id : {} ---", Thread.currentThread().getId());
    }

    @Override
    public void commitTransaction() {
        LOG.debug("--- Ignore call to commit transaction for thread id : {} ---", Thread.currentThread().getId());
    }

    @Override
    public boolean readLock(StorableKey key, Long time, TimeUnit timeUnit) throws StorageException {
        return true;
    }

    @Override
    public boolean writeLock(StorableKey key, Long time, TimeUnit timeUnit) throws StorageException {
        return true;
    }

}
