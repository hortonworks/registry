/**
 * Copyright 2016-2019 Cloudera, Inc.
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

package com.hortonworks.registries.schemaregistry.locks;

import com.google.common.base.Preconditions;
import com.hortonworks.registries.storage.TransactionManager;

public class SchemaLockManager {

    private TransactionManager transactionManager;

    public SchemaLockManager(TransactionManager transactionManager) {
        Preconditions.checkNotNull(transactionManager,"Transaction manager can't be null");
        this.transactionManager = transactionManager;
    }

    public Lock getReadLock(String lockName) {
        return new ReadLock(lockName, transactionManager);
    }

    public Lock getWriteLock(String lockName) {
        return new WriteLock(lockName, transactionManager);
    }
}
