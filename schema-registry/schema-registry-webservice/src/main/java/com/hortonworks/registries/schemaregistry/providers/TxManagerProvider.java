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
package com.hortonworks.registries.schemaregistry.providers;

import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;

import javax.inject.Inject;
import javax.inject.Provider;

public class TxManagerProvider implements Provider<TransactionManager> {

    private final StorageManager storageManager;

    @Inject
    public TxManagerProvider(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public TransactionManager get() {
        if (storageManager instanceof TransactionManager) {
            return (TransactionManager) storageManager;
        } else {
            return new NOOPTransactionManager();
        }
    }
}
