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
package com.hortonworks.registries.schemaregistry.webservice;

import com.google.inject.AbstractModule;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.providers.FileStorageProvider;
import com.hortonworks.registries.schemaregistry.providers.StorageManagerProvider;
import com.hortonworks.registries.schemaregistry.providers.TxManagerProvider;
import com.hortonworks.registries.schemaregistry.webservice.validator.JarInputStreamValidator;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.transaction.TransactionEventListener;

import javax.inject.Singleton;

/** Set up beans which are always required. */
public class CoreModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(FileStorage.class).toProvider(FileStorageProvider.class).in(Singleton.class);
        bind(JarInputStreamValidator.class).in(Singleton.class);
        bind(StorageManager.class).toProvider(StorageManagerProvider.class).in(Singleton.class);
        bind(TransactionManager.class).toProvider(TxManagerProvider.class).in(Singleton.class);
        bind(TransactionEventListener.class).asEagerSingleton();
    }
}
