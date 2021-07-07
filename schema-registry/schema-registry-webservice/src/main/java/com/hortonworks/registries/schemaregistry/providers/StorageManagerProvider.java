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

import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.common.RegistryConfiguration;

import javax.inject.Inject;
import javax.inject.Provider;

public class StorageManagerProvider implements Provider<StorageManager> {

    private final RegistryConfiguration configuration;

    @Inject
    public StorageManagerProvider(RegistryConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public StorageManager get() {
        StorageProviderConfiguration storageProviderConfiguration = configuration.getStorageProviderConfiguration();
        final String providerClass = storageProviderConfiguration.getProviderClass();
        StorageManager storageManager;
        try {
            storageManager = (StorageManager) Class.forName(providerClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        storageManager.init(storageProviderConfiguration);
        return storageManager;
    }
}
