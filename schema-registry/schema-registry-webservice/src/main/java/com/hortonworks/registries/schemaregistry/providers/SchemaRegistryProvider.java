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

import com.hortonworks.registries.common.CompatibilityConfig;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.common.RegistryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;

public class SchemaRegistryProvider implements Provider<ISchemaRegistry> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryProvider.class);

    private final RegistryConfiguration configuration;
    private final ModuleDetailsConfiguration moduleDetailsConfiguration;
    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final SchemaLockManager schemaLockManager;
    private final CompatibilityConfig compatibilityConfig;

    @Inject
    public SchemaRegistryProvider(RegistryConfiguration configuration,
                                  ModuleDetailsConfiguration moduleDetailsConfiguration,
                                  StorageManager storageManager,
                                  FileStorage fileStorage,
                                  SchemaLockManager schemaLockManager) {
        this.configuration = configuration;
        this.moduleDetailsConfiguration = moduleDetailsConfiguration;
        this.compatibilityConfig = configuration.getCompatibility();
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaLockManager = schemaLockManager;
    }

    @Override
    public ISchemaRegistry get() {
            return defaultSchemaRegistry();
    }
    

    private DefaultSchemaRegistry defaultSchemaRegistry() {
        LOG.info("Configuring {}", DefaultSchemaRegistry.class);

        return new DefaultSchemaRegistry(moduleDetailsConfiguration, storageManager, fileStorage,
                moduleDetailsConfiguration.getSchemaProviders(), schemaLockManager, compatibilityConfig);
    }



}
