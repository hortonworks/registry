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
package com.cloudera.dim.atlas.conf;

import com.cloudera.dim.atlas.AtlasSchemaRegistry;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.io.File;
import java.util.Arrays;
import java.util.Optional;

public class AtlasSchemaRegistryProvider implements Provider<IAtlasSchemaRegistry> {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasSchemaRegistryProvider.class);

    private final AtlasConfiguration configuration;
    private final ModuleDetailsConfiguration moduleDetailsConfiguration;
    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final SchemaLockManager schemaLockManager;

    @Inject
    public AtlasSchemaRegistryProvider(AtlasConfiguration configuration,
                                  ModuleDetailsConfiguration moduleDetailsConfiguration,
                                  StorageManager storageManager,
                                  FileStorage fileStorage,
                                  SchemaLockManager schemaLockManager) {
        this.configuration = configuration;
        this.moduleDetailsConfiguration = moduleDetailsConfiguration;
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
        this.schemaLockManager = schemaLockManager;
    }

    @Override
    public IAtlasSchemaRegistry get() {
        if (configuration != null && configuration.isEnabled()) {
            return atlasSchemaRegistry();
        } else {
            return noopSchemaRegistry();
        }
    }

    private IAtlasSchemaRegistry noopSchemaRegistry() {
        return new NOOPSchemaRegistry();
    }

    private AtlasSchemaRegistry atlasSchemaRegistry() {
        LOG.info("Configuring {}", AtlasSchemaRegistry.class);

        if (System.getProperty("atlas.conf") == null) {
            final Optional<String> fallbackAtlasConf = findAtlasConf();
            if (fallbackAtlasConf.isPresent()) {
                LOG.warn("Environment variable \"atlas.conf\" was not defined, trying to fallback to {}", fallbackAtlasConf.get());
                System.setProperty("atlas.conf", fallbackAtlasConf.get());
            } else {
                LOG.error("Environment variable \"atlas.conf\" was not defined and we couldn't find an Atlas configuration path on the classpath.");
            }
        }

        return new AtlasSchemaRegistry(moduleDetailsConfiguration, configuration, fileStorage);
    }

    private Optional<String> findAtlasConf() {
        for (String attempt : Arrays.asList("./conf", "../conf", "build/conf", "./atlas", ".")) {
            File dir = new File(attempt);
            if (dir.exists() && dir.isDirectory() && new File(dir, "atlas-application.properties").exists()) {
                return Optional.of(dir.getAbsolutePath());
            }
        }

        return Optional.empty();
    }
}
