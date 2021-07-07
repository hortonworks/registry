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

import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.common.RegistryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.google.common.base.Preconditions.checkNotNull;

/** Reads the configuration file and provides a FileStorage based on the given class name. */
public class FileStorageProvider implements Provider<FileStorage> {

    private static final Logger LOG = LoggerFactory.getLogger(FileStorageProvider.class);

    private final RegistryConfiguration configuration;

    @Inject
    public FileStorageProvider(RegistryConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public FileStorage get() {
        try {
            Class<? extends FileStorage> clazz = getJarStorageClass(configuration.getFileStorageConfiguration());
            return clazz.getDeclaredConstructor(FileStorageConfiguration.class).newInstance(configuration.getFileStorageConfiguration());
        } catch (Exception ex) {
            LOG.error("Could not initialize file storage, with configuration {}", configuration.getFileStorageConfiguration(), ex);
            throw new RuntimeException("Could not initialize file storage.");
        }
    }

    @SuppressWarnings("unchecked")
    private Class<? extends FileStorage> getJarStorageClass(FileStorageConfiguration fileStorageConfiguration)
            throws ClassNotFoundException {

        checkNotNull(fileStorageConfiguration.getClassName(), "FileStorage class name must be declared.");
        return (Class<? extends FileStorage>) Class.forName(fileStorageConfiguration.getClassName(),
                    true, Thread.currentThread().getContextClassLoader());
    }
}
