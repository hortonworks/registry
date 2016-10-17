/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.webservice;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.hortonworks.registries.common.ModuleRegistration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageManagerAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class SchemaRegistryModule implements ModuleRegistration, StorageManagerAware {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryModule.class);
    public static final String SCHEMA_PROVIDER_CLASSES = "schemaProviderClasses";

    private Map<String, Object> config;
    private FileStorage fileStorage;
    private StorageManager storageManager;

    @Override
    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void init(Map<String, Object> config, FileStorage fileStorage) {
        this.config = config;
        this.fileStorage = fileStorage;
    }

    @Override
    public List<Object> getResources() {
        Collection<? extends SchemaProvider> schemaProviders = getSchemaProviders();
        DefaultSchemaRegistry schemaRegistry = new DefaultSchemaRegistry(storageManager, fileStorage, schemaProviders);
        schemaRegistry.init(config);
        SchemaRegistryResource schemaRegistryResource = new SchemaRegistryResource(schemaRegistry);
        return Collections.<Object>singletonList(schemaRegistryResource);
    }

    private Collection<? extends SchemaProvider> getSchemaProviders() {
        Collection<String> schemaProviderClassNames = (Collection<String>) config.get(SCHEMA_PROVIDER_CLASSES);
        if (schemaProviderClassNames == null || schemaProviderClassNames.isEmpty()) {
            throw new IllegalArgumentException("No " + SCHEMA_PROVIDER_CLASSES + " property is configured");
        }

        return Collections2.transform(schemaProviderClassNames, new Function<String, SchemaProvider>() {
            @Nullable
            @Override
            public SchemaProvider apply(@Nullable String className) {
                try {
                    return (SchemaProvider) Class.forName(className, true, Thread.currentThread().getContextClassLoader()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    LOG.error("Error encountered while loading SchemaProvider [{}] ", className, e);
                    throw new IllegalArgumentException(e);
                }
            }
        });
    }

}
