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
package com.hortonworks.registries.schemaregistry.webservice;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.hortonworks.registries.common.ModuleRegistration;
import com.hortonworks.registries.common.SchemaRegistryServiceInfo;
import com.hortonworks.registries.common.SchemaRegistryVersion;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgentFactory;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.schemaregistry.webservice.validator.JarInputStreamValidator;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageManagerAware;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.TransactionManagerAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.ISchemaRegistry.AUTHORIZATION;
import static com.hortonworks.registries.schemaregistry.ISchemaRegistry.SCHEMA_PROVIDERS;

/**
 *
 */
public class SchemaRegistryModule implements ModuleRegistration, StorageManagerAware, TransactionManagerAware {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryModule.class);

    private Map<String, Object> config;
    private FileStorage fileStorage;
    private StorageManager storageManager;
    private TransactionManager transactionManager;

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
        Collection<Map<String, Object>> schemaProviders = (Collection<Map<String, Object>>) config.get(SCHEMA_PROVIDERS);
        DefaultSchemaRegistry schemaRegistry = new DefaultSchemaRegistry(storageManager,
                                                                         fileStorage,
                                                                         schemaProviders,
                                                                         new SchemaLockManager(transactionManager));
        schemaRegistry.init(config);
        SchemaRegistryVersion schemaRegistryVersion = SchemaRegistryServiceInfo.get().version();

        Map<String, Object> authorizationProps = (Map<String, Object>) config.get(AUTHORIZATION);
        AuthorizationAgent authorizationAgent = AuthorizationAgentFactory.getAuthorizationAgent(authorizationProps);

        SchemaMetadataTypeValidator schemaMetadataTypeValidator = new SchemaMetadataTypeValidator(schemaRegistry);
        SchemaRegistryResource schemaRegistryResource = new SchemaRegistryResource(schemaRegistry,
                                                                                   schemaRegistryVersion,
                                                                                   authorizationAgent,
                                                                                   new JarInputStreamValidator(),
                                                                                    schemaMetadataTypeValidator);
        ConfluentSchemaRegistryCompatibleResource
            confluentSchemaRegistryResource = new ConfluentSchemaRegistryCompatibleResource(schemaRegistry,
                authorizationAgent);

        return Arrays.asList(schemaRegistryResource, confluentSchemaRegistryResource);
    }

    private Collection<? extends SchemaProvider> getSchemaProviders() {
        Collection<Map<String, Object>> schemaProviders = (Collection<Map<String, Object>>) config.get(SCHEMA_PROVIDERS);
        if (schemaProviders == null || schemaProviders.isEmpty()) {
            throw new IllegalArgumentException("No [" + SCHEMA_PROVIDERS + "] property is configured in schema registry configuration file.");
        }

        return Collections2.transform(schemaProviders, new Function<Map<String, Object>, SchemaProvider>() {
            @Nullable
            @Override
            public SchemaProvider apply(@Nullable Map<String, Object> schemaProviderConfig) {
                String className = (String) schemaProviderConfig.get("providerClass");
                if (className == null || className.isEmpty()) {
                    throw new IllegalArgumentException("Schema provider class name must be non empty, Invalid provider class name [" + className + "]");
                }
                try {
                    SchemaProvider schemaProvider = (SchemaProvider) Class.forName(className, true, Thread.currentThread().getContextClassLoader()).newInstance();
                    schemaProvider.init(schemaProviderConfig);
                    return schemaProvider;
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    LOG.error("Error encountered while loading SchemaProvider [{}] ", className, e);
                    throw new IllegalArgumentException(e);
                }
            }
        });
    }

    @Override
    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }
}
