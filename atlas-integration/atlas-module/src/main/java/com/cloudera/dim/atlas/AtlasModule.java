/**
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.cloudera.dim.atlas;

import com.hortonworks.registries.common.ModuleRegistration;
import com.hortonworks.registries.common.SchemaRegistryServiceInfo;
import com.hortonworks.registries.common.SchemaRegistryVersion;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgentFactory;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryResource;
import com.hortonworks.registries.schemaregistry.webservice.validator.JarInputStreamValidator;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hortonworks.registries.schemaregistry.ISchemaRegistry.AUTHORIZATION;
import static com.hortonworks.registries.schemaregistry.ISchemaRegistry.SCHEMA_PROVIDERS;

/**
 * The Atlas module is plugged into the application via the Dropwizard framework.
 * Adding this module to the application enables persistence via Atlas.
 *
 * @see registry.yaml
 */
public class AtlasModule implements ModuleRegistration {

    private Map<String, Object> config;
    private FileStorage fileStorage;

    @Override
    public void init(Map<String, Object> config, FileStorage fileStorage) {
        this.config = config;
        this.fileStorage = fileStorage;

        // fail fast if the config file is bad
        checkNotNull(config.get(AtlasPlugin.ATLAS_HOSTS_PARAM), "List of Atlas hosts was not provided in the configuration file.");
    }

    @Override
    public List<Object> getResources() {
        Collection<Map<String, Object>> schemaProviders = (Collection<Map<String, Object>>) config.get(SCHEMA_PROVIDERS);
        /*
        DefaultSchemaRegistry schemaRegistry = new DefaultSchemaRegistry(storageManager,
                                                                         fileStorage,
                                                                         schemaProviders,
                                                                         haServerNotificationManager,
                                                                         new SchemaLockManager(transactionManager));
         */
        AtlasSchemaRegistry schemaRegistry = new AtlasSchemaRegistry(fileStorage, schemaProviders);
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
        AtlasRestResource atlasRestResource = new AtlasRestResource(schemaRegistry);

        return Arrays.asList(schemaRegistryResource, confluentSchemaRegistryResource, atlasRestResource);
    }

}
