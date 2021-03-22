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

import com.cloudera.dim.atlas.AtlasRestResource;
import com.cloudera.dim.atlas.AtlasSchemaRegistry;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.webservice.RegistryConfiguration;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * We only want to expose the SR Atlas REST interface if Atlas is enabled. For this
 * reason we are using a Provider, which returns a REST endpoint if Atlas is enabled.
 * If it is not enabled, we'll return a <i>null</i> (this is acceptable since
 * Dropwizard ignores it in this case).
 */
public class AtlasRestResourceProvider implements Provider<AtlasRestResource> {

    private final RegistryConfiguration configuration;
    private final ISchemaRegistry schemaRegistry;
    private final AuthorizationAgent authorizationAgent;
    private final SchemaMetadataTypeValidator schemaMetadataTypeValidator;

    @Inject
    public AtlasRestResourceProvider(RegistryConfiguration configuration, ISchemaRegistry schemaRegistry,
                                     AuthorizationAgent authorizationAgent,
                                     SchemaMetadataTypeValidator schemaMetadataTypeValidator) {

        this.configuration = configuration;
        this.schemaRegistry = schemaRegistry;
        this.authorizationAgent = authorizationAgent;
        this.schemaMetadataTypeValidator = schemaMetadataTypeValidator;
    }

    @Override
    public AtlasRestResource get() {
        if (configuration.getAtlasConfiguration() != null && configuration.getAtlasConfiguration().isEnabled()) {
            AtlasSchemaRegistry atlasSchemaRegistry = (AtlasSchemaRegistry) schemaRegistry;
            return new AtlasRestResource(atlasSchemaRegistry, authorizationAgent, schemaMetadataTypeValidator);
        } else {
            return null;
        }


    }

}
