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

import com.hortonworks.registries.common.ModuleConfiguration;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryModule;
import com.hortonworks.registries.webservice.RegistryConfiguration;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Optional;

public class ModuleDetailsConfigurationProvider implements Provider<ModuleDetailsConfiguration> {

    private final RegistryConfiguration configuration;

    @Inject
    public ModuleDetailsConfigurationProvider(RegistryConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public ModuleDetailsConfiguration get() {
        // TODO CDPD-23287 refactor the modules list, so SR is not in the list anymore
        Optional<ModuleConfiguration> configurationOpt = configuration.getModules().stream()
                .filter(module -> module.getClassName().equalsIgnoreCase(SchemaRegistryModule.class.getCanonicalName()))
                .findFirst();

        if (!configurationOpt.isPresent()) {
            throw new RuntimeException("Schema Registry is not configured.");
        }

        return configurationOpt.get().getConfig();
    }
}
