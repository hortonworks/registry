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
package com.hortonworks.registries.webservice.healthchecks;

import com.codahale.metrics.health.HealthCheck;
import com.hortonworks.registries.common.ModuleConfiguration;
import com.hortonworks.registries.webservice.RegistryConfiguration;

/**
 * The application can have one or more modules, which may be enabled/disabled. We need
 * to ensure there is at least one module which is enabled.
 */
public class ModulesHealthCheck extends HealthCheck {

    private final RegistryConfiguration configuration;

    public ModulesHealthCheck(RegistryConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    protected Result check() throws Exception {
        if (configuration.getModules().isEmpty() ||
                configuration.getModules().stream().noneMatch(ModuleConfiguration::isEnabled)) {
            return Result.unhealthy("There are no enabled modules!");
        }
        return Result.healthy();
    }

}
