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

import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.common.RegistryConfiguration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

public class AtlasConfigurationProvider implements Provider<AtlasConfiguration> {
    private final RegistryConfiguration configuration;
    
    @Inject
    public AtlasConfigurationProvider(RegistryConfiguration registryConfiguration) {
        this.configuration = registryConfiguration;
    }

    @Nullable
    @Override
    public AtlasConfiguration get() {
        return configuration.getAtlasConfiguration();
    }
}
