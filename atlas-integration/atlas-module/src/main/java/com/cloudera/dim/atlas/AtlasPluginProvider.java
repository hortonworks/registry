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
package com.cloudera.dim.atlas;

import com.hortonworks.registries.common.AtlasConfiguration;
import com.cloudera.dim.atlas.shim.AtlasPluginFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Map;

/** The AtlasPlugin provides access to AtlasClient via a separate classpath. */
public class AtlasPluginProvider implements Provider<AtlasPlugin> {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasPluginProvider.class);

    private final AtlasPlugin instance;

    @Inject
    public AtlasPluginProvider(@Nullable AtlasConfiguration atlasConfig) {
        this.instance = createSingleton(atlasConfig);
    }

    @Override
    public AtlasPlugin get() {
        return instance;
    }

    @SuppressWarnings("unchecked")
    private AtlasPlugin createSingleton(AtlasConfiguration config) {
        if (config == null || !config.isEnabled()) {
            return new NoopAtlasPlugin();
        }

        final Map<String, Object> configMap = config.asMap();
        if (StringUtils.isNotBlank(config.getCustomClasspathLoader())) {
            try {
                LOG.info("Using custom classpath loader for AtlasPlugin: {}", config.getCustomClasspathLoader());
                Class<AtlasClasspathLoaderFactory> factoryClass = (Class<AtlasClasspathLoaderFactory>) Class.forName(config.getCustomClasspathLoader());
                AtlasClasspathLoaderFactory factory = factoryClass.newInstance();

                factory.configure(configMap);

                return AtlasPluginFactory.create(configMap, factory.create());
            } catch (Throwable t) {
                LOG.error("Failed to create custom classpath loader. Proceeding with the default classpath loader.", t);
            }
        }

        // config contains "urls" parameter which tells the client where to connect
        return AtlasPluginFactory.create(configMap);
    }
}
