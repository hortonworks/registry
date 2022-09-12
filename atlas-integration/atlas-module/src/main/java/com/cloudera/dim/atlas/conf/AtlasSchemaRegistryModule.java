/**
 * Copyright 2016-2022 Cloudera, Inc.
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

import com.cloudera.dim.atlas.AtlasPlugin;
import com.cloudera.dim.atlas.NoopAtlasPlugin;
import com.cloudera.dim.atlas.bootstrap.AtlasModelBootstrap;
import com.cloudera.dim.atlas.events.AtlasEventLogger;
import com.hortonworks.registries.common.AtlasConfiguration;
import com.hortonworks.registries.common.RegistryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.support.DropwizardAwareModule;

import javax.inject.Provider;
import javax.inject.Singleton;

public class AtlasSchemaRegistryModule extends DropwizardAwareModule<RegistryConfiguration> {

    private static final String DEFAULT_ATLAS_PLUGIN_PROVIDER = "com.cloudera.dim.atlas.AtlasPluginProvider";

    private static final Logger LOG = LoggerFactory.getLogger(AtlasSchemaRegistryModule.class);

    @Override
    protected void configure() {
        LOG.debug("Configuring AtlasSchemaRegistry module ...");
        bind(AtlasConfiguration.class).toProvider(AtlasConfigurationProvider.class).in(Singleton.class);
        loadAtlasPlugin();
        bind(AtlasEventLogger.class).in(Singleton.class);
        bind(AtlasModelBootstrap.class).in(Singleton.class);
    }

    @SuppressWarnings("unchecked")
    private void loadAtlasPlugin() {
        boolean providerLoaded = false;
        String pluginProvider = DEFAULT_ATLAS_PLUGIN_PROVIDER;
        try {
            AtlasConfiguration atlasConfiguration = configuration().getAtlasConfiguration();
            if (atlasConfiguration != null && atlasConfiguration.getCustomPluginProvider() != null) {
                pluginProvider = atlasConfiguration.getCustomPluginProvider();
            }

            Class<Provider<AtlasPlugin>> providerClass = (Class<Provider<AtlasPlugin>>) Class.forName(pluginProvider);
            bind(AtlasPlugin.class).toProvider(providerClass).in(Singleton.class);
            providerLoaded = true;
        } catch (Throwable t) {
            LOG.info("Atlas plugin provider [{}] was not found on the classpath.", pluginProvider);
        }

        if (!providerLoaded) {
            // load noop provider
            bind(AtlasPlugin.class).to(NoopAtlasPlugin.class).in(Singleton.class);
        }
    }
}
