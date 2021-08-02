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

import com.cloudera.dim.atlas.AtlasPlugin;
import com.cloudera.dim.atlas.AtlasPluginProvider;
import com.cloudera.dim.atlas.bootstrap.AtlasModelBootstrap;
import com.cloudera.dim.atlas.events.AtlasEventLogger;
import com.google.inject.AbstractModule;
import com.hortonworks.registries.common.AtlasConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

public class AtlasSchemaRegistryModule extends AbstractModule {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasSchemaRegistryModule.class);

    @Override
    protected void configure() {
        LOG.debug("Configuring AtlasSchemaRegistry module ...");
        bind(AtlasConfiguration.class).toProvider(AtlasConfigurationProvider.class).in(Singleton.class);
        bind(AtlasPlugin.class).toProvider(AtlasPluginProvider.class).in(Singleton.class);;
        bind(AtlasEventLogger.class).in(Singleton.class);;
        bind(AtlasModelBootstrap.class).in(Singleton.class);;
    }
}
