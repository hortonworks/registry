/**
 * Copyright 2016-2021 Cloudera, Inc.
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

import com.cloudera.dim.registry.oauth2.ranger.RangerOAuth2Authenticator;
import com.hortonworks.registries.common.CompatibilityConfig;
import com.hortonworks.registries.common.FileService;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.KerberosService;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgent;
import com.hortonworks.registries.schemaregistry.authorizer.agent.AuthorizationAgentFactory;
import com.hortonworks.registries.schemaregistry.authorizer.core.RangerAuthenticator;
import com.hortonworks.registries.schemaregistry.authorizer.core.util.RangerKerberosAuthenticator;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.schemaregistry.providers.CompatibilityConfigurationProvider;
import com.hortonworks.registries.schemaregistry.providers.SchemaRegistryProvider;
import com.hortonworks.registries.schemaregistry.validator.SchemaMetadataTypeValidator;
import com.hortonworks.registries.schemaregistry.webservice.auth.RangerCompositeAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.support.DropwizardAwareModule;

import javax.inject.Provider;
import javax.inject.Singleton;

public class SchemaRegistryModule extends DropwizardAwareModule<RegistryConfiguration> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryModule.class);

    @Override
    protected void configure() {
        LOG.debug("Configuring SchemaRegistry module ...");

        if (configuration().getFileStorageConfiguration() != null) {
            bind(FileStorageConfiguration.class).toInstance(configuration().getFileStorageConfiguration());
        }
        loadPlugin(configuration().getFileServiceImplementation(), FileService.class, FileService.NoopFileService.class);
        loadPlugin(configuration().getKerberosServiceImplementation(), KerberosService.class, KerberosService.NoopKerberosService.class);

        bind(AuthorizationAgent.class).toProvider(AuthorizationAgentFactory.class).in(Singleton.class);
        configureAuthenticator();

        bind(ISchemaRegistry.class).toProvider(SchemaRegistryProvider.class).in(Singleton.class);
        bind(CompatibilityConfig.class).toProvider(CompatibilityConfigurationProvider.class).in(Singleton.class);

        bind(SchemaLockManager.class).in(Singleton.class);
        bind(SchemaMetadataTypeValidator.class).in(Singleton.class);

        bind(SchemaRegistryResource.class).in(Singleton.class);
        bind(ConfluentSchemaRegistryCompatibleResource.class).in(Singleton.class);
    }

    /** Since we can have multiple authentication sources (kerberos, oauth2) we need to extract
     * the principal from the SecurityContext in different ways, depending on the source. */
    private void configureAuthenticator() {
        bind(RangerKerberosAuthenticator.class).in(Singleton.class);
        bind(RangerOAuth2Authenticator.class).in(Singleton.class);

        bind(RangerAuthenticator.class).to(RangerCompositeAuthenticator.class).in(Singleton.class);
    }

    @SuppressWarnings("unchecked")
    private <T> void loadPlugin(String implClassName, Class<T> bindInterface, Class<? extends T> noopImpl) {
        boolean pluginLoaded = false;

        // first try to load a provider - this is the preferred method because we would be loading with a
        // separate classpath loader
        try {
            Class<Provider<T>> implClass = (Class<Provider<T>>) Class.forName(implClassName);
            bind(bindInterface).toProvider(implClass).in(Singleton.class);
            pluginLoaded = true;
        } catch (Throwable t) {
            LOG.debug("Plugin provider was not found on the classpath, trying with an implementation.");
        }

        if (pluginLoaded) {
            return;
        }

        // if a provider was not found then try a direct implementation
        try {
            Class<T> implClass = (Class<T>) Class.forName(implClassName);
            bind(bindInterface).to(implClass).in(Singleton.class);
            pluginLoaded = true;
        } catch (Throwable t) {
            LOG.info("Plugin implementation [{}] was not found on the classpath.", implClassName);
        }

        // if no implementation was found then use the NO-OP class
        if (!pluginLoaded) {
            bind(bindInterface).to(noopImpl).in(Singleton.class);
        }
    }

}
