/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.registries.webservice;

import org.apache.registries.common.FileStorageConfiguration;
import org.apache.registries.common.ModuleConfiguration;
import org.apache.registries.common.ModuleRegistration;
import org.apache.registries.common.util.FileStorage;
import org.apache.registries.storage.StorageManager;
import org.apache.registries.storage.StorageManagerAware;
import org.apache.registries.storage.StorageProviderConfiguration;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;

/**
 *
 */
public class RegistryApplication extends Application<RegistryConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryApplication.class);

    @Override
    public void run(RegistryConfiguration registryConfiguration, Environment environment) throws Exception {

        registerResources(environment, registryConfiguration);

        if (registryConfiguration.isEnableCors()) {
            enableCORS(environment);
        }
    }

    @Override
    public void initialize(Bootstrap<RegistryConfiguration> bootstrap) {
        bootstrap.addBundle(new SwaggerBundle<RegistryConfiguration>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(RegistryConfiguration registryConfiguration) {
                return registryConfiguration.getSwaggerBundleConfiguration();
            }
        });
    }

    private void registerResources(Environment environment, RegistryConfiguration registryConfiguration)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        StorageManager storageManager = getStorageManager(registryConfiguration.getStorageProviderConfiguration());
        FileStorage fileStorage = getJarStorage(registryConfiguration.getFileStorageConfiguration());

        List<ModuleConfiguration> modules = registryConfiguration.getModules();
        List<Object> resourcesToRegister = new ArrayList<>();
        for (ModuleConfiguration moduleConfiguration : modules) {
            String moduleName = moduleConfiguration.getName();
            String moduleClassName = moduleConfiguration.getClassName();
            LOG.info("Registering module [{}] with class [{}]", moduleName, moduleClassName);
            ModuleRegistration moduleRegistration = (ModuleRegistration) Class.forName(moduleClassName).newInstance();
            if (moduleConfiguration.getConfig() == null) {
                moduleConfiguration.setConfig(new HashMap<String, Object>());
            }
            moduleRegistration.init(moduleConfiguration.getConfig(), fileStorage);

            if (moduleRegistration instanceof StorageManagerAware) {
                LOG.info("Module [{}] is StorageManagerAware and setting StorageManager.", moduleName);
                StorageManagerAware storageManagerAware = (StorageManagerAware) moduleRegistration;
                storageManagerAware.setStorageManager(storageManager);
            }
            resourcesToRegister.addAll(moduleRegistration.getResources());
        }

        LOG.info("Registering resources to Jersey environment: [{}]", resourcesToRegister);
        for (Object resource : resourcesToRegister) {
            environment.jersey().register(resource);
        }
        environment.jersey().register(MultiPartFeature.class);
    }

    private void enableCORS(Environment environment) {
        // Enable CORS headers
        final FilterRegistration.Dynamic cors = environment.servlets().addFilter("CORS", CrossOriginFilter.class);

        // Configure CORS parameters
        cors.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        cors.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "X-Requested-With,Authorization,Content-Type,Accept,Origin");
        cors.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "OPTIONS,GET,PUT,POST,DELETE,HEAD");

        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

    private FileStorage getJarStorage(FileStorageConfiguration fileStorageConfiguration) {
        FileStorage fileStorage = null;
        if (fileStorageConfiguration.getClassName() != null)
            try {
                fileStorage = (FileStorage) Class.forName(fileStorageConfiguration.getClassName(), true,
                                                          Thread.currentThread().getContextClassLoader()).newInstance();
                fileStorage.init(fileStorageConfiguration.getProperties());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        return fileStorage;
    }

    private StorageManager getStorageManager(StorageProviderConfiguration storageProviderConfiguration) {
        final String providerClass = storageProviderConfiguration.getProviderClass();
        StorageManager storageManager;
        try {
            storageManager = (StorageManager) Class.forName(providerClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        storageManager.init(storageProviderConfiguration.getProperties());
        return storageManager;
    }

    public static void main(String[] args) throws Exception {
        RegistryApplication registryApplication = new RegistryApplication();
        registryApplication.run(args);
    }

}
