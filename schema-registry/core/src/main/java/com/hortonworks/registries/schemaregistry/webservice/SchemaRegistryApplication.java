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
package com.hortonworks.registries.schemaregistry.webservice;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaFieldInfoStorable;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaSerDesMapping;
import com.hortonworks.registries.schemaregistry.SchemaVersionStorable;
import com.hortonworks.registries.schemaregistry.SerDesInfoStorable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import io.dropwizard.Application;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.setup.Environment;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaRegistryApplication extends Application<SchemaRegistryConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryApplication.class);

    @Override
    public void run(SchemaRegistryConfiguration configuration, Environment environment) throws Exception {

        StorageManager storageManager = getStorageManager(configuration.getStorageProviderConfiguration());
        FileStorage fileStorage = getJarStorage(configuration.getFileStorageConfiguration());
        Collection<? extends SchemaProvider> schemaProviders = getSchemaProviders(configuration.getSchemaProviderClasses());
        ISchemaRegistry schemaRegistry = new DefaultSchemaRegistry(storageManager, fileStorage, schemaProviders);

        initializeSchemaRegistry(schemaRegistry, configuration);

        registerResources(environment, new SchemaRegistryResource(schemaRegistry));

        if (configuration.isEnableCors()) {
            enableCORS(environment);
        }
    }

    private void initializeSchemaRegistry(ISchemaRegistry schemaRegistry, SchemaRegistryConfiguration schemaRegistryConfiguration) {
        Map<String, Object> config = new HashMap<>();
        config.put(DefaultSchemaRegistry.Options.SCHEMA_CACHE_SIZE, schemaRegistryConfiguration.getSchemaCacheSize());
        config.put(DefaultSchemaRegistry.Options.SCHEMA_CACHE_EXPIRY_INTERVAL_SECS, schemaRegistryConfiguration.getSchemaCacheExpiryInterval());
        schemaRegistry.init(config);
    }

    private void registerResources(Environment environment, SchemaRegistryResource schemaRegistryResource) {
        JerseyEnvironment jersey = environment.jersey();
        jersey.register(MultiPartFeature.class);
        jersey.register(schemaRegistryResource);

        // register swagger resources
        jersey.register(ApiListingResource.class);
        jersey.register(SwaggerSerializers.class);

        // this is required to set ScanFactory and scanning enabled for given packages.
        BeanConfig config = new BeanConfig();
        config.setTitle("SchemaRegistry Application");
        config.setVersion("0.1.0");
        config.setResourcePackage("com.hortonworks.registries.schemaregistry.webservice");
        config.setScan(true);
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
                fileStorage = (FileStorage) Class.forName(fileStorageConfiguration.getClassName(), true, Thread.currentThread().getContextClassLoader()).newInstance();
                fileStorage.init(fileStorageConfiguration.getProperties());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        return fileStorage;
    }

    private Collection<? extends SchemaProvider> getSchemaProviders(Collection<String> schemaProviderClassNames) {
        return Collections2.transform(schemaProviderClassNames, new Function<String, SchemaProvider>() {
            @Nullable
            @Override
            public SchemaProvider apply(@Nullable String className) {
                try {
                    return (SchemaProvider) Class.forName(className, true, Thread.currentThread().getContextClassLoader()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    LOG.error("Error encountered while loading SchemaProvider", e);
                    throw new IllegalArgumentException(e);
                }
            }
        });
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
        storageManager.registerStorables(
                Lists.newArrayList(SchemaMetadataStorable.class, SchemaVersionStorable.class, SchemaFieldInfoStorable.class,
                        SerDesInfoStorable.class, SchemaSerDesMapping.class));
        return storageManager;
    }

    public static void main(String[] args) throws Exception {
        SchemaRegistryApplication schemaRegistryApplication = new SchemaRegistryApplication();
        schemaRegistryApplication.run(args);
    }

}
