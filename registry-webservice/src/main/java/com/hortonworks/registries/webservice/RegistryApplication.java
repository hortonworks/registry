/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.webservice;

import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.GenericExceptionMapper;
import com.hortonworks.registries.common.ModuleConfiguration;
import com.hortonworks.registries.common.ModuleRegistration;
import com.hortonworks.registries.common.SchemaRegistryServiceInfo;
import com.hortonworks.registries.common.SchemaRegistryVersion;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import com.hortonworks.registries.webservice.healthchecks.DummyHealthCheck;
import com.hortonworks.registries.storage.TransactionManagerAware;
import com.hortonworks.registries.storage.transaction.TransactionEventListener;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.TransactionManager;
import io.dropwizard.assets.AssetsBundle;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageManagerAware;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RegistryApplication extends Application<RegistryConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryApplication.class);
    protected StorageManager storageManager;
    protected TransactionManager transactionManager;

    @Override
    public void run(RegistryConfiguration registryConfiguration, Environment environment) throws Exception {
        initializeUGI(registryConfiguration);

        registerResources(environment, registryConfiguration);

        environment.jersey().register(GenericExceptionMapper.class);
        environment.healthChecks().register("dummy", new DummyHealthCheck());

        if (registryConfiguration.isEnableCors()) {
            enableCORS(environment);
        }

        addServletFilters(registryConfiguration, environment);

        registerModules(registryConfiguration, environment);

    }

    private void initializeUGI(RegistryConfiguration conf) throws IOException {
        if (conf.getServiceAuthenticationConfiguration() != null) {
            String authenticationType = conf.getServiceAuthenticationConfiguration().getType();
            if (authenticationType != null && authenticationType.equals("kerberos")) {
                Map<String, String> serviceAuthenticationProperties = conf.getServiceAuthenticationConfiguration().getProperties();
                if (serviceAuthenticationProperties != null) {
                    String principal = serviceAuthenticationProperties.get("principal");
                    String keytab = serviceAuthenticationProperties.get("keytab");

                    if (StringUtils.isNotEmpty(principal) && StringUtils.isNotEmpty(keytab)) {
                        LOG.debug("Login with principal = '" + principal + "' and keyTab = '" + keytab + "'");
                        try {
                            UserGroupInformation.loginUserFromKeytab(principal, keytab);
                            LOG.debug("Successfully logged in");
                        } catch (Exception e) {
                            LOG.error("Failed to log in", e);
                        }
                    } else {
                        LOG.error("Invalid service authentication configuration for 'kerberos' principal = '" + principal + "' and keytab = '" + keytab + "'");
                    }
                } else {
                    LOG.error("No service authentication properties were configured for 'kerberos'");
                }
            } else {
                LOG.error("Invalid service authentication type : " + authenticationType);
            }
        } else {
            LOG.debug("No service authentication is configured");
        }
    }

    private void registerModules(RegistryConfiguration configuration, Environment environment) {
        environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
            @Override
            public void serverStarted(Server server) {
                if (configuration.getModules().isEmpty() ||
                        configuration.getModules().stream().noneMatch(ModuleConfiguration::isEnabled)) {
                    throw new RuntimeException("There are no enabled modules!");
                }
            }
        });

    }

    @Override
    public String getName() {
        return "Schema Registry";
    }

    @Override
    public void initialize(Bootstrap<RegistryConfiguration> bootstrap) {
        // always deploy UI on /ui. If there is no other filter like Confluent etc, redirect / to /ui
        bootstrap.addBundle(new AssetsBundle("/assets", "/ui", "index.html", "static"));
        bootstrap.addBundle(new SwaggerBundle<RegistryConfiguration>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(RegistryConfiguration registryConfiguration) {
                return registryConfiguration.getSwaggerBundleConfiguration();
            }
        });
        super.initialize(bootstrap);
    }

    private void registerResources(Environment environment, RegistryConfiguration registryConfiguration)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        storageManager = getStorageManager(registryConfiguration.getStorageProviderConfiguration());
        if (storageManager instanceof TransactionManager)
            transactionManager = (TransactionManager) storageManager;
        else
            transactionManager = new NOOPTransactionManager();
        FileStorage fileStorage = getJarStorage(registryConfiguration.getFileStorageConfiguration());

        List<ModuleConfiguration> modules = registryConfiguration.getModules();
        List<Object> resourcesToRegister = new ArrayList<>();
        SchemaRegistryVersion schemaRegistryVersion = SchemaRegistryServiceInfo.get().version();
        LOG.info("SchemaRegistry is starting with {}", schemaRegistryVersion);
        for (ModuleConfiguration moduleConfiguration : modules) {
            String moduleName = moduleConfiguration.getName();
            String moduleClassName = moduleConfiguration.getClassName();

            if (!moduleConfiguration.isEnabled()) {
                LOG.info("Module [{}] is disabled, skipping initialization", moduleClassName);
                continue;
            }

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

            if (moduleRegistration instanceof TransactionManagerAware) {
                LOG.info("Module [{}] is TransactionManagerAware and setting TransactionManager.", moduleName);
                TransactionManagerAware transactionManagerAware = (TransactionManagerAware) moduleRegistration;
                transactionManagerAware.setTransactionManager(transactionManager);
            }

            resourcesToRegister.addAll(moduleRegistration.getResources());
        }

        LOG.info("Registering resources to Jersey environment: [{}]", resourcesToRegister);
        for (Object resource : resourcesToRegister) {
            environment.jersey().register(resource);
        }

        environment.jersey().register(MultiPartFeature.class);
        environment.jersey().register(new TransactionEventListener(transactionManager, TransactionIsolation.READ_COMMITTED));

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

    private void addServletFilters(RegistryConfiguration registryConfiguration, Environment environment) {
        List<ServletFilterConfiguration> servletFilterConfigurations = registryConfiguration.getServletFilters();
        if (servletFilterConfigurations != null && !servletFilterConfigurations.isEmpty()) {
            for (ServletFilterConfiguration servletFilterConfig : servletFilterConfigurations) {
                try {
                    String className = servletFilterConfig.getClassName();
                    Map<String, String> params = servletFilterConfig.getParams();
                    String typeSuffix = params.get("type") != null ? ("-" + params.get("type").toString()) : "";
                    LOG.info("Registering servlet filter [{}]", servletFilterConfig);
                    Class<? extends Filter> filterClass = (Class<? extends Filter>) Class.forName(className);
                    FilterRegistration.Dynamic dynamic = environment.servlets().addFilter(className + typeSuffix, filterClass);
                    if (params != null) {
                        dynamic.setInitParameters(params);
                    }
                    dynamic.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
                } catch (Exception e) {
                    LOG.error("Error registering servlet filter {}", servletFilterConfig);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        RegistryApplication registryApplication = new RegistryApplication();
        registryApplication.run(args);
    }

}
