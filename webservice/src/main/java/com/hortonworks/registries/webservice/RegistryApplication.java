/**
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.webservice;

import com.hortonworks.registries.common.GenericExceptionMapper;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.common.transaction.TransactionIsolation;
import com.hortonworks.registries.cron.RefreshHAServerManagedTask;
import com.hortonworks.registries.schemaregistry.HAServerNotificationManager;
import com.hortonworks.registries.schemaregistry.HAServersAware;
import com.hortonworks.registries.schemaregistry.HostConfigStorable;
import com.hortonworks.registries.storage.transaction.TransactionEventListener;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.TransactionManager;
import io.dropwizard.assets.AssetsBundle;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.HAConfiguration;
import com.hortonworks.registries.common.ModuleConfiguration;
import com.hortonworks.registries.common.ModuleRegistration;
import com.hortonworks.registries.common.ha.LeadershipAware;
import com.hortonworks.registries.common.ha.LeadershipParticipant;
import com.hortonworks.registries.common.ha.LocalLeader;
import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageManagerAware;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class RegistryApplication extends Application<RegistryConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryApplication.class);
    protected AtomicReference<LeadershipParticipant> leadershipParticipantRef = new AtomicReference<>();
    protected StorageManager storageManager;
    protected HAServerNotificationManager haServerNotificationManager = new HAServerNotificationManager();
    protected TransactionManager transactionManager;
    protected RefreshHAServerManagedTask refreshHAServerManagedTask;

    @Override
    public void run(RegistryConfiguration registryConfiguration, Environment environment) throws Exception {

        // handle HA if it is configured
        registerHA(registryConfiguration.getHaConfig(), environment);

        registerResources(environment, registryConfiguration);

        environment.jersey().register(GenericExceptionMapper.class);

        if (registryConfiguration.isEnableCors()) {
            enableCORS(environment);
        }

        addServletFilters(registryConfiguration, environment);

        registerAndNotifyOtherServers(environment);

    }

    private void registerAndNotifyOtherServers(Environment environment) {
        environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
            @Override
            public void serverStarted(Server server) {

                String serverURL = server.getURI().toString();

                haServerNotificationManager.setHomeNodeURL(serverURL);

                try {
                    transactionManager.beginTransaction(TransactionIsolation.SERIALIZABLE);
                    HostConfigStorable hostConfigStorable = storageManager.get(new HostConfigStorable(serverURL).getStorableKey());
                    if (hostConfigStorable == null) {
                        storageManager.add(new HostConfigStorable(storageManager.nextId(HostConfigStorable.NAME_SPACE), serverURL,
                                System.currentTimeMillis()));
                    }
                    haServerNotificationManager.refreshServerInfo(storageManager.<HostConfigStorable>list(HostConfigStorable.NAME_SPACE));
                    transactionManager.commitTransaction();
                } catch (Exception e) {
                    transactionManager.rollbackTransaction();
                    throw e;
                }

                haServerNotificationManager.notifyDebut();

                refreshHAServerManagedTask = new RefreshHAServerManagedTask(storageManager,transactionManager, haServerNotificationManager);
                environment.lifecycle().manage(refreshHAServerManagedTask);
                refreshHAServerManagedTask.start();
            }
        });

    }

    private void registerHA(HAConfiguration haConfiguration, Environment environment) throws Exception {
        if(haConfiguration != null) {
            environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server) {

                    String serverUrl = server.getURI().toString();

                    LOG.info("Received callback as server is started with server URL:[{}]", server);
                    LOG.info("HA configuration: [{}]", haConfiguration);
                    String className = haConfiguration.getClassName();

                    LeadershipParticipant leadershipParticipant = null;
                    try {
                        leadershipParticipant = (LeadershipParticipant) Class.forName(className).newInstance();
                    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    leadershipParticipant.init(haConfiguration.getConfig(), serverUrl);

                    leadershipParticipantRef.set(leadershipParticipant);
                    LOG.info("Registering for leadership with participant [{}]", leadershipParticipant);
                    try {
                        leadershipParticipantRef.get().participateForLeadership();
                    } catch (Exception e) {
                        LOG.error("Error occurred while participating for leadership with serverUrl [{}]", serverUrl, e);
                        throw new RuntimeException(e);
                    }
                    LOG.info("Registered for leadership with participant [{}]", leadershipParticipant);
                }
            });
        } else {
            leadershipParticipantRef.set(LocalLeader.getInstance());
            LOG.info("No HA configuration exists, using [{}]", leadershipParticipantRef);
        }
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

            if(moduleRegistration instanceof LeadershipAware) {
                LOG.info("Module [{}] is registered for LeadershipParticipant registration.", moduleName);
                LeadershipAware leadershipAware = (LeadershipAware) moduleRegistration;
                leadershipAware.setLeadershipParticipant(leadershipParticipantRef);
            }

            if(moduleRegistration instanceof HAServersAware) {
                LOG.info("Module [{}] is registered for HAServersAware registration.");
                HAServersAware leadershipAware = (HAServersAware) moduleRegistration;
                leadershipAware.setHAServerConfigManager(haServerNotificationManager);
            }

            resourcesToRegister.addAll(moduleRegistration.getResources());
        }

        LOG.info("Registering resources to Jersey environment: [{}]", resourcesToRegister);
        for (Object resource : resourcesToRegister) {
            environment.jersey().register(resource);
        }
        
        environment.jersey().register(MultiPartFeature.class);
        environment.jersey().register(new TransactionEventListener(transactionManager));

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
            for (ServletFilterConfiguration servletFilterConfig: servletFilterConfigurations) {
                try {
                    String className = servletFilterConfig.getClassName();
                    Map<String, String> params = servletFilterConfig.getParams();
                    LOG.info("Registering servlet filter [{}]", servletFilterConfig);
                    Class<? extends Filter> filterClass = (Class<? extends Filter>) Class.forName(className);
                    FilterRegistration.Dynamic dynamic = environment.servlets().addFilter(className, filterClass);
                    if(params != null) {
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
