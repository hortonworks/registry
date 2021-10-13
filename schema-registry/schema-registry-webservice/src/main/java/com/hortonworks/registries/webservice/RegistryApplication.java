/**
 * Copyright 2016-2021 Cloudera, Inc.
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

import com.cloudera.dim.atlas.conf.AtlasSchemaRegistryModule;
import com.hortonworks.registries.common.GenericExceptionMapper;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.SchemaRegistryServiceInfo;
import com.hortonworks.registries.common.SchemaRegistryVersion;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.common.util.HadoopPlugin;
import com.hortonworks.registries.common.util.HadoopPluginFactory;
import com.hortonworks.registries.schemaregistry.webservice.CoreModule;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryModule;
import com.hortonworks.registries.webservice.healthchecks.ModulesHealthCheck;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.GuiceBundle;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Application entry point.
 */
public class RegistryApplication extends Application<RegistryConfiguration> {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryApplication.class);

    @Override
    public void run(RegistryConfiguration registryConfiguration, Environment environment) throws Exception {
        initializeUGI(registryConfiguration);

        SchemaRegistryVersion schemaRegistryVersion = SchemaRegistryServiceInfo.get().version();
        LOG.info("SchemaRegistry is starting with {}", schemaRegistryVersion);

        environment.jersey().register(MultiPartFeature.class);
        environment.jersey().register(GenericExceptionMapper.class);
        environment.healthChecks().register("modulesHealthCheck", new ModulesHealthCheck(registryConfiguration));

        if (registryConfiguration.isEnableCors()) {
            enableCORS(environment);
        }

        addServletFilters(registryConfiguration, environment);
    }

    private void initializeUGI(RegistryConfiguration conf) {
        if (conf.getServiceAuthenticationConfiguration() != null) {
            HadoopPlugin keytabCheck = HadoopPluginFactory.createKeytabCheck();
            keytabCheck.loadKerberosUser(conf.getServiceAuthenticationConfiguration());
        } else {
            LOG.debug("No service authentication is configured");
        }
    }

    @Override
    public String getName() {
        return "Schema Registry";
    }

    @Override
    public void initialize(Bootstrap<RegistryConfiguration> bootstrap) {
        LOG.debug("Initializing Registry ...");
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false)
                )
        );

        bootstrap.addBundle(GuiceBundle.builder()
                .modules(new CoreModule())
                .modules(new SchemaRegistryModule())
                .modules(new AtlasSchemaRegistryModule())
                .build());

        // always deploy UI on /ui. If there is no other filter like Confluent etc, redirect / to /ui
        bootstrap.addBundle(new AssetsBundle("/assets", "/ui", "index.html", "static"));
        bootstrap.addBundle(new SwaggerBundle<RegistryConfiguration>() {

            private SwaggerBundleConfiguration swaggerConfig;

            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(RegistryConfiguration registryConfiguration) {
                if (swaggerConfig == null) {
                    this.swaggerConfig = registryConfiguration.getSwaggerBundleConfiguration();
                    String resourcePackage = swaggerConfig.getResourcePackage();
                    swaggerConfig.setResourcePackage(resourcePackage);
                    // dropwizard-swagger 2.0.12-1 has a bug in its template, hence the custom one
                    swaggerConfig.getSwaggerViewConfiguration().setTemplateUrl("/swagger-index.ftl");
                }
                return swaggerConfig;
            }
        });
        super.initialize(bootstrap);
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

    @SuppressWarnings("unchecked")
    private void addServletFilters(RegistryConfiguration registryConfiguration, Environment environment) {
        List<ServletFilterConfiguration> servletFilterConfigurations = registryConfiguration.getServletFilters();
        if (servletFilterConfigurations != null && !servletFilterConfigurations.isEmpty()) {
            for (ServletFilterConfiguration servletFilterConfig : servletFilterConfigurations) {
                try {
                    String className = servletFilterConfig.getClassName();
                    Map<String, String> params = servletFilterConfig.getParams();
                    String typeSuffix = params.get("type") != null ? ("-" + params.get("type")) : "";
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
