/**
 * Copyright 2016-2023 Cloudera, Inc.
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

package com.cloudera.dim.schemaregistry.testcontainers.env.envutils.config;

import com.cloudera.dim.registry.oauth2.JwtKeyStoreType;
import com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler;
import com.cloudera.dim.registry.oauth2.OAuth2Config;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.KerberosSetup;
import com.hortonworks.registries.auth.server.AuthenticationFilter;
import com.hortonworks.registries.common.RegistryConfiguration;
import com.hortonworks.registries.common.ServletFilterConfiguration;
import com.hortonworks.registries.schemaregistry.webservice.RewriteUriFilter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServletFilterConfigGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(ServletFilterConfigGenerator.class);

    private ServletFilterConfigGenerator() {
    }

    public static List<ServletFilterConfiguration> prepareServletFilters(boolean sslEnabled,
                                                                         boolean oauth2Enabled,
                                                                         Class classForOauthResources,
                                                                         KerberosSetup kerberosSetup,
                                                                         boolean mtlsEnabled,
                                                                         List<ServletFilterConfiguration> additionalFilters) {
        List<ServletFilterConfiguration> servletFilters = additionalFilters;
        if (servletFilters == null) {
            servletFilters = new ArrayList<>();
        }

        addHeaderFilter(servletFilters, sslEnabled);

        int sumAuthFiltersCount = 0;
        if (oauth2Enabled) {
            sumAuthFiltersCount++;
        }
        if (kerberosSetup != null) {
            sumAuthFiltersCount++;
        }
        if (mtlsEnabled) {
            sumAuthFiltersCount++;
        }

        if (oauth2Enabled) {
            addOAuthServletFilterConfig(servletFilters, classForOauthResources, sumAuthFiltersCount > 1);
        }

        if (kerberosSetup != null) {
            addKerberosAuthFilterConfigs(servletFilters, sumAuthFiltersCount > 1,
                    kerberosSetup.getSrServerPrincipalName(),
                    kerberosSetup.getSrServerPrincipalKeytabPath());
        }

        if (mtlsEnabled) {
            addMTlsAuthServletFilterConfig(servletFilters, sumAuthFiltersCount > 1);
        }

        if (sumAuthFiltersCount > 1) {
            addCompositeAuthTerminatorFilterConfigs(servletFilters);
        }

        if (!servletFilters.isEmpty()) {
            LOG.info("{} servlet filters: {}", servletFilters.size(), servletFilters.stream()
                    .map(ServletFilterConfiguration::getClassName)
                    .collect(Collectors.joining(", ")));
        }

        return servletFilters;
    }


    public static void addOptionalServletFilterConfigEntries(RegistryConfiguration configuration,
                                                             List<ServletFilterConfiguration> servletFilters) {
        if (configuration.getServletFilters() == null) {
            configuration.setServletFilters(new ArrayList<>());
        }
        if (servletFilters != null) {
            configuration.getServletFilters().addAll(servletFilters);
        }
    }

    public static void addRewriteServletFilterConfig(RegistryConfiguration configuration) {
        ServletFilterConfiguration rewriteFilterConf = new ServletFilterConfiguration();
        configuration.getServletFilters().add(rewriteFilterConf);
        rewriteFilterConf.setClassName(RewriteUriFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();

        rewriteFilterConf.setParams(params);
        params.put("forwardPaths", "/api/v1/confluent,/subjects/*,/schemas/ids/*");
        params.put("redirectPaths", "/ui/,/");

        // based on jinja: https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L188-L193
    }

    private static void addOAuthServletFilterConfig(List<ServletFilterConfiguration> servletFilters,
                                                    Class classForOauthResources,
                                                    boolean compositeAuth) {
        ServletFilterConfiguration filterConfig = new ServletFilterConfiguration();
        servletFilters.add(filterConfig);
        filterConfig.setClassName(AuthenticationFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();

        filterConfig.setParams(params);
        params.put("type", OAuth2AuthenticationHandler.class.getName());
        if (compositeAuth) {
            params.put("composite", "true");
        }
        params.put(OAuth2Config.KEY_STORE_TYPE, JwtKeyStoreType.PROPERTY.getValue());
        params.put(OAuth2Config.KEY_ALGORITHM, "RS256");
        try {
            params.put(OAuth2Config.PUBLIC_KEY_PROPERTY,
                    IOUtils.toString(
                                    classForOauthResources.getResource("/sr_server_container/template/for_oauth/test_rsa.pub"),
                                    StandardCharsets.UTF_8)
                            .replaceAll("\n", ""));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // implementation is not tested yet
        // based on jinja: https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L74-L161
    }

    private static void addMTlsAuthServletFilterConfig(List<ServletFilterConfiguration> servletFilters,
                                                       boolean compositeAuth) {
        ServletFilterConfiguration filterConfig = new ServletFilterConfiguration();
        servletFilters.add(filterConfig);
        filterConfig.setClassName(com.hortonworks.registries.auth.server.AuthenticationFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();
        filterConfig.setParams(params);
        params.put("type", com.cloudera.dim.registry.ssl.MutualSslFilter.class.getName());
        if (compositeAuth) {
            params.put("composite", "true");
        }

        params.put("rules", "RULE:^[Cc][Nn]=([a-zA-Z0-9.-]*).*$/$1/L");

        // based on jinja: https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L64-L73
    }

    private static void addHeaderFilter(List<ServletFilterConfiguration> servletFilters,
                                        boolean sslEnabled) {

        String headerValues = "" +
                "set X-XSS-PROTECTION: 1; mode=block," +
                "set X-Content-Type-Options: nosniff," +
                "set Cache-Control: no-store," +
                "set X-Frame-Options: DENY";

        if (sslEnabled) {
            headerValues += ", set Strict-Transport-Security: max-age=31536000; includeSubDomains";
        }

        ServletFilterConfiguration filterConfig = new ServletFilterConfiguration();
        servletFilters.add(filterConfig);
        filterConfig.setClassName(org.eclipse.jetty.servlets.HeaderFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();

        filterConfig.setParams(params);
        params.put("headerConfig", headerValues);

        // based on jinja: https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L38-L44
        // and https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L61-L63

    }

    private static void addCompositeAuthTerminatorFilterConfigs(List<ServletFilterConfiguration> servletFilters) {
        ServletFilterConfiguration filterConfig = new ServletFilterConfiguration();
        servletFilters.add(filterConfig);
        filterConfig.setClassName(com.hortonworks.registries.auth.server.AuthTerminatorFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();
        filterConfig.setParams(params);

        // based on jinja: https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L185-L187
    }

    private static void addKerberosAuthFilterConfigs(List<ServletFilterConfiguration> servletFilters,
                                                     boolean compositeAuth,
                                                     String principal,
                                                     String keytabPath) {

        ServletFilterConfiguration filterConfig = new ServletFilterConfiguration();
        servletFilters.add(filterConfig);
        filterConfig.setClassName(com.hortonworks.registries.auth.server.AuthenticationFilter.class.getName());
        Map<String, String> params = new LinkedHashMap<>();
        filterConfig.setParams(params);
        params.put("type", "kerberos");
        if (compositeAuth) {
            params.put("composite", "true");
        }

        params.put("kerberos.principal", principal); // "HTTP/{{ .Release.Name }}.{{ .Values.namespace }}.svc.cluster.local@K8S.COM"
        params.put("kerberos.keytab", keytabPath); //"/tmp/registry.service.keytab"

        params.put("kerberos.name.rules", "RULE:[2:$1@$0]([jt]t@.*K8S.COM)s/.*/$MAPRED_USER/ RULE:[2:$1@$0]([nd]n@.*K8S.COM)s/.*/$HDFS_USER/DEFAULT");
        params.put("allowed.resources", "401.html,back-default.png,favicon.ico");

        // based on jinja: https://github.infra.cloudera.com/Starship/cmf/blob/eb69ad6202c5f24b3705e6ae6572181e66c29591/csd/SCHEMAREGISTRY/src/aux/templates/registry.yaml.j2#L162-L184
    }

}
