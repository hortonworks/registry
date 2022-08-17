/**
 * Copyright 2016-2019 Cloudera, Inc.
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

package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The main configuration class. This object is created by parsing the <tt>registry.yaml</tt> file.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RegistryConfiguration extends Configuration {

    private Collection<Map<String, Object>> schemaProviders;

    @JsonProperty("authorization")
    private Map<String, Object> authorizationProps;

    @JsonProperty("customSchemaStateExecutor")
    private Map<String, Object> schemaReviewExecConfig;

    @NotNull
    private StorageProviderConfiguration storageProviderConfiguration;

    @NotNull
    private FileStorageConfiguration fileStorageConfiguration;

    @JsonProperty("atlas")
    private AtlasConfiguration atlasConfiguration;

    @JsonProperty
    private boolean enableCors;

    @JsonProperty
    private CompatibilityConfig compatibility = new CompatibilityConfig();

    private String httpProxyUrl;
    private String httpProxyUsername;
    private String httpProxyPassword;

    @JsonProperty
    private ServiceAuthenticationConfiguration serviceAuthenticationConfiguration;

    public Collection<Map<String, Object>> getSchemaProviders() {
        return schemaProviders;
    }

    public void setSchemaProviders(Collection<Map<String, Object>> schemaProviders) {
        this.schemaProviders = schemaProviders;
    }

    public Map<String, Object> getAuthorizationProps() {
        return authorizationProps;
    }

    public void setAuthorizationProps(Map<String, Object> authorizationProps) {
        this.authorizationProps = authorizationProps;
    }

    public Map<String, Object> getSchemaReviewExecConfig() {
        return schemaReviewExecConfig;
    }

    public void setSchemaReviewExecConfig(Map<String, Object> schemaReviewExecConfig) {
        this.schemaReviewExecConfig = schemaReviewExecConfig;
    }

    public String getHttpProxyUrl() {
        return httpProxyUrl;
    }

    public void setHttpProxyUrl(String httpProxyUrl) {
        this.httpProxyUrl = httpProxyUrl;
    }

    public String getHttpProxyUsername() {
        return httpProxyUsername;
    }

    public void setHttpProxyUsername(String httpProxyUsername) {
        this.httpProxyUsername = httpProxyUsername;
    }

    public String getHttpProxyPassword() {
        return httpProxyPassword;
    }

    public void setHttpProxyPassword(String httpProxyPassword) {
        this.httpProxyPassword = httpProxyPassword;
    }

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfiguration;

    private List<ServletFilterConfiguration> servletFilters;

    public StorageProviderConfiguration getStorageProviderConfiguration() {
        return storageProviderConfiguration;
    }

    public void setStorageProviderConfiguration(StorageProviderConfiguration storageProviderConfiguration) {
        this.storageProviderConfiguration = storageProviderConfiguration;
    }

    public FileStorageConfiguration getFileStorageConfiguration() {
        return fileStorageConfiguration;
    }

    public void setFileStorageConfiguration(FileStorageConfiguration fileStorageConfiguration) {
        this.fileStorageConfiguration = fileStorageConfiguration;
    }

    public boolean isEnableCors() {
        return enableCors;
    }

    public void setSwaggerBundleConfiguration(SwaggerBundleConfiguration swaggerBundleConfiguration) {
        this.swaggerBundleConfiguration = swaggerBundleConfiguration;
    }

    public SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
        return swaggerBundleConfiguration;
    }

    public List<ServletFilterConfiguration> getServletFilters() {
        return servletFilters;
    }

    public void setServletFilters(List<ServletFilterConfiguration> servletFilters) {
        this.servletFilters = servletFilters;
    }

    public ServiceAuthenticationConfiguration getServiceAuthenticationConfiguration() {
        return serviceAuthenticationConfiguration;
    }

    public void setServiceAuthenticationConfiguration(ServiceAuthenticationConfiguration serviceAuthenticationConfiguration) {
        this.serviceAuthenticationConfiguration = serviceAuthenticationConfiguration;
    }

    public AtlasConfiguration getAtlasConfiguration() {
        return atlasConfiguration;
    }

    public void setAtlasConfiguration(AtlasConfiguration atlasConfiguration) {
        this.atlasConfiguration = atlasConfiguration;
    }

    public CompatibilityConfig getCompatibility() {
        return compatibility;
    }

    public void setCompatibility(CompatibilityConfig compatibility) {
        this.compatibility = compatibility;
    }
}
