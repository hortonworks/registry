/**
 * Copyright 2016 Hortonworks.
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

package com.hortonworks.registries.webservice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.registries.common.FileStorageConfiguration;
import com.hortonworks.registries.common.HAConfiguration;
import com.hortonworks.registries.common.ModuleConfiguration;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import io.dropwizard.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RegistryConfiguration extends Configuration {

    @NotNull
    private StorageProviderConfiguration storageProviderConfiguration;

    @NotNull
    private FileStorageConfiguration fileStorageConfiguration;

    @NotNull
    @JsonProperty
    private List<ModuleConfiguration> modules;

    @JsonProperty
    private HAConfiguration haConfig;

    @JsonProperty
    private boolean enableCors;

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfiguration;

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

    public List<ModuleConfiguration> getModules() {
        return modules;
    }

    public void setModules(List<ModuleConfiguration> modules) {
        this.modules = modules;
    }

    public void setSwaggerBundleConfiguration(SwaggerBundleConfiguration swaggerBundleConfiguration) {
        this.swaggerBundleConfiguration = swaggerBundleConfiguration;
    }

    public SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
        return swaggerBundleConfiguration;
    }

    public HAConfiguration getHaConfig() {
        return haConfig;
    }

    public void setHaConfig(HAConfiguration haConfig) {
        this.haConfig = haConfig;
    }
}
