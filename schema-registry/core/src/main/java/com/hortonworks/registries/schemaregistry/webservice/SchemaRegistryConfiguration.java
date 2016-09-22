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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.iotas.common.FileStorageConfiguration;
import com.hortonworks.iotas.storage.StorageProviderConfiguration;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 *
 */
public class SchemaRegistryConfiguration extends Configuration {

    @NotNull
    private StorageProviderConfiguration storageProviderConfiguration;

    @NotNull
    private FileStorageConfiguration fileStorageConfiguration;

    @NotNull
    @NotEmpty
    private List<String> schemaProviderClasses;

    @JsonProperty
    private boolean enableCors;

    @JsonProperty
    private Integer schemaCacheSize;

    @JsonProperty
    private Long schemaCacheExpiryInterval;

    public StorageProviderConfiguration getStorageProviderConfiguration() {
        return storageProviderConfiguration;
    }

    public void setStorageProviderConfiguration(StorageProviderConfiguration storageProviderConfiguration) {
        this.storageProviderConfiguration = storageProviderConfiguration;
    }

    public List<String> getSchemaProviderClasses() {
        return schemaProviderClasses;
    }

    public void setSchemaProviderClasses(List<String> schemaProviderClasses) {
        this.schemaProviderClasses = schemaProviderClasses;
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

    public Integer getSchemaCacheSize() {
        return schemaCacheSize;
    }

    public Long getSchemaCacheExpiryInterval() {
        return schemaCacheExpiryInterval;
    }
}
