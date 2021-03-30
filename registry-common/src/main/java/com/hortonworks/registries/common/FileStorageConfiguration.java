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

import com.fasterxml.jackson.annotation.JsonProperty;


import javax.validation.constraints.NotEmpty;

/**
 * The configuration for jar storage.
 */

public class FileStorageConfiguration {

    @NotEmpty
    private String className;
    
    private FileStorageProperties properties = new FileStorageProperties();
    
    @JsonProperty
    public String getClassName() {
        return className;
    }

    @JsonProperty
    public void setClassName(String className) {
        this.className = className;
    }

    @JsonProperty
    public FileStorageProperties getProperties() {
        return properties;
    }

    @JsonProperty
    public void setProperties(FileStorageProperties config) {
        this.properties = config;
    }
}
