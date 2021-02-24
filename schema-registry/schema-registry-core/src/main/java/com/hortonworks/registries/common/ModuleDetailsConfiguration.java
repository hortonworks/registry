/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.common;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Map;

public class ModuleDetailsConfiguration {

    private Collection<Map<String, Object>> schemaProviders;

    @JsonProperty("authorization")
    private Map<String, Object> authorizationProps;

    @JsonProperty("customSchemaStateExecutor")
    private Map<String, Object> schemaReviewExecConfig;

    private Map<String, ?> cache;

    public ModuleDetailsConfiguration() { }

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

    public Map<String, ?> getCache() {
        return cache;
    }

    public void setCache(Map<String, ?> cache) {
        this.cache = cache;
    }
}
