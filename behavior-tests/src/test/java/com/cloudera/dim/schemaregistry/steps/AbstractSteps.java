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
package com.cloudera.dim.schemaregistry.steps;

import com.cloudera.dim.schemaregistry.GlobalState;
import com.cloudera.dim.schemaregistry.TestAtlasServer;
import com.cloudera.dim.schemaregistry.TestSchemaRegistryServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;

import java.util.HashMap;
import java.util.Map;

abstract class AbstractSteps {

    protected final TestSchemaRegistryServer testServer;
    protected final TestAtlasServer testAtlasServer;
    protected final GlobalState sow;
    protected SchemaRegistryClient schemaRegistryClient;
    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected AbstractSteps(TestSchemaRegistryServer testServer, GlobalState sow, TestAtlasServer testAtlasServer) {
        this.testServer = testServer;
        this.sow = sow;
        this.testAtlasServer = testAtlasServer;
    }

    protected String getBaseUrl(int port) {
        return String.format("http://localhost:%d", port);
    }

    protected SchemaRegistryClient createSchemaRegistryClient(int port) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), getBaseUrl(port) + "/api/v1");
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);

        return new SchemaRegistryClient(config);
    }

    protected CloseableHttpClient getHttpClient() {
        return HttpClientBuilder.create()
                .setRetryHandler(new StandardHttpRequestRetryHandler())
                .build();
    }

}
