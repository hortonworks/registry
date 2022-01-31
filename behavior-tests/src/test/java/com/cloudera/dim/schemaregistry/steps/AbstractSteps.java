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
import com.cloudera.dim.schemaregistry.TestOAuth2Server;
import com.cloudera.dim.schemaregistry.TestSchemaRegistryServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

abstract class AbstractSteps {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSteps.class);

    protected final TestSchemaRegistryServer testServer;
    protected final TestAtlasServer testAtlasServer;
    protected final TestOAuth2Server testOAuth2Server;
    protected final GlobalState sow;
    protected SchemaRegistryClient schemaRegistryClient;
    protected final ObjectMapper objectMapper = new ObjectMapper();

    protected AbstractSteps() {
        testServer = TestSchemaRegistryServer.getInstance();
        sow = GlobalState.getInstance();
        testAtlasServer = TestAtlasServer.getInstance();
        testAtlasServer.replaceGlobalState(sow);
        testOAuth2Server = TestOAuth2Server.getInstance();
    }

    protected String getBaseUrl(int port) {
        return String.format("http://localhost:%d", port);
    }

    protected SchemaRegistryClient createSchemaRegistryClient(int port, boolean oauth2Enabled) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), getBaseUrl(port) + "/api/v1");
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);

        if (oauth2Enabled) {
            // TODO CDPD-34178 Configure test client to use the mock OAuth2 server
            /*
            config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(),
                    String.format("http://localhost:%s", testOAuth2Server.getPort()));
             */
        }

        return new SchemaRegistryClient(config);
    }

    protected CloseableHttpClient getHttpClient() {
        return HttpClientBuilder.create()
                .setRetryHandler(new StandardHttpRequestRetryHandler())
                .build();
    }

    protected String loadTestData(String fileName) throws URISyntaxException, IOException {
        URL url = requireNonNull(getClass().getClassLoader().getResource("testdata/" + fileName));
        return String.join("\n", Files.readAllLines(Paths.get(url.toURI())));
    }

    protected SchemaRegistryClient getSchemaRegistryClient() {
        if (schemaRegistryClient == null) {
            schemaRegistryClient = createSchemaRegistryClient(testServer.getPort(), false);
        }
        return schemaRegistryClient;
    }
}
