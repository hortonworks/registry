/*
 * Copyright 2017-2019 Cloudera, Inc.
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
 */

package com.hortonworks.registries.examples.schema.lifecycle;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SchemaLifecycleApp {

    private LocalSchemaRegistryServer localSchemaRegistryServer;
    private volatile Map<String, Object> cachedClientConf;
    private volatile SchemaRegistryClient cachedSchemaRegistryClient;
    private static final String V1_API_PATH = "api/v1";

    private final String serverConfigurationPath;

    public SchemaLifecycleApp(String serverConfigurationPath) {
        this.serverConfigurationPath = serverConfigurationPath;
    }

    public SchemaLifecycleApp() {
        this(SchemaLifecycleApp.class.getClassLoader()
                .getResource("registry-lifecycle-example.yaml").getPath());
    }

    public void startUp() throws Exception {
        localSchemaRegistryServer = new LocalSchemaRegistryServer(serverConfigurationPath);
        localSchemaRegistryServer.start();
    }

    public void stop() throws Exception {
        localSchemaRegistryServer.stop();
    }

    public SchemaRegistryClient getClient() {
        return getClient(null);
    }

    public SchemaRegistryClient getClient(String clientConfigPath) {
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(exportClientConf(false, clientConfigPath));
        if (cachedSchemaRegistryClient == null) {
            cachedSchemaRegistryClient = schemaRegistryClient;
        }
        return schemaRegistryClient;
    }

    private Map<String, Object> exportClientConf(boolean cached, String clientConfigPath) {
        if (!cached) {
            Map<String, Object> clientConfig = createClientConf(clientConfigPath);
            if (cachedClientConf == null) {
                cachedClientConf = clientConfig;
            }
            return clientConfig;
        } else {
            if (cachedClientConf == null) {
                cachedClientConf = createClientConf(clientConfigPath);
            }
            return cachedClientConf;
        }
    }

    private Map<String, Object> createClientConf(String clientConfigPath) {
        String registryURL = getRegistryURL();
        if (clientConfigPath == null) {
            Map<String, Object> ret = new HashMap<>();
            ret.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryURL);
            return ret;
        }
        try (FileInputStream fis = new FileInputStream(clientConfigPath)) {
            Map<String, Object> ret = new Yaml().load(IOUtils.toString(fis, StandardCharsets.UTF_8));
            ret.put("schema.registry.url", registryURL);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException("Failed to export schema client configuration for yaml : " +
                    clientConfigPath, e);
        }
    }

    private String getRegistryURL() {
        return localSchemaRegistryServer.getLocalURL() + V1_API_PATH;
    }
}
