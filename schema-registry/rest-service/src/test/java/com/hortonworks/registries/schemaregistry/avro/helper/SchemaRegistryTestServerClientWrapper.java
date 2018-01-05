/**
 * Copyright 2017 Hortonworks.
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

package com.hortonworks.registries.schemaregistry.avro.helper;

import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestConfiguration;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryTestServerClientWrapper {

    private LocalSchemaRegistryServer localSchemaRegistryServer;
    private SchemaRegistryTestConfiguration schemaRegistryTestConfiguration;
    private volatile Map<String, Object> cachedClientConf;
    private volatile SchemaRegistryClient cachedSchemaRegistryClient;
    private static final String V1_API_PATH = "api/v1";

    public SchemaRegistryTestServerClientWrapper(SchemaRegistryTestConfiguration schemaRegistryTestConfiguration) throws URISyntaxException {
        this.schemaRegistryTestConfiguration = schemaRegistryTestConfiguration;
        localSchemaRegistryServer = new LocalSchemaRegistryServer(schemaRegistryTestConfiguration.getServerYAMLPath());
    }

    public SchemaRegistryTestServerClientWrapper(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws URISyntaxException {
        this(SchemaRegistryTestConfiguration.forProfileType(schemaRegistryTestProfileType));
    }

    public void startTestServer() throws Exception {
        try {
            localSchemaRegistryServer.start();
        } catch (Exception e) {
            localSchemaRegistryServer.stop();
            throw e;
        }
    }

    public boolean hasLeadership() {
        return this.localSchemaRegistryServer.hasLeadership();
    }

    public int getLocalPort() {
        return this.localSchemaRegistryServer.getLocalPort();
    }

    public int getAdminPort() {
        return this.localSchemaRegistryServer.getAdminPort();
    }

    public void stopTestServer() throws Exception {
        localSchemaRegistryServer.stop();
    }

    public SchemaRegistryClient getClient() throws IOException {
        return getClient(false);
    }

    public SchemaRegistryClient getClient(boolean cached) throws IOException {
        if (!cached) {
            SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(exportClientConf(false));
            if (cachedSchemaRegistryClient == null) {
                cachedSchemaRegistryClient = schemaRegistryClient;
            }
            return schemaRegistryClient;
        } else {
            if (cachedSchemaRegistryClient == null) {
                cachedSchemaRegistryClient = new SchemaRegistryClient(exportClientConf(true));
            }
            return cachedSchemaRegistryClient;
        }
    }

    public Map<String, Object> exportClientConf() {
        return exportClientConf(false);
    }

    public Map<String, Object> exportClientConf(boolean cached) {
        if (!cached) {
            Map<String, Object> clientConfig = createClientConf();
            if (cachedClientConf == null) {
                cachedClientConf = clientConfig;
            }
            return clientConfig;
        } else {
            if (cachedClientConf == null) {
                cachedClientConf = createClientConf();
            }
            return cachedClientConf;
        }
    }

    private Map<String, Object> createClientConf() {
        String registryURL = localSchemaRegistryServer.getLocalURL() + V1_API_PATH;
        if (schemaRegistryTestConfiguration.getClientYAMLPath() == null) {
            Map<String, Object> ret = new HashMap<>();
            ret.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryURL);
            return ret;
        }
        try (FileInputStream fis = new FileInputStream(schemaRegistryTestConfiguration.getClientYAMLPath())) {
            Map<String, Object> ret = (Map<String, Object>) new Yaml().load(IOUtils.toString(fis, "UTF-8"));
            ret.put("schema.registry.url", registryURL);
            return ret;
        } catch(Exception e) {
            throw new RuntimeException("Failed to export schema client configuration for yaml : " + schemaRegistryTestConfiguration.getClientYAMLPath(), e);
        }
    }
}
