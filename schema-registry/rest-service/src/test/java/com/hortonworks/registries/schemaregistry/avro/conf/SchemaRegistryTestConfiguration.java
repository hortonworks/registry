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

package com.hortonworks.registries.schemaregistry.avro.conf;

import com.google.common.io.Resources;

import java.io.File;
import java.net.URISyntaxException;

public class SchemaRegistryTestConfiguration {
    private String serverYAMLPath;
    private String clientYAMLPath;

    public SchemaRegistryTestConfiguration(String serverYAMLPath, String clientYAMLPath) {
        this.serverYAMLPath = serverYAMLPath;
        this.clientYAMLPath = clientYAMLPath;
    }

    public static SchemaRegistryTestConfiguration forProfileType(SchemaRegistryTestProfileType testProfileType) throws URISyntaxException {
        String serverYAMLFileName;
        String clientYAMLFileName;
        switch (testProfileType) {
            case DEFAULT:
                serverYAMLFileName = "schema-registry-test.yaml";
                clientYAMLFileName = "schema-registry-client.yaml";
                break;
            case SSL:
                serverYAMLFileName = "ssl-schema-registry-test.yaml";
                clientYAMLFileName = "ssl-schema-registry-client.yaml";
                break;
            case DEFAULT_HA:
                serverYAMLFileName = "schema-registry-test-ha.yaml";
                clientYAMLFileName = null;
                break;
            case SSL_HA:
                serverYAMLFileName = "ssl-schema-registry-test-ha.yaml";
                clientYAMLFileName = "ssl-schema-registry-client.yaml";
                break;
            default:
                throw new IllegalArgumentException("Unrecognized SchemaRegistryTestProfileType : " + testProfileType);
        }

        String serverYAMLPath = new File(Resources.getResource(serverYAMLFileName).toURI()).getAbsolutePath();
        String clientYAMLPath = clientYAMLFileName == null ? null : new File(Resources.getResource(clientYAMLFileName).toURI()).getAbsolutePath();

        return new SchemaRegistryTestConfiguration(serverYAMLPath, clientYAMLPath);
    }

    public String getServerYAMLPath() {
        return this.serverYAMLPath;
    }

    public String getClientYAMLPath() {
        return this.clientYAMLPath;
    }

}
