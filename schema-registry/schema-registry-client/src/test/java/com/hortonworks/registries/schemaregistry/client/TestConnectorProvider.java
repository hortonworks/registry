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
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.shaded.javax.ws.rs.client.Client;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Configuration;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientRequest;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientResponse;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.spi.AsyncConnectorCallback;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.spi.Connector;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.spi.ConnectorProvider;

import java.util.concurrent.Future;

public class TestConnectorProvider implements ConnectorProvider {
    @Override
    public Connector getConnector(Client client, Configuration configuration) {
        return new Connector() {
            @Override
            public ClientResponse apply(ClientRequest clientRequest) {
                return null;
            }

            @Override
            public Future<?> apply(ClientRequest clientRequest, AsyncConnectorCallback asyncConnectorCallback) {
                return null;
            }

            @Override
            public String getName() {
                return "test";
            }

            @Override
            public void close() {

            }
        };
    }
}
