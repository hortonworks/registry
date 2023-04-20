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

import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientConfig;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.spi.ConnectorProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaRegistryClientTest {
    
    private SchemaRegistryClient schemaRegistryClientMock;

    @BeforeEach
    public void setup() {
        schemaRegistryClientMock = mock(SchemaRegistryClient.class);
    }

    @Test
    public void testClientWithCache() throws Exception {

        final String schemaName = "foo";
        final SchemaMetadata schemaMetaData = new SchemaMetadata.Builder(schemaName).schemaGroup("group").type("type").build();
        final SchemaVersion schemaVersion = new SchemaVersion("schema-text", "desc");
        final SchemaIdVersion schemaIdVersion = new SchemaIdVersion(1L, 1);

        when(schemaRegistryClientMock.registerSchemaMetadata(schemaMetaData)).thenReturn(1L);
        when(schemaRegistryClientMock.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion, false)).thenReturn(schemaIdVersion);

        Long metadataId = schemaRegistryClientMock.registerSchemaMetadata(schemaMetaData);
        schemaRegistryClientMock.addSchemaVersion(schemaMetaData, schemaVersion);
        schemaRegistryClientMock.addSchemaVersion(schemaMetaData, schemaVersion);
        schemaRegistryClientMock.addSchemaVersion(schemaName, schemaVersion);
    }
    
    @Test
    public void testConfigEntryValidation() {
        //given
        Map<String, String> goodConf = new HashMap<>();
        goodConf.put("schema.registry.url", "goodvalue");
        goodConf.put("schema.registry.client.url.selector", "nonnullvalue");

        SchemaRegistryClient.Configuration configuration = new SchemaRegistryClient.Configuration(ImmutableMap.of("key", "value"));
        
        //when
        Map<String, ?> result = configuration.buildConfig(goodConf);
        
        //then
        Assertions.assertEquals(goodConf, result);
    }

    @Test 
    public void testConfigEntryValidationBadconf() {
        //given
        Map<String, String> badConf = new HashMap<>();
        badConf.put("schema.registry.url", "goodvalue");
        badConf.put("schema.registry.client.url.selector", "nonnullvalue");
        badConf.put("schema.registry.client.local.jars.path", "");

        SchemaRegistryClient.Configuration configuration = new SchemaRegistryClient.Configuration(ImmutableMap.of("key", "value"));
        
        //when
        Assertions.assertThrows(IllegalArgumentException.class, () -> configuration.buildConfig(badConf));
        
    }
    
    @Test
    public void testCreateClientConfigWithDefaultConnectorProvider() {
        //given
        Map<String, String> conf = new HashMap<>();
        String nettyConnectorProvider = "com.hortonworks.registries.shaded.org.glassfish.jersey.netty.connector.NettyConnectorProvider";
        conf.put("schema.registry.url", "http://localhost:9090/api/v1");
        conf.put("connector.provider.class", nettyConnectorProvider);
        when(schemaRegistryClientMock.createClientConfig(conf)).thenCallRealMethod();
        
        //when
        ClientConfig underTest = schemaRegistryClientMock.createClientConfig(conf);
        
        //then
        ConnectorProvider connector = underTest.getConnectorProvider();
        Assertions.assertEquals(nettyConnectorProvider, connector.getClass().getName());
    }
    
    @Test
    public void testCreateClientConfigWithBadConnectionProvider() {
        //given
        Map<String, String> conf = new HashMap<>();
        conf.put("schema.registry.url", "http://localhost:9090/api/v1");
        conf.put("connector.provider.class", "bad.class.name");
        when(schemaRegistryClientMock.createClientConfig(conf)).thenCallRealMethod();

        //when
        Assertions.assertThrows(RuntimeException.class, () -> schemaRegistryClientMock.createClientConfig(conf));
        
    }
    
    @Test
    public void testCreateClientConfigWithUnknownConnectorProvider() {
        //given
        String testConnectorProvider = "com.hortonworks.registries.schemaregistry.client.TestConnectorProvider";
        Map<String, String> conf = new HashMap<>();
        conf.put("schema.registry.url", "http://localhost:9090/api/v1");
        conf.put("connector.provider.class", testConnectorProvider);
        when(schemaRegistryClientMock.createClientConfig(conf)).thenCallRealMethod();

        //when
        ClientConfig underTest = schemaRegistryClientMock.createClientConfig(conf);

        //then
        ConnectorProvider connector = underTest.getConnectorProvider();
        Assertions.assertEquals(testConnectorProvider, connector.getClass().getName());
        
    }

    @Test
    public void testCreateClientConfigWithNotSetConnectorProvider() {
        //given
        String testConnectorProvider = "com.hortonworks.registries.shaded.org.glassfish.jersey.client.HttpUrlConnectorProvider";
        Map<String, String> conf = new HashMap<>();
        conf.put("schema.registry.url", "http://localhost:9090/api/v1");
        when(schemaRegistryClientMock.createClientConfig(conf)).thenCallRealMethod();

        //when
        ClientConfig underTest = schemaRegistryClientMock.createClientConfig(conf);

        //then
        ConnectorProvider connector = underTest.getConnectorProvider();
        Assertions.assertEquals(testConnectorProvider, connector.getClass().getName());

    }
}
