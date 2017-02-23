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
package com.hortonworks.registries.schemaregistry.avro;

import com.google.common.io.Resources;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class LocalRegistryServerHATest {
    private static final Logger LOG = LoggerFactory.getLogger(LocalRegistryServerHATest.class);
    private TestingServer testingServer;
    private List<LocalSchemaRegistryServer> registryServers;

    @Before
    public void startZooKeeper() throws Exception {
        testingServer = new TestingServer(true);
        URI configPath = Resources.getResource("schema-registry-test-ha.yaml").toURI();
        String fileContent = IOUtils.toString(configPath, "UTF-8");

        File registryConfigFile = File.createTempFile("ha-", ".yaml");
        registryConfigFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(registryConfigFile)) {
            IOUtils.write(fileContent.replace("__zk_connect_url__", testingServer.getConnectString()), writer);
        }

        List<LocalSchemaRegistryServer> schemaRegistryServers = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            LocalSchemaRegistryServer server = new LocalSchemaRegistryServer(registryConfigFile.getAbsolutePath());
            schemaRegistryServers.add(server);
            server.start();
        }

        registryServers = Collections.unmodifiableList(schemaRegistryServers);
    }

    @After
    public void stopZooKeeper() throws Exception {
        for (LocalSchemaRegistryServer registryServer : registryServers) {
            try {
                registryServer.stop();
            } catch (Exception e) {
                // ignore any exceptions as one of the servers have been already stopped and that may throw an exception.
            }
        }

        testingServer.stop();
    }

    private LocalSchemaRegistryServer leaderSchemaRegistryServer() {
        LocalSchemaRegistryServer leader = null;

        while (leader == null) {
            for (LocalSchemaRegistryServer registryServer : registryServers) {
                if (registryServer.hasLeadership()) {
                    leader = registryServer;
                }
            }
        }

        //leader may have been changed before it is returned when the current leader leaves HA quorum.
        return leader;
    }
    
    private LocalSchemaRegistryServer followerSchemaRegistryServer() {
        leaderSchemaRegistryServer();

        for (LocalSchemaRegistryServer registryServer : registryServers) {
            if (!registryServer.hasLeadership()) {
                return registryServer;
            }
        }

        throw new IllegalStateException("None of the servers are followers!!");
    }

    @Test
    public void testHASanity() throws Exception {

        LocalSchemaRegistryServer followerServer = followerSchemaRegistryServer();
        SchemaRegistryClient schemaRegistryClient = createSchemaRegistryClient(followerServer.getLocalPort());

        // registering schema metadata on follower, this should have been redirected to leader.
        String schemaName = "foo";
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(schemaName).type("avro").build();
        Long schemaId = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(schemaId);

        // registering a new schema on follower, this should have been redirected to leader.
        String schema1 = IOUtils.toString(LocalRegistryServerTest.class.getResourceAsStream("/schema-1.avsc"), "UTF-8");
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName,
                                                                   new SchemaVersion(schema1, "Initial version of the schema"));

        // retrieve schema on leader as the schema data is stored in memory in leader. this data does not exist on
        // followers as the storage is inmemory.
        LocalSchemaRegistryServer leaderServer = leaderSchemaRegistryServer();
        int leaderPort = leaderServer.getLocalPort();
        SchemaRegistryClient leaderClient = createSchemaRegistryClient(leaderPort);
        SchemaMetadataInfo schemaMetadataInfo = leaderClient.getSchemaMetadataInfo(schemaName);
        Assert.assertEquals(schemaMetadata, schemaMetadataInfo.getSchemaMetadata());

        // stop the leader server
        leaderServer.stop();

        // get the new leader server and run operations.
        LocalSchemaRegistryServer newLeaderServer = leaderSchemaRegistryServer();
        Assert.assertNotEquals(leaderPort, newLeaderServer.getLocalPort());

        leaderClient = createSchemaRegistryClient(newLeaderServer.getLocalPort());
        String receivedSchema = leaderClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v1.getVersion())).getSchemaText();
        Assert.assertEquals(schema1, receivedSchema);
    }

    private SchemaRegistryClient createSchemaRegistryClient(int localPort) {
        final String rootUrl = String.format("http://localhost:%d/api/v1", localPort);
        final Map<String, String> SCHEMA_REGISTRY_CLIENT_CONF = Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), rootUrl);
        return new SchemaRegistryClient(SCHEMA_REGISTRY_CLIENT_CONF);
    }
}
