/*
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestConfiguration;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.CustomParameterizedRunner;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
@RunWith(CustomParameterizedRunner.class)
public class LocalRegistryServerHATest {
    private static SchemaRegistryTestConfiguration SCHEMA_REGISTRY_TEST_CONFIGURATION;
    private static final Logger LOG = LoggerFactory.getLogger(LocalRegistryServerHATest.class);
    private TestingServer testingServer;
    private List<SchemaRegistryTestServerClientWrapper> registryServers;

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT_HA, SchemaRegistryTestProfileType.SSL_HA);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void setUp(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws URISyntaxException {
        SCHEMA_REGISTRY_TEST_CONFIGURATION = SchemaRegistryTestConfiguration.forProfileType(schemaRegistryTestProfileType);
    }

    public LocalRegistryServerHATest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {

    }

    @Before
    public void startZooKeeper() throws Exception {
        testingServer = new TestingServer(true);
        File serverYAMLFile = new File(SCHEMA_REGISTRY_TEST_CONFIGURATION.getServerYAMLPath());
        String fileContent = FileUtils.readFileToString(serverYAMLFile,"UTF-8" );

        File registryConfigFile = File.createTempFile("ha-", ".yaml");
        registryConfigFile.deleteOnExit();
        try (FileWriter writer = new FileWriter(registryConfigFile)) {
            IOUtils.write(fileContent.replace("__zk_connect_url__", testingServer.getConnectString()), writer);
        }

        SchemaRegistryTestConfiguration testConfiguration = new SchemaRegistryTestConfiguration(registryConfigFile.getAbsolutePath(), SCHEMA_REGISTRY_TEST_CONFIGURATION.getClientYAMLPath());
        List<SchemaRegistryTestServerClientWrapper> schemaRegistryServers = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            SchemaRegistryTestServerClientWrapper server = new SchemaRegistryTestServerClientWrapper(testConfiguration);
            schemaRegistryServers.add(server);
            server.startTestServer();
        }

        registryServers = Collections.unmodifiableList(schemaRegistryServers);
    }

    @After
    public void stopZooKeeper() throws Exception {
        for (SchemaRegistryTestServerClientWrapper registryServer : registryServers) {
            try {
                registryServer.startTestServer();
            } catch (Exception e) {
                // ignore any exceptions as one of the servers have been already stopped and that may throw an exception.
            }
        }

        testingServer.stop();
    }

    private SchemaRegistryTestServerClientWrapper leaderSchemaRegistryServer() {
        SchemaRegistryTestServerClientWrapper leader = null;

        while (leader == null) {
            for (SchemaRegistryTestServerClientWrapper registryServer : registryServers) {
                if (registryServer.hasLeadership()) {
                    leader = registryServer;
                }
            }
        }

        //leader may have been changed before it is returned when the current leader leaves HA quorum.
        return leader;
    }
    
    private SchemaRegistryTestServerClientWrapper followerSchemaRegistryServer() {
        leaderSchemaRegistryServer();

        for (SchemaRegistryTestServerClientWrapper registryServer : registryServers) {
            if (!registryServer.hasLeadership()) {
                return registryServer;
            }
        }

        throw new IllegalStateException("None of the servers are followers!!");
    }

    @Test
    public void testHASanity() throws Exception {

        SchemaRegistryTestServerClientWrapper followerServer = followerSchemaRegistryServer();
        SchemaRegistryClient schemaRegistryClient = followerServer.getClient();

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
        SchemaRegistryTestServerClientWrapper leaderServer = leaderSchemaRegistryServer();
        int leaderPort = leaderServer.getLocalPort();
        SchemaRegistryClient leaderClient = leaderServer.getClient();
        SchemaMetadataInfo schemaMetadataInfo = leaderClient.getSchemaMetadataInfo(schemaName);
        Assert.assertEquals(schemaMetadata, schemaMetadataInfo.getSchemaMetadata());

        // stop the leader server
        leaderServer.stopTestServer();

        // get the new leader server and run operations.
        SchemaRegistryTestServerClientWrapper newLeaderServer = leaderSchemaRegistryServer();
        Assert.assertNotEquals(leaderPort, newLeaderServer.getLocalPort());

        leaderClient = newLeaderServer.getClient();
        String receivedSchema = leaderClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v1.getVersion())).getSchemaText();
        Assert.assertEquals(schema1, receivedSchema);
    }

}
