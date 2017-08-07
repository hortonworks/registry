/**
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestConfiguration;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.util.CustomParameterizedRunner;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

/**
 *
 */

@RunWith(CustomParameterizedRunner.class)
public class LocalRegistryServerTest {

    private static SchemaRegistryTestConfiguration SCHEMA_REGISTRY_TEST_CONFIGURATION;

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void setUp(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_CONFIGURATION = SchemaRegistryTestConfiguration.forProfileType(schemaRegistryTestProfileType);
    }

    public LocalRegistryServerTest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {

    }

    @Test
    public void testSanity() throws Exception {

        SchemaRegistryTestServerClientWrapper schemaRegistryTestServerClientWrapper = new SchemaRegistryTestServerClientWrapper(SCHEMA_REGISTRY_TEST_CONFIGURATION);
        schemaRegistryTestServerClientWrapper.startTestServer();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryTestServerClientWrapper.getClient();

        // registering schema metadata
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("foo").type("avro").build();
        Long schemaId = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(schemaId);

        // registering a new schema
        String schemaName = schemaMetadata.getName();
        String schema1 = IOUtils.toString(LocalRegistryServerTest.class.getResourceAsStream("/schema-1.avsc"), "UTF-8");
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));

        schemaRegistryTestServerClientWrapper.stopTestServer();
    }
}
