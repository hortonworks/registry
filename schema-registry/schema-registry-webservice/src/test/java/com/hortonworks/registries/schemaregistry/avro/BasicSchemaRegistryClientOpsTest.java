/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Stream;

@Tag("IntegrationTest")
public class BasicSchemaRegistryClientOpsTest {
    private SchemaRegistryClient schemaRegistryClient;
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;

    private String testName;

    @BeforeEach
    void init(TestInfo testInfo) {
        testName = testInfo.getTestMethod().get().getName();
    }

    public static Stream<SchemaRegistryTestProfileType> profiles() {
        return Stream.of(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    public void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        schemaRegistryClient = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient();
    }

    @AfterEach
    public void stopServer() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void testSchemaOpsWithValidationLevelAsLatest(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        doTestSchemaOps(SchemaValidationLevel.LATEST);
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void testSchemaOpsWithValidationLevelAsAll(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        doTestSchemaOps(SchemaValidationLevel.ALL);
    }

    private void doTestSchemaOps(SchemaValidationLevel validationLevel) 
            throws IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, 
            SchemaBranchNotFoundException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(testName + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(testName + "-group")
                .description("Schema for " + testName)
                .validationLevel(validationLevel)
                .compatibility(SchemaCompatibility.BOTH)
                .build();

        Long id = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assertions.assertNotNull(id);

        // registering a new schema
        String schemaName = schemaMetadata.getName();
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc");
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));
        Assertions.assertNotNull(v1.getSchemaMetadataId());
        Assertions.assertEquals(1, v1.getVersion().intValue());

        SchemaMetadataInfo schemaMetadataInfoForId = schemaRegistryClient.getSchemaMetadataInfo(v1.getSchemaMetadataId());
        SchemaMetadataInfo schemaMetadataInfoForName = schemaRegistryClient.getSchemaMetadataInfo(schemaName);
        Assertions.assertEquals(schemaMetadataInfoForId, schemaMetadataInfoForName);

        // adding a new version of the schema using uploadSchemaVersion API
        SchemaIdVersion v2 = schemaRegistryClient.uploadSchemaVersion(schemaMetadata.getName(),
                                                                      "second version",
                                                                      AvroSchemaRegistryClientTest.class.getResourceAsStream("/schema-2.avsc"));
        Assertions.assertEquals(v1.getVersion() + 1, v2.getVersion().intValue());

        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2
                .getVersion()));
        SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
        Assertions.assertEquals(latest, schemaVersionInfo);

        Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
        Assertions.assertEquals(2, allVersions.size());

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "already added schema"));
        Assertions.assertEquals(version, v1);

        Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder()
                                                                                                               .name("md5")
                                                                                                               .build());
        Assertions.assertEquals(2, md5SchemaVersionKeys.size());

        Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder()
                                                                                                                .name("txid")
                                                                                                                .build());
        Assertions.assertEquals(1, txidSchemaVersionKeys.size());

        // checks we can update schema meta data.
        SchemaMetadata currentSchemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaName)
                                                                   .getSchemaMetadata();
        SchemaMetadata schemaMetadataToUpdateTo = new SchemaMetadata.Builder(currentSchemaMetadata).validationLevel(SchemaValidationLevel.LATEST)
                                                                                                   .build();
        SchemaMetadataInfo updatedSchemaMetadata = schemaRegistryClient.updateSchemaMetadata(schemaName, schemaMetadataToUpdateTo);

        Assertions.assertEquals(SchemaValidationLevel.LATEST, updatedSchemaMetadata.getSchemaMetadata()
                                                                               .getValidationLevel());

        schemaRegistryClient.disableSchemaVersion(v2.getSchemaVersionId());
        SchemaVersionInfo latestSchemaVersion = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);

        Assertions.assertEquals(v1.getSchemaVersionId(), latestSchemaVersion.getId());
    }


}
