/**
 * Copyright 2016-2019 Cloudera, Inc.
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

import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING;
import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE;
import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.shaded.javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.state.InbuiltSchemaVersionLifecycleState;
import com.hortonworks.registries.shaded.javax.ws.rs.BadRequestException;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;

import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.JarFileFactory;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateTransition;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.util.CustomParameterizedRunner;
import com.hortonworks.registries.util.SchemaRegistryTestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(CustomParameterizedRunner.class)
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaRegistryClientTest.class);

    private static final String INVALID_SCHEMA_PROVIDER_TYPE = "invalid-schema-provider-type";
    private static SchemaRegistryClient SCHEMA_REGISTRY_CLIENT;
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;
    private static Map<String, Object> SCHEMA_REGISTRY_CLIENT_CONF;

    @Rule
    public SchemaRegistryTestName testNameRule = new SchemaRegistryTestName();

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT,
                             SchemaRegistryTestProfileType.SSL,
                             SchemaRegistryTestProfileType.ONE_WAY_SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        SCHEMA_REGISTRY_CLIENT = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient();
        SCHEMA_REGISTRY_CLIENT_CONF = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.exportClientConf();
    }

    @CustomParameterizedRunner.AfterParam
    public static void afterParam() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }

    public AvroSchemaRegistryClientTest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {

    }

    /**
     * Class to describe schema create operation failure scenarios
     */
    private static class SchemaCreateFailureScenario {
        private final String test;
        private final String name;
        private final String group;
        private final String type;
        private final Response.Status expectedHttpResponse;
        private final CatalogResponse.ResponseMessage expectedCatalogResponse;

        SchemaCreateFailureScenario(String test, String name, String group, String type, Response.Status httpResponse,
                                    CatalogResponse.ResponseMessage catalogResponse) {
            this.test = test;
            this.name = name;
            this.group = group;
            this.type = type;
            expectedHttpResponse = httpResponse;
            expectedCatalogResponse = catalogResponse;
        }

        /**
         * Return true if the schema creation failed as expected
         */
        private void testCreate(SchemaRegistryClient client) {
            boolean failedAsExpected = false;
            SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(name).type(type).schemaGroup(group).
                    description("description").build();
            try {
                client.registerSchemaMetadata(schemaMetadata);
            } catch (BadRequestException ex) {
                Response resp = ex.getResponse();
                Assert.assertEquals(test + " - http response unexpected", expectedHttpResponse.getStatusCode(), resp.getStatus());
                CatalogResponse catalogResponse = SchemaRegistryClient.readCatalogResponse(resp.readEntity(String.class));
                Assert.assertEquals(test + " - catalog response unexpected",
                                    expectedCatalogResponse.getCode(), catalogResponse.getResponseCode());
                failedAsExpected = true;
            }
            Assert.assertTrue(test + " - did not fail as expected", failedAsExpected);
        }
    }

    /**
     * Tests for various schema create failure scenarios
     */
    private static SchemaCreateFailureScenario[] createFailureScenarios =
            {
                    // No schema type specified
                    new SchemaCreateFailureScenario("Test empty schema type", "name", "group", "",
                                                    BAD_REQUEST, BAD_REQUEST_PARAM_MISSING),
                    // Schema type is white spaces
                    new SchemaCreateFailureScenario("Test empty schema white spaces", "name", "group", "   ",
                                                    BAD_REQUEST, BAD_REQUEST_PARAM_MISSING),
                    // Invalid schema type
                    new SchemaCreateFailureScenario("Test invalid schema type", "name", "group", "invalid",
                                                    BAD_REQUEST, UNSUPPORTED_SCHEMA_TYPE),
                    // No schema name
                    new SchemaCreateFailureScenario("Test empty schema name", "", "group", AvroSchemaProvider.TYPE,
                                                    BAD_REQUEST, BAD_REQUEST_PARAM_MISSING),
                    // Schema name is white spaces
                    new SchemaCreateFailureScenario("Test schema name white spaces", "    ", "group", AvroSchemaProvider.TYPE,
                                                    BAD_REQUEST, BAD_REQUEST_PARAM_MISSING)
            };

    @Test
    public void testSchemaCreateFailures() throws Exception {
        // Run through all the tests related to schema create failure scenarios
        for (SchemaCreateFailureScenario scenario : createFailureScenarios) {
            scenario.testCreate(SCHEMA_REGISTRY_CLIENT);
        }
    }

    @Test
    public void testValidationLevels() throws Exception {
        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BOTH);
        String schemaName = schemaMetadata.getName();

        Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"),
                "Initial version of the schema"));
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"),
                "Second version of the schema"));
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-3.avsc"),
                "Third version of the schema, removes name field"));

        try {
            SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, 
                    new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-4.avsc"), 
                            "Forth version of the schema, adds back name field, but different type"));
            Assert.fail("Should throw IncompatibleSchemaException as check against all schema's would find name " +
                    "field is not compatible with v1 and v2");
        } catch (IncompatibleSchemaException ise) {
            //expected
        }

        SchemaMetadata currentSchemaMetadata = SCHEMA_REGISTRY_CLIENT.getSchemaMetadataInfo(schemaName)
                                                                     .getSchemaMetadata();
        SchemaMetadata schemaMetadataToUpdateTo = new SchemaMetadata.Builder(currentSchemaMetadata).validationLevel(SchemaValidationLevel.LATEST)
                                                                                                   .build();
        SchemaMetadataInfo updatedSchemaMetadata = SCHEMA_REGISTRY_CLIENT.updateSchemaMetadata(schemaName, schemaMetadataToUpdateTo);

        Assert.assertEquals(SchemaValidationLevel.LATEST, updatedSchemaMetadata.getSchemaMetadata()
                                                                               .getValidationLevel());

        try {
            SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, 
                    new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-4.avsc"), 
                            "Forth version of the schema, adds back name field, but different type"));
        } catch (IncompatibleSchemaException ise) {
            Assert.fail("Should not throw IncompatibleSchemaException as check against only latest schema as such should ignore v1 and v2");
        }
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schema = "--- invalid schema ---";
        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BACKWARD);
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        String incompatSchema = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, incompatSchemaInfo);
    }

    private SchemaMetadata createSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(schemaDesc + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(schemaDesc + "-group")
                .description("Schema for " + schemaDesc)
                .compatibility(compatibility)
                .build();
    }

    @Test
    public void testDefaultSerDes() throws Exception {
        Object defaultSerializer = SCHEMA_REGISTRY_CLIENT.getDefaultSerializer(AvroSchemaProvider.TYPE);
        Object defaultDeserializer = SCHEMA_REGISTRY_CLIENT.getDefaultDeserializer(AvroSchemaProvider.TYPE);
        Assert.assertEquals(AvroSnapshotDeserializer.class, defaultDeserializer.getClass());
        Assert.assertEquals(AvroSnapshotSerializer.class, defaultSerializer.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTypeForDefaultSer() throws Exception {
        SCHEMA_REGISTRY_CLIENT.getDefaultSerializer(INVALID_SCHEMA_PROVIDER_TYPE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTypeForDefaultDes() throws Exception {
        SCHEMA_REGISTRY_CLIENT.getDefaultDeserializer(INVALID_SCHEMA_PROVIDER_TYPE);
    }

    @Test
    public void testAvroSerDesGenericObj() throws Exception {
        for (Byte protocol : SerDesProtocolHandlerRegistry.get().getRegisteredSerDesProtocolHandlers().keySet()) {
            testAvroSerDesGenericObj(protocol);
        }
    }

    private void testAvroSerDesGenericObj(Byte protocolId) 
            throws IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        Map<String, Object> config = Maps.newHashMap();
        config.putAll(SCHEMA_REGISTRY_CLIENT_CONF);
        config.put(SERDES_PROTOCOL_VERSION, protocolId);

        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        String deviceSchema = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BOTH);
        SchemaIdVersion v1 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, 
                new SchemaVersion(deviceSchema, "Initial version of the schema"));
        assertNotNull(v1);

        Object deviceObject = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();

        byte[] serializedData = avroSnapshotSerializer.serialize(deviceObject, schemaMetadata);
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), null);
        Assert.assertEquals(deviceObject, deserializedObj);
    }

    @Test
    public void testAvroSerDePrimitives() throws Exception {
        for (Byte protocol : SerDesProtocolHandlerRegistry.get().getRegisteredSerDesProtocolHandlers().keySet()) {
            testAvroSerDesPrimitives(protocol);
        }
    }

    private void testAvroSerDesPrimitives(Byte protocolId) {
        Map<String, Object> config = Maps.newHashMap();
        config.putAll(SCHEMA_REGISTRY_CLIENT_CONF);
        config.put(SERDES_PROTOCOL_VERSION, protocolId);

        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object[] objects = AvroSchemaRegistryClientUtil.generatePrimitivePayloads();
        for (Object obj : objects) {
            String name = obj != null ? obj.getClass().getName() : Void.TYPE.getName();
            SchemaMetadata schemaMetadata = createSchemaMetadata(name, SchemaCompatibility.BOTH);
            byte[] serializedData = avroSnapshotSerializer.serialize(obj, schemaMetadata);

            Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), null);

            if (obj instanceof byte[]) {
                Assert.assertArrayEquals((byte[]) obj, (byte[]) deserializedObj);
            } else if (deserializedObj instanceof Utf8) {
                Assert.assertEquals(obj, deserializedObj.toString());
            } else {
                Assert.assertEquals(obj, deserializedObj);
            }
        }
    }

    @Test
    public void testSerializerOps() throws Exception {
        String fileId = uploadValidFile();
        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BOTH);

        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, 
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/device.avsc"), "Initial version of the schema"));
        SerDesPair serDesPair = createSerDesInfo(fileId);
        Long serDesId = SCHEMA_REGISTRY_CLIENT.addSerDes(serDesPair);
        assertNotNull("Returned serDesId can not be null", serDesId);
        String schemaName = schemaMetadata.getName();
        SCHEMA_REGISTRY_CLIENT.mapSchemaWithSerDes(schemaName, serDesId);
        Collection<SerDesInfo> serializers = SCHEMA_REGISTRY_CLIENT.getSerDes(schemaName);

        Assert.assertTrue(serializers.stream()
                                     .map(x -> x.getSerDesPair())
                                     .collect(Collectors.toList())
                                     .contains(serDesPair));
    }

    private SerDesPair createSerDesInfo(String fileId) {
        return new SerDesPair(
                "avro serializer",
                "avro serializer",
                fileId,
                "con.hwx.registries.serializer.AvroSnapshotSerializer",
                "con.hwx.registries.serializer.AvroSnapshotDeserializer"
        );
    }

    private String uploadValidFile() throws IOException {
        InputStream inputStream = new FileInputStream(JarFileFactory.createValidJar());
        return SCHEMA_REGISTRY_CLIENT.uploadFile(inputStream);
    }


    @Test
    public void testSchemaVersionDeletion() throws Exception {

        SchemaVersionKey schemaVersionKey = addAndDeleteSchemaVersion(testNameRule.getMethodName());

        Assert.assertTrue(SCHEMA_REGISTRY_CLIENT.getAllVersions(schemaVersionKey.getSchemaName()).isEmpty());
    }

    private SchemaVersionKey addAndDeleteSchemaVersion(String schemaName) 
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, IOException, 
            SchemaBranchNotFoundException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = createSchemaMetadata(schemaName, SchemaCompatibility.BOTH);
        SchemaIdVersion schemaIdVersion = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, 
                new SchemaVersion(AvroSchemaRegistryClientUtil
                        .getSchema("/device.avsc"), "Initial version of the schema"));
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata.getName(), schemaIdVersion.getVersion());
        SCHEMA_REGISTRY_CLIENT.deleteSchemaVersion(schemaVersionKey);

        return schemaVersionKey;
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testDeletingNonExistingSchema() throws Exception {

        SchemaVersionKey schemaVersionKey = addAndDeleteSchemaVersion(testNameRule.getMethodName());

        // deleting again should return SchemaNotFoundException
        SCHEMA_REGISTRY_CLIENT.deleteSchemaVersion(schemaVersionKey);
    }

    @Test
    public void testSchemaVersionEnableState() throws Exception {
        IntStream.range(1, 100).forEach(x -> {
            try {
                LOG.trace("########## Running for iter: {}", x);
                doTestSchemaVersionEnableState(testNameRule.getMethodName() + x);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void doTestSchemaVersionEnableState(String schemaName) throws Exception {
        String schema = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        String incompatSchema = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata(schemaName, SchemaCompatibility.BACKWARD);

        // registering a new schema
        SchemaIdVersion schemaIdVersion1 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata,
                                                                                    new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        SchemaIdVersion schemaIdVersion2 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, incompatSchemaInfo);

        Long schemaVersionId2 = schemaIdVersion2.getSchemaVersionId();
        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaVersionId2);
        SchemaVersionInfo schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2);
        System.out.println("disable :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        assertEquals(SchemaVersionLifecycleStates.DISABLED, schemaVersionInfo);

        SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaVersionId2);
        schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2);
        System.out.println("enable :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        assertEquals(SchemaVersionLifecycleStates.ENABLED, schemaVersionInfo);

        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaVersionId2);
        schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2);
        System.out.println("disable :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        assertEquals(SchemaVersionLifecycleStates.DISABLED, schemaVersionInfo);

        SCHEMA_REGISTRY_CLIENT.archiveSchemaVersion(schemaVersionId2);
        schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2);
        System.out.println("archive :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        assertEquals(SchemaVersionLifecycleStates.ARCHIVED, schemaVersionInfo);
    }

    @Test
    public void testSchemaVersionLifeCycleStatesWithValidationAsLatest() throws Exception {
        doTestSchemaVersionLifeCycleStates(SchemaValidationLevel.LATEST);
    }

    @Test
    public void testSchemaVersionLifeCycleStatesWithValidationAsAll() throws Exception {
        doTestSchemaVersionLifeCycleStates(SchemaValidationLevel.ALL);
    }

    private void doTestSchemaVersionLifeCycleStates(SchemaValidationLevel validationLevel)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, 
            IOException, SchemaLifecycleException, SchemaBranchNotFoundException {

        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(testNameRule.getMethodName() + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup("group")
                .compatibility(SchemaCompatibility.BOTH)
                .validationLevel(validationLevel)
                .build();
        String schemaName = schemaMetadata.getName();

        Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);

        SchemaIdVersion schemaIdVersion1 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"), "Initial version of the schema"));
        SchemaIdVersion schemaIdVersion2 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"), "Second version of the schema"));

        // disable version 2
        LOG.info("Disable version {}", schemaIdVersion2.getSchemaVersionId());
        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaIdVersion2.getSchemaVersionId());
        assertEquals(SchemaVersionLifecycleStates.DISABLED, SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2));

        // add version 3
        SchemaIdVersion schemaIdVersion3 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-3.avsc"), "Third version of the schema, removes name field"));
        LOG.info("Added version {} with unique id {}", schemaIdVersion3.getVersion(), schemaIdVersion3.getSchemaVersionId());
        // enable version 2
        LOG.info("Enable schema version {} with unique id {}", schemaIdVersion2.getVersion(), schemaIdVersion2.getSchemaVersionId());
        SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaIdVersion2.getSchemaVersionId());
        assertEquals(SchemaVersionLifecycleStates.ENABLED, SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2));

        // disable version 3
        LOG.info("Disable version {}", schemaIdVersion3.getSchemaVersionId());
        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaIdVersion3.getSchemaVersionId());
        assertEquals(SchemaVersionLifecycleStates.DISABLED, SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion3));

        // enable version 3
        LOG.info("Enable schema version {}", schemaIdVersion3.getSchemaVersionId());
        SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaIdVersion3.getSchemaVersionId());
        assertEquals(SchemaVersionLifecycleStates.ENABLED, SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion3));
    }

    private static void assertEquals(InbuiltSchemaVersionLifecycleState expected, SchemaVersionInfo actualState) {
        assertNotNull("Null state returned by the client", actualState);
        assertNotNull("Null state id returned by the client", actualState.getStateId());
        Byte actualStateId = actualState.getStateId();
        String msg = String.format("Expecting state %s (%s) and we got actually: %s (%s)", expected.getId(), expected.getName(),
                actualStateId, SchemaVersionLifecycleStates.valueOf(actualStateId).getName());
        Assert.assertEquals(msg, expected.getId(), actualStateId);
    }

    @Test
    public void testSchemaVersionLifeCycleStateMachineConfig() throws Exception {
        SchemaVersionLifecycleStateMachineInfo stateMachineInfo =
                SCHEMA_REGISTRY_CLIENT.getSchemaVersionLifecycleStateMachineInfo();
        ObjectMapper objectMapper = new ObjectMapper();
        String stateMachineAsStr = objectMapper.writeValueAsString(stateMachineInfo);
        SchemaVersionLifecycleStateMachineInfo readStateMachineInfo =
                objectMapper.readValue(stateMachineAsStr, SchemaVersionLifecycleStateMachineInfo.class);

        Assert.assertEquals(readStateMachineInfo, stateMachineInfo);

        // check for duplicate state/transitions
        checkDuplicateEntries(stateMachineInfo.getStates());
        checkDuplicateEntries(stateMachineInfo.getTransitions());
    }

    private <T> void checkDuplicateEntries(Collection<T> states) {
        HashSet<T> statesSet = new HashSet<>();
        for (T state : states) {
            if (!statesSet.add(state)) {
                Assert.fail("stateMachineInfo contains duplicate state: " + state);
            }
        }
    }

    @Test
    public void testSchemaVersionStatesThroughIds() throws Exception {
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(testNameRule.getMethodName() + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup("group")
                .compatibility(SchemaCompatibility.BOTH)
                .build();
        String schemaName = schemaMetadata.getName();

        // build nextTransitions from state machine
        SchemaVersionLifecycleStateMachineInfo stateMachine = SCHEMA_REGISTRY_CLIENT.getSchemaVersionLifecycleStateMachineInfo();
        Map<Byte, List<SchemaVersionLifecycleStateTransition>> nextTransitionsForStateIds = new HashMap<>();
        for (SchemaVersionLifecycleStateTransition transition : stateMachine.getTransitions()) {
            List<SchemaVersionLifecycleStateTransition> nextTransitions =
                    nextTransitionsForStateIds.computeIfAbsent(transition.getSourceStateId(), aByte -> new ArrayList<>());
            nextTransitions.add(transition);
        }

        Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);

        SchemaIdVersion schemaIdVersion1 =
                SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                                                        new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"),
                                                                          "Initial version of the schema"));
        SchemaIdVersion schemaIdVersion2 =
                SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                                                        new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"),
                                                                          "Second version of the schema"));

        // disable version 2
        SchemaVersionInfo schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2);
        Byte stateId = schemaVersionInfo.getStateId();
        List<SchemaVersionLifecycleStateTransition> nextTransitions = nextTransitionsForStateIds.get(stateId);

        Byte targetStateId = nextTransitions.get(0).getTargetStateId();
        SCHEMA_REGISTRY_CLIENT.transitionState(schemaVersionInfo.getId(), targetStateId, null);

        assertEquals(SchemaVersionLifecycleStates.valueOf(targetStateId), SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion2));
    }

    @Test
    public void testGetSchemaVersionFromStates() 
            throws IOException, SchemaNotFoundException, InvalidSchemaException, 
            IncompatibleSchemaException, SchemaLifecycleException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schemaName = schemaMetadata.getName();

        Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);
        SchemaIdVersion v1 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, 
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"), "Initial version of the schema"));
        SchemaIdVersion v2 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, 
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"), "Second version of the schema"));
        SchemaIdVersion v3 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, 
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-3.avsc"), 
                        "Third version of the schema, removes name field"));

        SchemaBranch schemaBranch = SCHEMA_REGISTRY_CLIENT.createSchemaBranch(v3.getSchemaVersionId(), new SchemaBranch("Branch-1", schemaName));
        SchemaIdVersion v4 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaBranch.getName(), schemaName, 
                new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-4.avsc"), 
                        "Forth version of the schema, adds back name field, but different type"));
        SCHEMA_REGISTRY_CLIENT.startSchemaVersionReview(v4.getSchemaVersionId());
        SCHEMA_REGISTRY_CLIENT.transitionState(v4.getSchemaVersionId(), SchemaVersionLifecycleStates.REVIEWED.getId(), null);

        SCHEMA_REGISTRY_CLIENT.archiveSchemaVersion(v2.getSchemaVersionId());
        SCHEMA_REGISTRY_CLIENT.archiveSchemaVersion(v3.getSchemaVersionId());

        Assert.assertEquals(
                transformToSchemaIdVersions(SCHEMA_REGISTRY_CLIENT.getAllVersions(SchemaBranch.MASTER_BRANCH, 
                        schemaName, Collections.singletonList(SchemaVersionLifecycleStates.ENABLED.getId()))),
                new HashSet<>(Arrays.asList(v1)));
        Assert.assertEquals(
                transformToSchemaIdVersions(SCHEMA_REGISTRY_CLIENT.getAllVersions(SchemaBranch.MASTER_BRANCH, 
                        schemaName, Collections.singletonList(SchemaVersionLifecycleStates.ARCHIVED.getId()))),
                new HashSet<>(Arrays.asList(v2, v3)));
        Assert.assertEquals(
                transformToSchemaIdVersions(SCHEMA_REGISTRY_CLIENT.getAllVersions(schemaBranch.getName(), 
                        schemaName, Collections.singletonList(SchemaVersionLifecycleStates.REVIEWED.getId()))),
                new HashSet<>(Arrays.asList(v4)));

        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(v1.getSchemaVersionId());
        Assert.assertEquals(
                transformToSchemaIdVersions(SCHEMA_REGISTRY_CLIENT.getAllVersions(SchemaBranch.MASTER_BRANCH, schemaName,
                        Arrays.asList(SchemaVersionLifecycleStates.ARCHIVED.getId(), SchemaVersionLifecycleStates.DISABLED.getId()))),
                new HashSet<>(Arrays.asList(v1, v2, v3)));
    }


    private Set<SchemaIdVersion> transformToSchemaIdVersions(Collection<SchemaVersionInfo> versionInfos) {
        return versionInfos.stream().map(versionInfo -> new SchemaIdVersion(versionInfo.getSchemaMetadataId(), 
                versionInfo.getVersion(), versionInfo.getId()))
                .collect(Collectors.toSet());
    }

    @Test
    public void testFindAggregatedSchemas() throws Exception {
        SchemaMetadata schemaMetadata = createSchemaMetadata("apples", SchemaCompatibility.BOTH);
        final String schema1 = schemaMetadata.getName();
        final Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schema1, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"), "Initial version of the schema"));
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schema1, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"), "Second version of the schema"));

        SchemaMetadata schemaMetadata2 = createSchemaMetadata("oranges", SchemaCompatibility.BACKWARD);
        final String schema2 = schemaMetadata2.getName();
        final Long id2 = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata2);
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schema2, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"), "Initial version of the schema"));

        List<AggregatedSchemaMetadataInfo> result1 = SCHEMA_REGISTRY_CLIENT.findAggregatedSchemas(schema1, null, null);
        Assert.assertNotNull(result1);
        Assert.assertFalse(result1.isEmpty());
        Assert.assertEquals(1, result1.size());
        Assert.assertEquals(id, result1.get(0).getId());

        List<AggregatedSchemaMetadataInfo> result2 = SCHEMA_REGISTRY_CLIENT.findAggregatedSchemas(schema2, null, null);
        Assert.assertNotNull(result2);
        Assert.assertFalse(result2.isEmpty());
        Assert.assertEquals(1, result2.size());
        Assert.assertEquals(id2, result2.get(0).getId());

        List<AggregatedSchemaMetadataInfo> bothSchemas = SCHEMA_REGISTRY_CLIENT.findAggregatedSchemas(null, null, null);
        Assert.assertNotNull(bothSchemas);
        Assert.assertFalse(bothSchemas.isEmpty());
        Assert.assertTrue(bothSchemas.size() >= 2);
        Assert.assertTrue(bothSchemas.stream().anyMatch(s -> s.getId().equals(id)));
        Assert.assertTrue(bothSchemas.stream().anyMatch(s -> s.getId().equals(id2)));

        List<AggregatedSchemaMetadataInfo> notFound = SCHEMA_REGISTRY_CLIENT.findAggregatedSchemas("differentSchema", null, null);
        Assert.assertNotNull(notFound);
        Assert.assertTrue(notFound.isEmpty());
    }

}
