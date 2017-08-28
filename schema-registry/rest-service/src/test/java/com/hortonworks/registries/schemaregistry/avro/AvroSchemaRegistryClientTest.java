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

import com.google.common.collect.Maps;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.test.IntegrationTest;
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
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.avro.util.CustomParameterizedRunner;
import com.hortonworks.registries.schemaregistry.avro.util.SchemaRegistryTestName;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING;
import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE;
import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@RunWith(CustomParameterizedRunner.class)
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest {

    private static final String INVALID_SCHEMA_PROVIDER_TYPE = "invalid-schema-provider-type";
    private static SchemaRegistryClient SCHEMA_REGISTRY_CLIENT;
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;
    private static Map<String, Object> SCHEMA_REGISTRY_CLIENT_CONF;

    @Rule
    public SchemaRegistryTestName TEST_NAME_RULE = new SchemaRegistryTestName();

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        SCHEMA_REGISTRY_CLIENT = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient(false);
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
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);
        String schemaName = schemaMetadata.getName();

        Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"), "Initial version of the schema"));
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"), "Second version of the schema"));
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-3.avsc"), "Third version of the schema, removes name field"));

        try {
            SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-4.avsc"), "Forth version of the schema, adds back name field, but different type"));
            Assert.fail("Should throw IncompatibleSchemaException as check against all schema's would find name field is not compatible with v1 and v2");
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
            SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-4.avsc"), "Forth version of the schema, adds back name field, but different type"));
        } catch (IncompatibleSchemaException ise) {
            Assert.fail("Should not throw IncompatibleSchemaException as check against only latest schema as such should ignore v1 and v2");
        }
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schema = "--- invalid schema ---";
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);
        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        String incompatSchema = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

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
            _testAvroSerDesGenericObj(protocol);
        }
    }

    private void _testAvroSerDesGenericObj(Byte protocolId) throws IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        Map<String, Object> config = Maps.newHashMap();
        config.putAll(SCHEMA_REGISTRY_CLIENT_CONF);
        config.put(SERDES_PROTOCOL_VERSION, protocolId);

        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        String deviceSchema = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);
        SchemaIdVersion v1 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, new SchemaVersion(deviceSchema, "Initial version of the schema"));
        Assert.assertNotNull(v1);

        Object deviceObject = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();

        byte[] serializedData = avroSnapshotSerializer.serialize(deviceObject, schemaMetadata);
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), null);
        Assert.assertEquals(deviceObject, deserializedObj);
    }

    @Test
    public void testAvroSerDePrimitives() throws Exception {
        for (Byte protocol : SerDesProtocolHandlerRegistry.get().getRegisteredSerDesProtocolHandlers().keySet()) {
            _testAvroSerDesPrimitives(protocol);
        }
    }

    private void _testAvroSerDesPrimitives(Byte protocolId) {
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
        String fileId = uploadFile();
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);

        SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/device.avsc"), "Initial version of the schema"));
        SerDesPair serDesPair = createSerDesInfo(fileId);
        Long serDesId = SCHEMA_REGISTRY_CLIENT.addSerDes(serDesPair);
        Assert.assertNotNull("Returned serDesId can not be null", serDesId);
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

    private String uploadFile() throws IOException {
        // upload a dummy file.
        File tmpJarFile = Files.createTempFile("foo", ".jar").toFile();
        tmpJarFile.deleteOnExit();
        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpJarFile)) {
            IOUtils.write(("Some random stuff: " + UUID.randomUUID()).getBytes(), fileOutputStream);
        }

        InputStream inputStream = new FileInputStream(tmpJarFile);
        return SCHEMA_REGISTRY_CLIENT.uploadFile(inputStream);
    }


    @Test
    public void testSchemaVersionDeletion() throws Exception {

        SchemaVersionKey schemaVersionKey = addAndDeleteSchemaVersion(TEST_NAME_RULE.getMethodName());

        Assert.assertTrue(SCHEMA_REGISTRY_CLIENT.getAllVersions(schemaVersionKey.getSchemaName()).isEmpty());
    }

    private SchemaVersionKey addAndDeleteSchemaVersion(String schemaName) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, IOException {
        SchemaMetadata schemaMetadata = createSchemaMetadata(schemaName, SchemaCompatibility.BOTH);
        SchemaIdVersion schemaIdVersion = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, new SchemaVersion(AvroSchemaRegistryClientUtil
                                                                                                                            .getSchema("/device.avsc"), "Initial version of the schema"));
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata.getName(), schemaIdVersion.getVersion());
        SCHEMA_REGISTRY_CLIENT.deleteSchemaVersion(schemaVersionKey);

        return schemaVersionKey;
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testDeletingNonExistingSchema() throws Exception {

        SchemaVersionKey schemaVersionKey = addAndDeleteSchemaVersion(TEST_NAME_RULE.getMethodName());

        // deleting again should return SchemaNotFoundException
        SCHEMA_REGISTRY_CLIENT.deleteSchemaVersion(schemaVersionKey);
    }

    @Test
    public void testSchemaVersionEnableState() throws Exception {
        IntStream.range(1, 100).forEach(x -> {
            try {
                System.out.println("########## Running for iter: " + x);
                doTestSchemaVersionEnableState(TEST_NAME_RULE.getMethodName() + x);
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
        SchemaIdVersion schemaIdVersion_1 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata,
                                                                                    new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        SchemaIdVersion schemaIdVersion_2 = SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaMetadata, incompatSchemaInfo);

        Long schemaVersionId_2 = schemaIdVersion_2.getSchemaVersionId();
        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaVersionId_2);
        SchemaVersionInfo schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_2);
        System.out.println("disable :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        Assert.assertEquals(SchemaVersionLifecycleStates.DISABLED.id(), schemaVersionInfo.getStateId());

        SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaVersionId_2);
        schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_2);
        System.out.println("enable :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        Assert.assertEquals(SchemaVersionLifecycleStates.ENABLED.id(), schemaVersionInfo.getStateId());

        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaVersionId_2);
        schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_2);
        System.out.println("disable :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        Assert.assertEquals(SchemaVersionLifecycleStates.DISABLED.id(), schemaVersionInfo.getStateId());

        SCHEMA_REGISTRY_CLIENT.archiveSchemaVersion(schemaVersionId_2);
        schemaVersionInfo = SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_2);
        System.out.println("archive :: schemaVersionInfo.getStateId() = " + schemaVersionInfo.getStateId());
        Assert.assertEquals(SchemaVersionLifecycleStates.ARCHIVED.id(), schemaVersionInfo.getStateId());
    }

    @Test
    public void testSchemaVersionLifeCycleStatesWithValidationAsLatest() throws Exception {
        doTestSchemaVersionLifeCycleStates(SchemaValidationLevel.LATEST);
    }

    @Test
    public void testSchemaVersionLifeCycleStatesWithValidationAsAll() throws Exception {
        doTestSchemaVersionLifeCycleStates(SchemaValidationLevel.ALL);
    }

    private void doTestSchemaVersionLifeCycleStates(SchemaValidationLevel validationLevel) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, IOException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(TEST_NAME_RULE.getMethodName() + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup("group")
                .compatibility(SchemaCompatibility.BOTH)
                .validationLevel(validationLevel)
                .build();
        String schemaName = schemaMetadata.getName();

        Long id = SCHEMA_REGISTRY_CLIENT.registerSchemaMetadata(schemaMetadata);

        SchemaIdVersion schemaIdVersion_1 =
                SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                                                        new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-1.avsc"),
                                                                          "Initial version of the schema"));
        SchemaIdVersion schemaIdVersion_2 =
                SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                                                        new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-2.avsc"),
                                                                          "Second version of the schema"));

        // disable version 2
        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaIdVersion_2.getSchemaVersionId());
        Assert.assertEquals(SchemaVersionLifecycleStates.DISABLED.id(),
                            SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_2).getStateId());

        // add version 3
        SchemaIdVersion schemaIdVersion_3 =
                SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                                                        new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-3.avsc"),
                                                                          "Third version of the schema, removes name field"));
        // enable version 2
        SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaIdVersion_2.getSchemaVersionId());
        Assert.assertEquals(SchemaVersionLifecycleStates.ENABLED.id(),
                            SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_2).getStateId());

        SchemaIdVersion schemaIdVersion_4 =
                SCHEMA_REGISTRY_CLIENT.addSchemaVersion(schemaName,
                                                        new SchemaVersion(AvroSchemaRegistryClientUtil.getSchema("/schema-4.avsc"),
                                                                          "Forth version of the schema, adds back name field, but different type",
                                                                          SchemaVersionLifecycleStates.INITIATED.id()));
        // enable version 4
        try {
            SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaIdVersion_4.getSchemaVersionId());
            Assert.fail("Enabling "+schemaIdVersion_4+" should have failed with incompatible schema error");
        } catch (IncompatibleSchemaException e) {
        }
        Assert.assertEquals(SchemaVersionLifecycleStates.INITIATED.id(),
                            SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_4).getStateId());

        // disable version 3
        SCHEMA_REGISTRY_CLIENT.disableSchemaVersion(schemaIdVersion_3.getSchemaVersionId());
        Assert.assertEquals(SchemaVersionLifecycleStates.DISABLED.id(),
                            SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_3).getStateId());

        // enable version 3
        SCHEMA_REGISTRY_CLIENT.enableSchemaVersion(schemaIdVersion_3.getSchemaVersionId());
        Assert.assertEquals(SchemaVersionLifecycleStates.ENABLED.id(),
                            SCHEMA_REGISTRY_CLIENT.getSchemaVersionInfo(schemaIdVersion_3).getStateId());
    }

}
