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
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING;
import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE;
import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest extends AbstractAvroSchemaRegistryCientTest {

    private static final String INVALID_SCHEMA_PROVIDER_TYPE = "invalid-schema-provider-type";

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
            scenario.testCreate(schemaRegistryClient);
        }
    }

    @Test
    public void testSchemaOps() throws Exception {
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);

        Long id = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(id);

        // registering a new schema
        String schemaName = schemaMetadata.getName();
        String schema1 = getSchema("/schema-1.avsc");
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));
        Assert.assertNotNull(v1.getSchemaMetadataId());
        Assert.assertEquals(1, v1.getVersion().intValue());

        SchemaMetadataInfo schemaMetadataInfoForId = schemaRegistryClient.getSchemaMetadataInfo(v1.getSchemaMetadataId());
        SchemaMetadataInfo schemaMetadataInfoForName = schemaRegistryClient.getSchemaMetadataInfo(schemaName);
        Assert.assertEquals(schemaMetadataInfoForId, schemaMetadataInfoForName);

        // adding a new version of the schema using uploadSchemaVersion API
        SchemaIdVersion v2 = schemaRegistryClient.uploadSchemaVersion(schemaMetadata.getName(),
                                                                      "second version",
                                                                      AvroSchemaRegistryClientTest.class.getResourceAsStream("/schema-2.avsc"));
        Assert.assertEquals(v1.getVersion() + 1, v2.getVersion().intValue());

        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2.getVersion()));
        SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
        Assert.assertEquals(latest, schemaVersionInfo);

        Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
        Assert.assertEquals(2, allVersions.size());

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "already added schema"));
        Assert.assertEquals(version, v1);

        Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("md5").build());
        Assert.assertEquals(2, md5SchemaVersionKeys.size());

        Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("txid").build());
        Assert.assertEquals(1, txidSchemaVersionKeys.size());

        // checks we can update schema meta data.
        SchemaMetadata currentSchemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaName).getSchemaMetadata();
        SchemaMetadata schemaMetadataToUpdateTo = new SchemaMetadata.Builder(currentSchemaMetadata).validationLevel(SchemaValidationLevel.LATEST).build();
        SchemaMetadataInfo updatedSchemaMetadata = schemaRegistryClient.updateSchemaMetadata(schemaName, schemaMetadataToUpdateTo);

        Assert.assertEquals(SchemaValidationLevel.LATEST, updatedSchemaMetadata.getSchemaMetadata().getValidationLevel());
    }

    @Test
    public void testValidationLevels() throws Exception {
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);
        String schemaName = schemaMetadata.getName();

        Long id = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(getSchema("/schema-1.avsc"), "Initial version of the schema"));
        schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(getSchema("/schema-2.avsc"), "Second version of the schema"));
        schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(getSchema("/schema-3.avsc"), "Third version of the schema, removes name field"));

        try {
            schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(getSchema("/schema-4.avsc"), "Forth version of the schema, adds back name field, but different type"));
            Assert.fail("Should throw IncompatibleSchemaException as check against all schema's would find name field is not compatible with v1 and v2");
        } catch (IncompatibleSchemaException ise) {
            //expected
        }

        SchemaMetadata currentSchemaMetadata = schemaRegistryClient.getSchemaMetadataInfo(schemaName).getSchemaMetadata();
        SchemaMetadata schemaMetadataToUpdateTo = new SchemaMetadata.Builder(currentSchemaMetadata).validationLevel(SchemaValidationLevel.LATEST).build();
        SchemaMetadataInfo updatedSchemaMetadata = schemaRegistryClient.updateSchemaMetadata(schemaName, schemaMetadataToUpdateTo);

        Assert.assertEquals(SchemaValidationLevel.LATEST, updatedSchemaMetadata.getSchemaMetadata().getValidationLevel());

        try {
            schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(getSchema("/schema-4.avsc"), "Forth version of the schema, adds back name field, but different type"));
        } catch (IncompatibleSchemaException ise) {
            Assert.fail("Should not throw IncompatibleSchemaException as check against only latest schema as such should ignore v1 and v2");
        }
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schema = "--- invalid schema ---";
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);
        schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        schemaRegistryClient.addSchemaVersion(schemaMetadata, incompatSchemaInfo);
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
        Object defaultSerializer = schemaRegistryClient.getDefaultSerializer(AvroSchemaProvider.TYPE);
        Object defaultDeserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);
        Assert.assertEquals(AvroSnapshotDeserializer.class, defaultDeserializer.getClass());
        Assert.assertEquals(AvroSnapshotSerializer.class, defaultSerializer.getClass());
    }

    @Test(expected = Exception.class)
    public void testInvalidTypeForDefaultSer() throws Exception {
        schemaRegistryClient.getDefaultSerializer(INVALID_SCHEMA_PROVIDER_TYPE);
    }

    @Test(expected = Exception.class)
    public void testInvalidTypeForDefaultDes() throws Exception {
        schemaRegistryClient.getDefaultDeserializer(INVALID_SCHEMA_PROVIDER_TYPE);
    }

    @Test
    public void testAvroSerDesGenericObj() throws Exception {
        for (Byte protocol : SerDesProtocolHandlerRegistry.get().getRegisteredSerDesProtocolHandlers().keySet()) {
            _testAvroSerDesGenericObj(protocol);
        }
    }

    private void _testAvroSerDesGenericObj(Byte protocolId) throws IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        Map<String, Object> config = Maps.newHashMap();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), rootUrl);
        config.put(SERDES_PROTOCOL_VERSION, protocolId);

        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        String deviceSchema = getSchema("/device.avsc");
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(deviceSchema, "Initial version of the schema"));
        Assert.assertNotNull(v1);

        Object deviceObject = createGenericRecordForDevice();

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
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), rootUrl);
        config.put(SERDES_PROTOCOL_VERSION, protocolId);

        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object[] objects = generatePrimitivePayloads();
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

        schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(getSchema("/device.avsc"), "Initial version of the schema"));
        SerDesPair serDesPair = createSerDesInfo(fileId);
        Long serDesId = schemaRegistryClient.addSerDes(serDesPair);
        Assert.assertNotNull("Returned serDesId can not be null", serDesId);
        String schemaName = schemaMetadata.getName();
        schemaRegistryClient.mapSchemaWithSerDes(schemaName, serDesId);
        Collection<SerDesInfo> serializers = schemaRegistryClient.getSerDes(schemaName);

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
        return schemaRegistryClient.uploadFile(inputStream);
    }

    @Test
    public void testSchemaVersionDeletion() throws Exception {

        SchemaVersionKey schemaVersionKey = addAndDeleteSchemaVersion(TEST_NAME_RULE.getMethodName());

        Assert.assertTrue(schemaRegistryClient.getAllVersions(schemaVersionKey.getSchemaName()).isEmpty());
    }

    private SchemaVersionKey addAndDeleteSchemaVersion(String schemaName) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, IOException {
        SchemaMetadata schemaMetadata = createSchemaMetadata(schemaName, SchemaCompatibility.BOTH);
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(getSchema("/device.avsc"), "Initial version of the schema"));
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaMetadata.getName(), schemaIdVersion.getVersion());
        schemaRegistryClient.deleteSchemaVersion(schemaVersionKey);

        return schemaVersionKey;
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testDeletingNonExistingSchema() throws Exception {

        SchemaVersionKey schemaVersionKey = addAndDeleteSchemaVersion(TEST_NAME_RULE.getMethodName());

        // deleting again should return SchemaNotFoundException
        schemaRegistryClient.deleteSchemaVersion(schemaVersionKey);
    }

}
