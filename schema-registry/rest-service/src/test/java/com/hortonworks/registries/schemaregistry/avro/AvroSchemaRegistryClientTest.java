/**
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
 **/
package com.hortonworks.registries.schemaregistry.avro;

import com.google.common.collect.Iterables;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.BAD_REQUEST_PARAM_MISSING;
import static com.hortonworks.registries.common.catalog.CatalogResponse.ResponseMessage.UNSUPPORTED_SCHEMA_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest extends AbstractAvroSchemaRegistryCientTest {

    private static final String INVALID_SCHEMA_PROVIDER_TYPE = "invalid-schema-provider-type";

    /** Class to describe schema create operation failure scenarios */
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

        /** Return true if the schema creation failed as expected */
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
        createSchema(schemaMetadata, schemaRegistryClient);

        // Add first version of the schema
        String schemaName = schemaMetadata.getName();
        int expectedVersion = 1;
        String schemaTxt1 = getSchemaTxt("/schema-1.avsc");
        String descriptionV1 = schemaName + "v1";
        SchemaIdVersion v1 = addSchemaVersion(schemaRegistryClient, schemaName, schemaTxt1, descriptionV1, expectedVersion);

        // Add a new version of the schema
        String schemaTxt2 = getSchemaTxt("/schema-2.avsc");
        expectedVersion = v1.getVersion() + 1;
        String descriptionV2 = schemaName + "v2";
        SchemaIdVersion v2 = addSchemaVersion(schemaRegistryClient, schemaName, schemaTxt2, descriptionV2, expectedVersion);

        // Get all the versions of the schema and check the returned values
        Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
        Assert.assertEquals(2, allVersions.size());
        for (SchemaVersionInfo versionInfo : allVersions) {
            int version = versionInfo.getVersion();
            if (version == v1.getVersion()) {
                assertEquals(versionInfo, schemaTxt1, descriptionV1);
            } else {
                Assert.assertEquals(v2.getVersion().intValue(), version);
                assertEquals(versionInfo, schemaTxt2, descriptionV2);
            }
        }

        // Search for field "md5". It exists in both schemaTxt1 (version v1) and schemaTxt2 (version v2)
        Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("md5").build());
        Assert.assertEquals(2, md5SchemaVersionKeys.size());
        for (SchemaVersionKey key : md5SchemaVersionKeys) {
            int version = key.getVersion();
            Assert.assertTrue(v1.getVersion() == version || v2.getVersion() == version);
            Assert.assertEquals(schemaName, key.getSchemaName());
        }

        // Search for field "txid". It exists in schemaTxt2
        Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("txid").build());
        Assert.assertEquals(1, txidSchemaVersionKeys.size());
        SchemaVersionKey key = Iterables.get(txidSchemaVersionKeys, 0);
        Assert.assertEquals(v2.getVersion(), key.getVersion());
        Assert.assertEquals(schemaName, key.getSchemaName());
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schemaTxt = "--- invalid schema ---";
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);
        schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schemaTxt, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schemaTxt = getSchemaTxt("/device.avsc");
        String incompatSchemaTxt = getSchemaTxt("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // Add a new version of the schema
        schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schemaTxt, "Initial version of the schema"));

        // Adding incompatible new version of the schema and check exception is thrown
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchemaTxt, "second version");
        schemaRegistryClient.addSchemaVersion(schemaMetadata, incompatSchemaInfo);
    }

    private static SchemaMetadata createSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
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
        Assert.assertEquals(AvroSnapshotDeserializer.class , defaultDeserializer.getClass());
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
    public void testAvroSerDeGenericObj() throws Exception {
        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), rootUrl);
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        String deviceSchemaTxt = getSchemaTxt("/device.avsc");
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(deviceSchemaTxt, "Initial version of the schema"));
        Assert.assertNotNull(v1);

        Object deviceObject = createGenericRecordForDevice();

        byte[] serializedData = avroSnapshotSerializer.serialize(deviceObject, schemaMetadata);
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), schemaMetadata, null);
        Assert.assertEquals(deviceObject, deserializedObj);
    }

    @Test
    public void testAvroSerDePrimitives() throws Exception {
        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), rootUrl);
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object[] objects = generatePrimitivePayloads();
        for (Object obj : objects) {
            String name = obj != null ? obj.getClass().getName() : Void.TYPE.getName();
            SchemaMetadata schemaMetadata = createSchemaMetadata(name, SchemaCompatibility.BOTH);
            byte[] serializedData = avroSnapshotSerializer.serialize(obj, schemaMetadata);

            Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), schemaMetadata, null);

            if (obj instanceof byte[]) {
                Assert.assertArrayEquals((byte[]) obj, (byte[]) deserializedObj);
            } else {
                Assert.assertEquals(obj, deserializedObj);
            }
        }
    }

    @Test
    public void testSerializerOps() throws Exception {
        String fileId = uploadFile();
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);

        schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(getSchemaTxt("/device.avsc"), "Initial version of the schema"));
        SerDesInfo serializerInfo = createSerDesInfo(fileId);
        Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);

        String schemaName = schemaMetadata.getName();
        schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);
        Collection<SerDesInfo> serializers = schemaRegistryClient.getSerializers(schemaName);

        Assert.assertTrue(new HashSet<>(serializers).contains(createSerializerInfo(serializerId, serializerInfo)));
    }

    private SerDesInfo createSerDesInfo(String fileId) {
        return new SerDesInfo.Builder()
                .name("avro serializer")
                .description("avro serializer")
                .fileId(fileId)
                .className("con.hwx.registries.serializer.AvroSnapshotSerializer")
                .buildSerializerInfo();
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

    private SerDesInfo createSerializerInfo(Long serializerId, SerDesInfo serializerInfo) {
        return new SerDesInfo.Builder()
                .id(serializerId)
                .description(serializerInfo.getDescription())
                .name(serializerInfo.getName())
                .fileId(serializerInfo.getFileId())
                .className(serializerInfo.getClassName())
                .buildSerializerInfo();
    }

    private static Long createSchema(SchemaMetadata schemaMetadata, SchemaRegistryClient client) throws Exception {
        Long id = client.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(id);

        // Get the schema created by name in the previous step & validate schema metadata returned by schema name
        Assert.assertEquals(schemaMetadata, client.getSchemaMetadataInfo(schemaMetadata.getName()).getSchemaMetadata());
        // Get the schema created by id in the previous step & validate schema metadata returned by schema name
        Assert.assertEquals(schemaMetadata, client.getSchemaMetadataInfo(id).getSchemaMetadata());
        // TODO Get the schema created by search and validate schema metadata returned by schema search
        return id;
    }

    private static SchemaIdVersion addSchemaVersion(SchemaRegistryClient client, String name, String schemaTxt,
                                                    String description, int expectedVersion) throws Exception {
        // Registering a new schema version
        SchemaIdVersion version = client.addSchemaVersion(name, new SchemaVersion(schemaTxt, description));
        Assert.assertNotNull(version.getSchemaMetadataId());
        Assert.assertEquals(expectedVersion, version.getVersion().intValue());

        // Create the same schema version again by schema name and ensure the existing schema version is returned
        SchemaIdVersion version1 = client.addSchemaVersion(name, new SchemaVersion(schemaTxt, "already added schema"));
        Assert.assertEquals(version, version1);

        // Ensure schema created is as expected
        SchemaVersionInfo schemaVersionInfo = client.getSchemaVersionInfo(new SchemaVersionKey(name, version.getVersion()));
        Assert.assertEquals(schemaVersionInfo.getSchemaText(), schemaTxt);
        Assert.assertEquals(schemaVersionInfo.getDescription(), description);

        // Ensure newly created schema is the latest version
        SchemaVersionInfo latest = client.getLatestSchemaVersionInfo(name);
        Assert.assertEquals(latest, schemaVersionInfo);
        return version;
    }

    private static void assertEquals(SchemaVersionInfo versionInfo, String schemaTxt, String description) {
        Assert.assertEquals(schemaTxt, versionInfo.getSchemaText());
        Assert.assertEquals(description, versionInfo.getDescription());
    }
}
