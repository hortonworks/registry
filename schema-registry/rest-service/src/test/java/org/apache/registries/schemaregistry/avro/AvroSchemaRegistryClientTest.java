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
package org.apache.registries.schemaregistry.avro;

import org.apache.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.common.test.IntegrationTest;
import org.apache.registries.schemaregistry.SchemaMetadataInfo;
import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.SchemaCompatibility;
import org.apache.registries.schemaregistry.SchemaFieldQuery;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaVersion;
import org.apache.registries.schemaregistry.SchemaVersionInfo;
import org.apache.registries.schemaregistry.SchemaVersionKey;
import org.apache.registries.schemaregistry.SerDesInfo;
import org.apache.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import org.apache.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

/**
 *
 */
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest extends AbstractAvroSchemaRegistryCientTest {

    private static final String INVALID_SCHEMA_PROVIDER_TYPE = "invalid-schema-provider-type";

    @Test
    public void testSchemaOps() throws Exception {
        String schema1 = getSchema("/schema-1.avsc");
        String schema2 = getSchema("/schema-2.avsc");
        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BOTH);

        Long id = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(id);

        // registering a new schema
        String schemaName = schemaMetadata.getName();
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));
        Assert.assertNotNull(v1.getSchemaMetadataId());
        Assert.assertTrue(1 == v1.getVersion());

        SchemaMetadataInfo schemaMetadataInfoForId = schemaRegistryClient.getSchemaMetadataInfo(v1.getSchemaMetadataId());
        SchemaMetadataInfo schemaMetadataInfoForName = schemaRegistryClient.getSchemaMetadataInfo(schemaName);
        Assert.assertEquals(schemaMetadataInfoForId, schemaMetadataInfoForName);

        // adding a new version of the schema
        SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
        SchemaIdVersion v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);

        Assert.assertTrue(v2.getVersion() == v1.getVersion() + 1);

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
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schema = "--- invalid schema ---";

        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaMetadata(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        SchemaIdVersion v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, incompatSchemaInfo);
    }

    private SchemaMetadata createSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(schemaDesc + "-schema")
                .type(type())
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

        Object defaultSerializer = schemaRegistryClient.getDefaultSerializer(INVALID_SCHEMA_PROVIDER_TYPE);
    }

    @Test(expected = Exception.class)
    public void testInvalidTypeForDefaultDes() throws Exception {
        Object defaultDeserializer = schemaRegistryClient.getDefaultDeserializer(INVALID_SCHEMA_PROVIDER_TYPE);
    }

    @Test
    public void testAvroSerDeGenericObj() throws Exception {
        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, rootUrl);
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
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), schemaMetadata, null);
        Assert.assertEquals(deviceObject, deserializedObj);
    }

    @Test
    public void testAvroSerDePrimitives() throws Exception {
        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, rootUrl);
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

        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(getSchema("/device.avsc"), "Initial version of the schema"));
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

    private String type() {
        return AvroSchemaProvider.TYPE;
    }

}
