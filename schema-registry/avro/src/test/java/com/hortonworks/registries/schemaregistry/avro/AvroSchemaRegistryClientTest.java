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

import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
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

    @Test
    public void testSchemaOps() throws Exception {
        String schema1 = getSchema("/schema-1.avsc");
        String schema2 = getSchema("/schema-2.avsc");
        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BOTH);
        SchemaKey schemaKey = schemaInfo.getSchemaKey();

        boolean success = schemaRegistryClient.registerSchemaInfo(schemaInfo);
        Assert.assertTrue(success);

        // registering a new schema
        Integer v1 = schemaRegistryClient.addSchemaVersion(schemaInfo, new SchemaVersion(schema1, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
        Integer v2 = schemaRegistryClient.addSchemaVersion(schemaKey, schemaInfo2);

        Assert.assertTrue(v2 == v1 + 1);

        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaKey, v2));
        SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaKey);

        Assert.assertEquals(latest, schemaVersionInfo);

        Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaKey);

        Assert.assertEquals(2, allVersions.size());

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        Integer version = schemaRegistryClient.addSchemaVersion(schemaKey, new SchemaVersion(schema1, "already added schema"));
        Assert.assertEquals(version, v1);

        Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("md5").build());
        Assert.assertEquals(2, md5SchemaVersionKeys.size());

        Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("txid").build());
        Assert.assertEquals(1, txidSchemaVersionKeys.size());
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalisSchema() throws Exception {
        String schema = "--- invalid schema ---";

        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistryClient.addSchemaVersion(schemaInfo, new SchemaVersion(schema, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BACKWARD);
        SchemaKey schemaKey = schemaInfo.getSchemaKey();

        // registering a new schema
        Integer v1 = schemaRegistryClient.addSchemaVersion(schemaInfo, new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        Integer v2 = schemaRegistryClient.addSchemaVersion(schemaKey, incompatSchemaInfo);
    }

    private SchemaInfo createSchemaInfo(String schemaDesc, SchemaProvider.Compatibility compatibility) {
        SchemaKey schemaKey = new SchemaKey(type(), schemaDesc + "-group", schemaDesc + "-schema");
        return new SchemaInfo(schemaKey, "Schema for " + schemaDesc, compatibility);
    }

    @Test
    public void testAvroSerDeGenericObj() throws Exception {
        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, rootUrl);
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        String deviceSchema = getSchema("/device.avsc");
        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BOTH);
        Integer v1 = schemaRegistryClient.addSchemaVersion(schemaInfo, new SchemaVersion(deviceSchema, "Initial version of the schema"));

        Object deviceObject = createGenericRecordForDevice();

        byte[] serializedData = avroSnapshotSerializer.serialize(deviceObject, schemaInfo);
        Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), schemaInfo.getSchemaKey(), null);
        Assert.assertEquals(deviceObject, deserializedObj);
    }

    @Test
    public void testAvroSerDePrimtives() throws Exception {
        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, rootUrl);
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object[] objects = generatePrimitivePayloads();
        for (Object obj : objects) {
            String name = obj != null ? obj.getClass().getName() : Void.TYPE.getName();
            SchemaInfo schemaInfo = createSchemaInfo(name, SchemaProvider.Compatibility.BOTH);
            byte[] serializedData = avroSnapshotSerializer.serialize(obj, schemaInfo);

            Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), schemaInfo.getSchemaKey(), null);

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
        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BOTH);

        Integer v1 = schemaRegistryClient.addSchemaVersion(schemaInfo,
                new SchemaVersion(getSchema("/device.avsc"), "Initial version of the schema"));
        SerDesInfo serializerInfo = createSerDesInfo(fileId);
        Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);

        SchemaKey schemaKey = schemaInfo.getSchemaKey();
        schemaRegistryClient.mapSchemaWithSerDes(schemaKey, serializerId);
        Collection<SerDesInfo> serializers = schemaRegistryClient.getSerializers(schemaKey);

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
