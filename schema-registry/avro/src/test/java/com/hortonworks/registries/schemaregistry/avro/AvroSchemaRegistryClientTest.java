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

import com.google.common.collect.Lists;
import com.hortonworks.iotas.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.VersionedSchema;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryApplication;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 *
 */
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest {

    @ClassRule
    public static final DropwizardAppRule<SchemaRegistryConfiguration> RULE
            = new DropwizardAppRule<>(SchemaRegistryApplication.class, ResourceHelpers.resourceFilePath("schema-registry-test.yaml"));

    private final String rootUrl = String.format("http://localhost:%d/api/v1", RULE.getLocalPort());
    private SchemaRegistryClient schemaRegistryClient;

    protected String schema1;
    protected String schema2;
    protected String schemaName;
    private SchemaMetadata schemaMetadata;
    private SchemaMetadataKey schemaMetadataKey;

    @Before
    public void setup() throws IOException {
        schemaRegistryClient = new SchemaRegistryClient(Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, rootUrl));
        schema1 = getSchema("/schema-1.avsc");
        schema2 = getSchema("/schema-2.avsc");
        schemaName = "schema-" + System.currentTimeMillis();
        schemaMetadataKey = new SchemaMetadataKey(type(), "group-1", "com.hwx.iot.device.schema");
        schemaMetadata = new SchemaMetadata(schemaMetadataKey, "device schema", SchemaProvider.Compatibility.BOTH);

    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testSchemaOps() throws Exception {

        // registering new schema-metadata
        Integer v1 = schemaRegistryClient.registerSchema(schemaMetadata, new VersionedSchema(schema1, "Initial version of the schema"));

        // adding a new version of the schema
        VersionedSchema schemaInfo2 = new VersionedSchema(schema2, "second version");
        Integer v2 = schemaRegistryClient.addVersionedSchema(schemaMetadataKey, schemaInfo2);

        Assert.assertTrue(v2 == v1 + 1);

        SchemaInfo schemaInfo = schemaRegistryClient.getSchema(new SchemaKey(schemaMetadataKey, v2));
        SchemaInfo latest = schemaRegistryClient.getLatestSchema(schemaMetadataKey);

        Assert.assertEquals(latest, schemaInfo);

        Collection<SchemaInfo> allVersions = schemaRegistryClient.getAllVersions(schemaMetadataKey);

        Assert.assertEquals(2, allVersions.size());

        Collection<SchemaKey> md5SchemaKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("md5").build());
        Assert.assertEquals(2, md5SchemaKeys.size());

        Collection<SchemaKey> txidSchemaKeys = schemaRegistryClient.findSchemasByFields(new SchemaFieldQuery.Builder().name("txid").build());
        Assert.assertEquals(1, txidSchemaKeys.size());
    }

    @Test
    public void testAvroSerDe() throws Exception {
        String deviceSchema = getSchema("/device.avsc");
        Integer v1 = schemaRegistryClient.registerSchema(schemaMetadata, new VersionedSchema(deviceSchema, "Initial version of the schema"));

        Map<String, String> config = Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, rootUrl);
        AvroSnapshotSerializer avroSnapshotSerializer = new AvroSnapshotSerializer();
        avroSnapshotSerializer.init(config);
        AvroSnapshotDeserializer avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        avroSnapshotDeserializer.init(config);

        Object avroObject = createGenericAvroRecord(deviceSchema);
        Random random = new Random();
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        List<?> objects = Lists.newArrayList(avroObject, random.nextBoolean(), random.nextInt(), random.nextLong(),
                                                bytes, "Current time:"+System.currentTimeMillis());
        for (Object obj : objects) {
            byte[] serializedData = avroSnapshotSerializer.serialize(obj, schemaMetadata);

            Object deserializedObj = avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(serializedData), schemaMetadata.getSchemaMetadataKey(), null);

            Assert.assertEquals(avroObject, deserializedObj);
        }
    }

    private Object createGenericAvroRecord(String schemaText) {
        Schema schema = new Schema.Parser().parse(schemaText);

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("xid", System.currentTimeMillis());
        avroRecord.put("name", "foo-"+System.currentTimeMillis());
        avroRecord.put("version", new Random().nextInt());
        avroRecord.put("timestamp", System.currentTimeMillis());

        return avroRecord;
    }


    @Test
    public void testSerializerOps() throws Exception {
        String fileId = uploadFile();
        Integer v1 = schemaRegistryClient.registerSchema(schemaMetadata, new VersionedSchema(schema1, "Initial version of the schema"));
        SerDesInfo serializerInfo = createSerDesInfo(fileId);
        Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);
        schemaRegistryClient.mapSchemaWithSerDes(schemaMetadataKey, serializerId);
        Collection<SerDesInfo> serializers = schemaRegistryClient.getSerializers(schemaMetadataKey);

        Assert.assertTrue(new HashSet<>(serializers).contains(createSerializerInfo(serializerId, serializerInfo)));
    }

    private SerDesInfo createSerDesInfo(String fileId) {
        return new SerDesInfo.Builder()
                .name("avro serializer")
                .description("avro serializer")
                .fileId(fileId)
                .className("con.hwx.iotas.serializer.AvroSnapshotSerializer")
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
