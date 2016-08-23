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

import com.hortonworks.iotas.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.VersionedSchema;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryApplication;
import com.hortonworks.registries.schemaregistry.webservice.SchemaRegistryConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

/**
 *
 */
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest {

    @ClassRule
    public static final DropwizardAppRule<SchemaRegistryConfiguration> RULE
            = new DropwizardAppRule<>(SchemaRegistryApplication.class, ResourceHelpers.resourceFilePath("schema-registry-test.yaml"));

    private String rootUrl = String.format("http://localhost:%d/api/v1/catalog", RULE.getLocalPort());
    private SchemaRegistryClient schemaRegistryClient;

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    @Before
    public void setup() throws IOException {
        schemaRegistryClient = new SchemaRegistryClient(Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, (Object) rootUrl));
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        schemaName = "schema-" + System.currentTimeMillis();

    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testSchemaRelatedOps() throws Exception {

        // registering new schema-metadata
        SchemaMetadataKey schemaMetadataKey = new SchemaMetadataKey(type(), "group-1", "com.hwx.iot.device.schema");
        SchemaMetadata schemaMetadata = new SchemaMetadata(schemaMetadataKey, "device schema", SchemaProvider.Compatibility.BOTH);

        Integer v1 = schemaRegistryClient.registerSchema(schemaMetadata, new VersionedSchema(schema1, "Initial version of the schema"));

        // adding a new version of the schema
        VersionedSchema schemaInfo2 = new VersionedSchema(schema2, "second version");
        Integer v2 = schemaRegistryClient.addVersionedSchema(schemaMetadataKey, schemaInfo2);

        Assert.assertTrue(v2 == v1 + 1);

        SchemaInfo schemaInfo = schemaRegistryClient.getSchema(new SchemaKey(schemaMetadataKey, v2));
        SchemaInfo latest = schemaRegistryClient.getLatestSchema(schemaMetadataKey);

        Assert.assertEquals(latest, schemaInfo);

        Collection<SchemaInfo> allVersions = schemaRegistryClient.getAllVersions(schemaMetadataKey);

        Assert.assertEquals(allVersions.size(), 2);
    }

    @Test
    public void testSerializerOps() throws Exception {

        // upload a dummy file.
        File tmpJarFile = Files.createTempFile("foo", ".jar").toFile();
        tmpJarFile.deleteOnExit();
        try (FileOutputStream fileOutputStream = new FileOutputStream(tmpJarFile)) {
            IOUtils.write(("Some random stuff: " + UUID.randomUUID()).getBytes(), fileOutputStream);
        }

        InputStream inputStream = new FileInputStream(tmpJarFile);
        String fileId = schemaRegistryClient.uploadFile(inputStream);

        Long schemaMetadataId = 0L;

        // add serializer with the respective uploaded jar file id.
        SerDesInfo serializerInfo =
                new SerDesInfo.Builder()
                        .name("avro serializer")
                        .description("avro serializer")
                        .fileId(fileId).className("con.hwx.iotas.serializer.AvroSnapshotSerializer")
                        .buildSerializerInfo();

        Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);

        // map this serializer with a registered schema
        schemaRegistryClient.mapSchemaWithSerDes(schemaMetadataId, serializerId);

        // get registered serializers
        Collection<SerDesInfo> serializers = schemaRegistryClient.getSerializers(schemaMetadataId);

        SerDesInfo registeredSerializerInfo = serializers.iterator().next();

        Assert.assertEquals(registeredSerializerInfo, createSerializerInfo(serializerId, serializerInfo));
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
