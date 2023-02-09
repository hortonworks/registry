/**
 * Copyright 2016-2019 Cloudera, Inc.
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

import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AvroSchemaProviderTest {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaProviderTest.class);

    @Test
    public void testAvroSchemas() throws Exception {
        AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        try (InputStream schemaStream = AvroSchemaProviderTest.class.getResourceAsStream("/avro/trucks.avsc");) {
            List<SchemaFieldInfo> schemaFieldInfos = avroSchemaProvider.generateFields(IOUtils.toString(schemaStream));
            LOG.info("generated schema fields [{}]", schemaFieldInfos);
            assertEquals(11, schemaFieldInfos.size());
        }
    }

    private String schema = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"namespace\" : \"com.hortonworks.registries\",\n" +
            "  \"name\" : \"trucks\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"driverId\" , \"type\" : \"int\" },\n" +
            "    { \"name\" : \"truckId\" , \"type\" : \"int\" }" +
            "  ]\n" +
            "}";

    private String schemaWithDefaults = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"namespace\" : \"com.hortonworks.registries\",\n" +
            "  \"name\" : \"trucks\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"driverId\" , \"type\" : \"int\" },\n" +
            "    { \"name\" : \"truckId\" , \"type\" : \"int\", \"default\":0 }\n" +
            "  ]\n" +
            "}\n";

    private String schemaWithAliases = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"namespace\" : \"com.hortonworks.registries\",\n" +
            "  \"aliases\":[\"truckType\"],\n" +
            "  \"name\" : \"trucks\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"driverId\" , \"type\" : \"int\" },\n" +
            "    { \"name\" : \"truckId\" , \"type\" : \"int\", \"default\":0 }\n" +
            "  ]\n" +
            "}";

    private String schemaWithBytesDefault = "{\n" +
        " \"type\": \"record\",\n" +
        " \"name\": \"test\",\n" +
        " \"fields\": [\n" +
        "  {\n" +
        "   \"name\": \"myDecimal\",\n" +
        "   \"type\": {\n" +
        "    \"type\": \"bytes\",\n" +
        "    \"logicalType\": \"decimal\",\n" +
        "    \"precision\": 10,\n" +
        "    \"scale\": 5\n" +
        "   },\n" +
        "   \"default\": \"\\u0000\"\n" +
        "  }\n" +
        " ]\n" +
        "}";

    @Test
    public void testFingerPrints() throws Exception {
        AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        byte[] schemaFingerprint = avroSchemaProvider.getFingerprint(schema);
        byte[] schemaFingerprint2 = avroSchemaProvider.getFingerprint(schema);
        byte[] schemaWithDefaultsFingerprint = avroSchemaProvider.getFingerprint(schemaWithDefaults);
        byte[] schemaWithAliasesFingerprint = avroSchemaProvider.getFingerprint(schemaWithAliases);

        assertArrayEquals(schemaFingerprint, schemaFingerprint2);
        assertFalse(Arrays.equals(schemaWithAliasesFingerprint, schemaFingerprint));
        assertFalse(Arrays.equals(schemaWithDefaultsFingerprint, schemaFingerprint));
        assertFalse(Arrays.equals(schemaWithAliasesFingerprint, schemaWithDefaultsFingerprint));
    }

    @Test
    public void testFingerPrintsWithBytesDefaultValue() throws Exception {
        AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        byte[] schemaFingerprint1 = avroSchemaProvider.getFingerprint(schemaWithBytesDefault);
        byte[] schemaFingerprint2 = avroSchemaProvider.getFingerprint(schemaWithBytesDefault);
        assertArrayEquals(schemaFingerprint1, schemaFingerprint2);
    }
}
