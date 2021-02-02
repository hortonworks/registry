/**
 * Copyright 2016-2021 Cloudera, Inc.
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

import com.hortonworks.registries.schemaregistry.SchemaProvider;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroSchemaProviderTest {

    private String schemaWithNullDefaults = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"namespace\" : \"com.hortonworks.registries\",\n" +
            "  \"name\" : \"trucks\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"driverId\" , \"type\" : [\"null\", \"int\"], \"default\": null }" +
            "  ]\n" +
            "}\n";

    @Test
    public void testGetFingerprintNullTypeDeterminism() throws Exception {
        AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();
        avroSchemaProvider.init(Collections.singletonMap(SchemaProvider.HASH_FUNCTION_CONFIG, "MD5"));

        String withNullDefaultsFingerprintHex = bytesToHex(avroSchemaProvider.getFingerprint(schemaWithNullDefaults));

        assertEquals("e1e17a3aef8c728c131204bbf49046b2", withNullDefaultsFingerprintHex);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

}
