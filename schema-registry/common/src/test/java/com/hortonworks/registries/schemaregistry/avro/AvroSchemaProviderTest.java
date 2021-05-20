package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.core.IsEqual.equalTo;

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

        Assert.assertThat(withNullDefaultsFingerprintHex, equalTo("e1e17a3aef8c728c131204bbf49046b2"));
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
