package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static junit.framework.TestCase.assertFalse;

public class AvroSchemaFingerprintTest {

    private static final AvroSchemaProvider avroSchemaProvider = new AvroSchemaProvider();

    @Test
    public void testForString() throws IOException, InvalidSchemaException, SchemaNotFoundException {
        String plainStringSchemaString = getResourceText("/avro/plain-string.avsc");
        String utf8StringSchemaString = getResourceText("/avro/java-string.avsc");
        assertFalse(Arrays.equals(avroSchemaProvider.getFingerprint(plainStringSchemaString), avroSchemaProvider.getFingerprint(utf8StringSchemaString)));
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidStringProp() throws IOException, InvalidSchemaException, SchemaNotFoundException {
        String invalidStringPropSchemaString = getResourceText("/avro/invalid-string-prop.avsc");
        avroSchemaProvider.getFingerprint(invalidStringPropSchemaString);
    }

    private String getResourceText(String name) throws IOException {
        return IOUtils.toString(AvroCompositeSchemasTest.class.getResourceAsStream(name), "UTF-8");
    }
}
