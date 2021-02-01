/*
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
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.errors.CyclicSchemaDependencyException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class AvroCompositeSchemasTest {
    private static final Logger LOG = LoggerFactory.getLogger(AvroCompositeSchemasTest.class);

    private AvroSchemaProvider avroSchemaProvider;
    private Map<String, SchemaVersionInfo> versions;

    @Before
    public void testComposites() throws Exception {
        versions = new HashMap<>();
        versions.put("utils", cretaeSchemaVersionInfo("/avro/composites/util.avsc"));
        versions.put("account", cretaeSchemaVersionInfo("/avro/composites/account.avsc"));
        versions.put("account-cyclic", cretaeSchemaVersionInfo("/avro/composites/account-cyclic.avsc"));
        versions.put("account-dep", cretaeSchemaVersionInfo("/avro/composites/account-dep.avsc"));

        avroSchemaProvider = new AvroSchemaProvider();
        SchemaVersionRetriever schemaVersionRetriever = new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return versions.get(key.getSchemaName());
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                return null;
            }
        };

        Map<String, Object> config = Collections.singletonMap(SchemaProvider.SCHEMA_VERSION_RETRIEVER_CONFIG,
                                                              schemaVersionRetriever);
        avroSchemaProvider.init(config);
    }

    private SchemaVersionInfo cretaeSchemaVersionInfo(String name) throws IOException {
        return new SchemaVersionInfo(1L, name, 1, getResourceText(name), System.currentTimeMillis(), "");
    }

    private String getResourceText(String name) throws IOException {
        return IOUtils.toString(AvroCompositeSchemasTest.class.getResourceAsStream(name), "UTF-8");
    }

    @Test
    public void testResultantSchema() throws Exception {
        String accountSchemaText = avroSchemaProvider.getResultantSchema(getResourceText("/avro/composites/account.avsc"));
        LOG.info("accountSchemaText [{}] ", accountSchemaText);
        LOG.info("Validating the resultant schema");
        Schema.Parser parser = new Schema.Parser();
        Schema parsedSchema = parser.parse(accountSchemaText);
        LOG.info("parsedSchema [{}] ", parsedSchema);
    }

    @Test(expected = CyclicSchemaDependencyException.class)
    public void testCyclicSchema() throws Exception {
        String accountSchemaText = avroSchemaProvider.getResultantSchema(getResourceText("/avro/composites/account-ref-cyclic.avsc"));
    }

    @Test
    public void testUnionSchemas() throws Exception {
        String givenSchemaLocation = "/avro/composites/unions.avsc";
        String expectedSchemaLocation = "/avro/composites/expected-unions.avsc";

        doTestSchemaResolution(givenSchemaLocation, expectedSchemaLocation);
    }

    private void doTestSchemaResolution(String givenSchemaLocation, String expectedSchemaLocation) throws IOException {
        AvroSchemaResolver avroSchemaResolver = new AvroSchemaResolver(null);
        Schema schema = new Schema.Parser().parse(getResourceText(givenSchemaLocation));
        LOG.info("schema = %s", schema);

        Schema effectiveSchema = avroSchemaResolver.handleUnionFieldsWithNull(schema, new HashSet<>());
        LOG.info("effectiveSchema = %s", effectiveSchema);
        String returnedSchemaText = effectiveSchema.toString();
        assertEquals(getResourceText(expectedSchemaLocation).replace(" ", ""),
                            returnedSchemaText.replace(" ", ""));

        // double check whether the effective schema is semantically right parsing
        Schema.Parser parser = new Schema.Parser();
        Schema parsedReturnedSchema = parser.parse(returnedSchemaText);
        assertEquals(effectiveSchema, parsedReturnedSchema);
    }

    @Test
    public void testSchemasWithDefaults() throws Exception {
        doTestSchemaResolution("/avro/composites/simple-union.avsc",
                               "/avro/composites/expected-simple-union.avsc");

        doTestSchemaResolution("/avro/composites/unions-with-defaults.avsc",
                "/avro/composites/expected-unions-with-defaults.avsc");
    }

    @Test
    public void testUnionSchemasPropRetention() throws Exception {
        AvroSchemaResolver avroSchemaResolver = new AvroSchemaResolver(null);
        Schema schema = new Schema.Parser().parse(getResourceText("/avro/composites/unions-with-props.avsc"));
        LOG.info("schema = %s", schema);

        Schema effectiveSchema = avroSchemaResolver.handleUnionFieldsWithNull(schema, new HashSet<>());
        LOG.info("effectiveSchema = %s", effectiveSchema);
        String returnedSchemaText = effectiveSchema.toString();
        assertEquals("foo", effectiveSchema.getField("name").getProp("someProp"));
        assertEquals("bar", effectiveSchema.getField("address").getProp("otherProp"));
        assertEquals("baz", effectiveSchema.getField("address").schema().getField("pincode").getProp("anotherProp"));
        assertEquals("quux", effectiveSchema.getField("secondaryAddress").getProp("moreProps"));

        // double check whether the effective schema is semantically right parsing
        Schema.Parser parser = new Schema.Parser();
        Schema parsedReturnedSchema = parser.parse(returnedSchemaText);
        assertEquals(effectiveSchema, parsedReturnedSchema);
    }

    @Test
    public void testHashing() throws Exception {
        final String originalText = getResourceText("/avro/composites/account.avsc");
        assertNotNull(originalText);

        HashMap<String, Object> config = new HashMap<>();
        config.putAll(avroSchemaProvider.getConfig());

        // md5

        config.put(SchemaProvider.HASH_FUNCTION_CONFIG, "MD5");
        avroSchemaProvider.init(config);

        String md5hash = bytesToHex(avroSchemaProvider.getFingerprint(originalText));
        assertEquals("8500728a811352bd7380466798187ff3", md5hash);

        // sha

        config.put(SchemaProvider.HASH_FUNCTION_CONFIG, "SHA-1");
        avroSchemaProvider.init(config);

        String shaHash = bytesToHex(avroSchemaProvider.getFingerprint(originalText));
        assertEquals("a6ea2620bce432bc20b6f580ddc1c4ed79d1c167", shaHash);

        // sha-256

        config.put(SchemaProvider.HASH_FUNCTION_CONFIG, "SHA-256");
        avroSchemaProvider.init(config);

        String sha256Hash = bytesToHex(avroSchemaProvider.getFingerprint(originalText));
        assertEquals("a379dec4b97b622cc28bc2c19f164dff91d64432165b4e7ed043d5f825b9684b", sha256Hash);
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder();
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

}
