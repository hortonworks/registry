/*
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
        return new SchemaVersionInfo(1l, name, 1, getResourceText(name), System.currentTimeMillis(), "");
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
        Assert.assertEquals(getResourceText(expectedSchemaLocation).replace(" ", ""),
                            returnedSchemaText.replace(" ", ""));

        // double check whether the effective schema is semantically right parsing
        Schema.Parser parser = new Schema.Parser();
        Schema parsedReturnedSchema = parser.parse(returnedSchemaText);
        Assert.assertEquals(effectiveSchema, parsedReturnedSchema);
    }

    @Test
    public void testSchemasWithDefaults() throws Exception {
        doTestSchemaResolution("/avro/composites/simple-union.avsc",
                               "/avro/composites/expected-simple-union.avsc");
    }
}
