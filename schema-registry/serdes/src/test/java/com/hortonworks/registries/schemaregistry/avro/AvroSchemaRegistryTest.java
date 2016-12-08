/**
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
 **/
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.errors.UnsupportedSchemaTypeException;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

/**
 *
 */
public class AvroSchemaRegistryTest {

    public static final String SCHEMA_GROUP = "test-group";
    public static final String INVALIDSCHEMA_METADATA_KEY = "invalid-schema" + System.currentTimeMillis();

    private DefaultSchemaRegistry schemaRegistry;

    @Rule
    public TestName TEST_NAME_RULE = new TestName();

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device-compat.avsc");
        schemaName = "org.hwx.schemas.test-schema." + UUID.randomUUID();
        StorageManager storageManager = new InMemoryStorageManager();
        schemaRegistry = new DefaultSchemaRegistry(storageManager, null, Collections.singleton(new AvroSchemaProvider()));
        schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSchemaRegistryTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistrySchemaOps() throws Exception {
        SchemaCompatibility compatibility = SchemaCompatibility.BOTH;
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(schemaName)
                .type(AvroSchemaProvider.TYPE)
                .description("devices schema")
                .compatibility(compatibility)
                .schemaGroup(SCHEMA_GROUP).build();

        Long schemaMetadataId = schemaRegistry.addSchemaMetadata(schemaMetadata);

        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadata, schema1, "initial version of the schema");
        Integer v2 = schemaRegistry.addSchemaVersion(schemaName, schema2, "second version of the the schema");
        Assert.assertTrue(v2 == v1 + 1);

        Collection<SchemaVersionInfo> allSchemaVersions = schemaRegistry.findAllVersions(schemaName);
        Assert.assertTrue(allSchemaVersions.size() == 2);

        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(schemaName);
        Assert.assertEquals(schemaMetadata, schemaMetadataInfo.getSchemaMetadata());

        Integer schemaVersion = schemaRegistry.getSchemaVersion(schemaName, schema1);
        Assert.assertEquals(v1, schemaVersion);

        SchemaVersionInfo schemaVersionInfo1 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v1));
        Assert.assertEquals(schemaVersionInfo1.getSchemaText(), schema1);

        SchemaVersionInfo schemaVersionInfo2 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2));
        Assert.assertEquals(schemaVersionInfo2.getSchemaText(), schema2);

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        Integer version = schemaRegistry.addSchemaVersion(schemaMetadata, schema1, "already added schema");
        Assert.assertEquals(version, v1);
    }

    @Test
    public void testNonExistingSchemaMetadata() {
        SchemaMetadataInfo schemaMetadataInfo = schemaRegistry.getSchemaMetadata(INVALIDSCHEMA_METADATA_KEY);
        Assert.assertNull(schemaMetadataInfo);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testAddVersionToNonExistingSchema() throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, UnsupportedSchemaTypeException {
        schemaRegistry.addSchemaVersion(INVALIDSCHEMA_METADATA_KEY, "foo", "dummy");
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalidSchema() throws Exception {
        String schema = "--- random invalid schema ---" + new Date();

        SchemaMetadata schemaMetadataInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadataInfo, schema, "Initial version of the schema");
    }


    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaMetadata schemaMetadata = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaCompatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaMetadata, schema, "Initial version of the schema");

        // adding a new version of the schema
        Integer v2 = schemaRegistry.addSchemaVersion(schemaMetadata, incompatSchema, "second version");
    }

    private SchemaMetadata createSchemaInfo(String testName, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(testName + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(testName + "-group")
                .description("Schema for " + testName)
                .compatibility(compatibility)
                .build();
    }

}