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

import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.memory.InMemoryStorageManager;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
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

/**
 *
 */
public class AvroSchemaRegistryTest {

    public static final String SCHEMA_GROUP = "test-group";
    public static final SchemaKey INVALIDSCHEMA_METADATA_KEY =
            new SchemaKey(AvroSchemaProvider.TYPE, SCHEMA_GROUP, "invalid-schema" + System.currentTimeMillis());

    private DefaultSchemaRegistry schemaRegistry;

    @Rule
    public TestName TEST_NAME_RULE = new TestName();

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device-compat.avsc");
        schemaName = "org.hwx.schemas.test-schema." + System.currentTimeMillis();
        StorageManager storageManager = new InMemoryStorageManager();
        schemaRegistry = new DefaultSchemaRegistry(storageManager, null, Collections.singleton(new AvroSchemaProvider()));
        schemaRegistry.init(Collections.emptyMap());
    }

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSchemaRegistryClientTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistrySchemaOps() throws Exception {
        SchemaKey schemaKey = new SchemaKey(AvroSchemaProvider.TYPE, SCHEMA_GROUP, schemaName);
        SchemaProvider.Compatibility compatibility = SchemaProvider.Compatibility.BOTH;
        SchemaInfo schemaInfo = new SchemaInfo(schemaKey, "devices schema", compatibility);

        schemaRegistry.addSchemaInfo(schemaInfo);

        Integer v1 = schemaRegistry.addSchemaVersion(schemaInfo, new SchemaVersion(schema1, "initial version of the schema"));
        Integer v2 = schemaRegistry.addSchemaVersion(schemaKey, new SchemaVersion(schema2, "second version of the the schema"));
        Assert.assertTrue(v2 == v1 + 1);

        Collection<SchemaVersionInfo> allSchemaVersions = schemaRegistry.findAllVersions(schemaKey);
        Assert.assertTrue(allSchemaVersions.size() == 2);

        SchemaInfo schemaInfoStorable = schemaRegistry.getSchemaVersionInfo(schemaKey);
        Assert.assertEquals(schemaKey, schemaInfoStorable.getSchemaKey());
        Assert.assertEquals(compatibility, schemaInfoStorable.getCompatibility());

        Integer schemaVersion = schemaRegistry.getSchemaVersion(schemaKey, schema1);
        Assert.assertEquals(v1, schemaVersion);

        SchemaVersionInfo schemaVersionInfo1 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaKey, v1));
        Assert.assertEquals(schemaVersionInfo1.getSchemaText(), schema1);

        SchemaVersionInfo schemaVersionInfo2 = schemaRegistry.getSchemaVersionInfo(new SchemaVersionKey(schemaKey, v2));
        Assert.assertEquals(schemaVersionInfo2.getSchemaText(), schema2);

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        Integer version = schemaRegistry.addSchemaVersion(schemaKey, new SchemaVersion(schema1, "already added schema"));
        Assert.assertEquals(version, v1);
    }

    @Test
    public void testNonExistingSchemaMetadata() {
        SchemaInfo schemaInfo = schemaRegistry.getSchemaVersionInfo(INVALIDSCHEMA_METADATA_KEY);
        Assert.assertNull(schemaInfo);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testAddVersionToNonExistingSchema() throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException {
        schemaRegistry.addSchemaVersion(INVALIDSCHEMA_METADATA_KEY, new SchemaVersion("foo", "dummy"));
    }

    @Test(expected = InvalidSchemaException.class)
    public void testInvalisSchema() throws Exception {
        String schema = "--- random invalid schema ---" + new Date();

        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BACKWARD);

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaInfo, new SchemaVersion(schema, "Initial version of the schema"));
    }

    @Test(expected = IncompatibleSchemaException.class)
    public void testIncompatibleSchemas() throws Exception {
        String schema = getSchema("/device.avsc");
        String incompatSchema = getSchema("/device-incompat.avsc");

        SchemaInfo schemaInfo = createSchemaInfo(TEST_NAME_RULE.getMethodName(), SchemaProvider.Compatibility.BACKWARD);
        SchemaKey schemaKey = schemaInfo.getSchemaKey();

        // registering a new schema
        Integer v1 = schemaRegistry.addSchemaVersion(schemaInfo, new SchemaVersion(schema, "Initial version of the schema"));

        // adding a new version of the schema
        SchemaVersion incompatSchemaInfo = new SchemaVersion(incompatSchema, "second version");
        Integer v2 = schemaRegistry.addSchemaVersion(schemaKey, incompatSchemaInfo);
    }

    private SchemaInfo createSchemaInfo(String schemaDesc, SchemaProvider.Compatibility compatibility) {
        SchemaKey schemaKey = new SchemaKey(AvroSchemaProvider.TYPE, schemaDesc + "-group", schemaDesc + "-schema");
        return new SchemaInfo(schemaKey, "Schema for " + schemaDesc, compatibility);
    }

}