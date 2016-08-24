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
import com.hortonworks.registries.schemaregistry.SchemaInfo;
import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.VersionedSchema;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class AvroSchemaRegistryTest {

    public static final String SCHEMA_GROUP = "test-group";
    public static final SchemaMetadataKey INVALIDSCHEMA_METADATA_KEY = new SchemaMetadataKey(AvroSchemaProvider.TYPE, SCHEMA_GROUP, "invalid-schema" + System.currentTimeMillis());

    private DefaultSchemaRegistry schemaRegistry;

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        schemaName = "org.hwx.schemas.test-schema." + System.currentTimeMillis();
        StorageManager storageManager = new InMemoryStorageManager();
        schemaRegistry = new DefaultSchemaRegistry(storageManager, null, Collections.singleton(new AvroSchemaProvider()));
        schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistrySchemaOps() throws Exception {
        SchemaMetadataKey schemaMetadataKey = new SchemaMetadataKey(AvroSchemaProvider.TYPE, SCHEMA_GROUP, schemaName);
        SchemaProvider.Compatibility compatibility = SchemaProvider.Compatibility.BOTH;
        SchemaMetadata schemaMetadata = new SchemaMetadata(schemaMetadataKey, "devices schema", compatibility);

        Long schemaMetadataId = schemaRegistry.addSchemaMetadata(schemaMetadata);

        Integer v1 = schemaRegistry.addSchema(schemaMetadata, new VersionedSchema(schema1, "initial version of the schema"));
        Integer v2 = schemaRegistry.addSchema(schemaMetadataKey, new VersionedSchema(schema2, "second version of the the schema"));
        Assert.assertTrue(v2 == v1 + 1);

        Collection<SchemaInfo> allSchemaVersions = schemaRegistry.findAllVersions(schemaMetadataId);
        Assert.assertTrue(allSchemaVersions.size() == 2);

        SchemaMetadata schemaMetadataStorable = schemaRegistry.getSchemaMetadata(schemaMetadataKey);
        Assert.assertEquals(schemaMetadataKey, schemaMetadataStorable.getSchemaMetadataKey());
        Assert.assertEquals(compatibility, schemaMetadataStorable.getCompatibility());

        Integer schemaVersion = schemaRegistry.getSchemaVersion(schemaMetadataKey, schema1);
        Assert.assertEquals(v1, schemaVersion);

        SchemaInfo schemaInfo1 = schemaRegistry.getSchemaInfo(schemaMetadataId, v1);
        Assert.assertEquals(schemaInfo1.getSchemaText(), schema1);

        SchemaInfo schemaInfo2 = schemaRegistry.getSchemaInfo(schemaMetadataId, v2);
        Assert.assertEquals(schemaInfo2.getSchemaText(), schema2);

        // receive the same version as earlier without adding a new schema entry as it exists in the same schema group.
        Integer version = schemaRegistry.addSchema(schemaMetadataKey, new VersionedSchema(schema1, "already added schema"));
        Assert.assertEquals(version, v1);
    }

    @Test
    public void testNonExistingSchemaMetadata() {
        SchemaMetadata schemaMetadata = schemaRegistry.getSchemaMetadata(INVALIDSCHEMA_METADATA_KEY);
        Assert.assertNull(schemaMetadata);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void testAddVersionToNonExistingSchema() throws SchemaNotFoundException {
        schemaRegistry.addSchema(INVALIDSCHEMA_METADATA_KEY, new VersionedSchema("foo", "dummy"));
    }

    //todo add tests for compatibility

}