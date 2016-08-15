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
import com.hortonworks.registries.schemaregistry.SchemaInfoStorable;
import com.hortonworks.registries.schemaregistry.SchemaMetadataStorable;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/**
 *
 */
public class AvroSchemaRegistryTest {

    private DefaultSchemaRegistry schemaRegistry;

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        schemaName = "schema-" + System.currentTimeMillis();
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

        SchemaMetadata schemaMetadata = new SchemaMetadata(schemaName, AvroSchemaProvider.TYPE, schemaName, SchemaProvider.Compatibility.BOTH, schema1);

        SchemaMetadataStorable schemaMetadataStorable = schemaRegistry.getOrCreateSchemaMetadata(schemaMetadata.schemaMetadataStorable());

        Long schemaMetadataId = schemaMetadataStorable.getId();
        SchemaInfoStorable givenSchemaInfoStorable = schemaMetadata.schemaInfoStorable();
        givenSchemaInfoStorable.setSchemaMetadataId(schemaMetadataId);
        SchemaInfoStorable addedSchemaInfoStorable = schemaRegistry.addSchemaInfo(givenSchemaInfoStorable);

        int v1 = addedSchemaInfoStorable.getVersion();

        SchemaInfoStorable schemaInfoStorable2 = new SchemaInfoStorable();
        schemaInfoStorable2.setSchemaMetadataId(schemaMetadataId);
        schemaInfoStorable2.setSchemaText(schema2);
        SchemaInfoStorable addedSchemaInfoStorable2 = addSchemaAndVerify(schemaInfoStorable2);
        int v2 = addedSchemaInfoStorable2.getVersion();

        Assert.assertTrue(v2 == v1 + 1);

        SchemaInfoStorable latest = schemaRegistry.getLatestSchemaInfo(schemaMetadataId);
        Assert.assertEquals(latest, addedSchemaInfoStorable2);

    }

    private SchemaInfoStorable addSchemaAndVerify(SchemaInfoStorable schemaInfoStorable) {
        SchemaInfoStorable addedSchemaInfoStorable = schemaRegistry.addSchemaInfo(schemaInfoStorable);
        Assert.assertEquals(addedSchemaInfoStorable.getSchemaMetadataId(), schemaInfoStorable.getSchemaMetadataId());
        Assert.assertEquals(addedSchemaInfoStorable.getSchemaText(), schemaInfoStorable.getSchemaText());
        return addedSchemaInfoStorable;
    }

}