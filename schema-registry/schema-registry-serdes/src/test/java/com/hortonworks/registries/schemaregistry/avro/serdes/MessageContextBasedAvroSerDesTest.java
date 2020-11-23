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
package com.hortonworks.registries.schemaregistry.avro.serdes;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.serdes.Device;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 *
 */
public class MessageContextBasedAvroSerDesTest {

    @Mocked
    SchemaRegistryClient mockSchemaRegistryClient;

    @Test
    public void testSerDes() throws Exception {
        SchemaMetadata schemaMetadata =
                new SchemaMetadata.Builder("msgCtx-" + System.currentTimeMillis())
                        .schemaGroup("custom")
                        .type(AvroSchemaProvider.TYPE)
                        .compatibility(SchemaCompatibility.BACKWARD)
                        .build();
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(1L, 1, 1L);
        Device input = new Device(1L, "device", 1, System.currentTimeMillis());
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(1l, input.getName().toString(), schemaIdVersion.getVersion(),
                                                                    input.getSchema().toString(),
                                                                    System.currentTimeMillis(),
                                                                    "");

        new Expectations() {
            {
                mockSchemaRegistryClient.addSchemaVersion(withInstanceOf(SchemaMetadata.class), withInstanceOf(SchemaVersion.class));
                result = schemaIdVersion;

                mockSchemaRegistryClient.getSchemaMetadataInfo(anyString);
                result = new SchemaMetadataInfo(schemaMetadata);

                mockSchemaRegistryClient.getSchemaVersionInfo(withInstanceOf(SchemaVersionKey.class));
                result = schemaVersionInfo;
            }
        };

        MessageContextBasedAvroSerializer serializer = new MessageContextBasedAvroSerializer();
        serializer.init(Collections.emptyMap());

        MessageContext messageContext = serializer.serialize(input, schemaMetadata);

        MessageContextBasedAvroDeserializer deserializer = new MessageContextBasedAvroDeserializer();
        deserializer.init(Collections.emptyMap());
        Object deserializedObject = deserializer.deserialize(messageContext, null);

        Assert.assertTrue(SpecificData.get().compare(input, deserializedObject, input.getSchema()) == 0);    }
}
