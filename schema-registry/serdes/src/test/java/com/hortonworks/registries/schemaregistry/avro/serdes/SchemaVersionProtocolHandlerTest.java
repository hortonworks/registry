/*
 * Copyright 2016 Hortonworks.
 * <p>
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
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.serdes.Device;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Random;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;

/**
 *
 */
public class SchemaVersionProtocolHandlerTest {

    @Mocked
    SchemaRegistryClient mockSchemaRegistryClient;

    @Test
    public void testSerDesWithVersionIdGtLtIntMax() throws Exception {
        int delta = Math.abs(new Random().nextInt());
        long[] ids = {((long) Integer.MAX_VALUE + delta), // more than int max, should trigger VERSION_ID_AS_LONG_PROTOCOL
                ((long) Integer.MAX_VALUE - delta)}; // less than int max, should trigger VERSION_ID_AS_INT_PROTOCOL
        for (long id : ids) {
            _testSerDes(id, SerDesProtocolHandlerRegistry.VERSION_ID_AS_INT_PROTOCOL);
        }
    }

    private void _testSerDes(Long id, Number serdesProtocolVersion) throws Exception {
        SchemaMetadata schemaMetadata =
                new SchemaMetadata.Builder("random-" + System.currentTimeMillis())
                        .schemaGroup("custom")
                        .type(AvroSchemaProvider.TYPE)
                        .compatibility(SchemaCompatibility.BACKWARD)
                        .build();

        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(1L, 1, id);

        Device input = new Device(1L, "device", 1, System.currentTimeMillis());
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(id, input.getName().toString(), schemaIdVersion.getVersion(),
                                                                    input.getSchema().toString(),
                                                                    System.currentTimeMillis(),
                                                                    "some device");

        new Expectations() {
            {
                mockSchemaRegistryClient.getSchemaMetadataInfo(anyString);
                result = new SchemaMetadataInfo(schemaMetadata); minTimes=0; maxTimes=1;

                mockSchemaRegistryClient.addSchemaVersion(withInstanceOf(SchemaMetadata.class), withInstanceOf(SchemaVersion.class));
                result = schemaIdVersion; minTimes=0; maxTimes=1;

                mockSchemaRegistryClient.getSchemaVersionInfo(withInstanceOf(SchemaVersionKey.class));
                result = schemaVersionInfo; minTimes=0; maxTimes=1;
            }
        };

        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.init(Collections.singletonMap(SERDES_PROTOCOL_VERSION, serdesProtocolVersion));

        AvroSnapshotDeserializer deserializer = new AvroSnapshotDeserializer();
        deserializer.init(Collections.emptyMap());

        byte[] serializedData = serializer.serialize(input, schemaMetadata);
        Object deserializedObj = deserializer.deserialize(new ByteArrayInputStream(serializedData), null);

        Assert.assertTrue(SpecificData.get().compare(input, deserializedObj, input.getSchema()) == 0);
    }

    @Test
    public void testIntegerSerDesProtocolVersion() throws Exception {
       _testSerDes(1L, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerDesProtocolVersionAsMoreThan127() throws Exception {
       _testSerDes(1L, Byte.MAX_VALUE + Math.abs(new Random().nextInt()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerDesProtocolVersionAsLessThanZero() throws Exception {
        _testSerDes(1L, new Random().nextInt(127) - 128);
    }

}
