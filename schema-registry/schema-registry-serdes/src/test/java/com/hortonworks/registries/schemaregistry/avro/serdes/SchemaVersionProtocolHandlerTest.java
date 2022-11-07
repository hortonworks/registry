/*
 * Copyright 2016-2022 Cloudera, Inc.
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
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import com.hortonworks.registries.serdes.Device;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Random;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaVersionProtocolHandlerTest {

    private SchemaRegistryClient mockSchemaRegistryClient;

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = mock(SchemaRegistryClient.class);
    }

    @Test
    public void testSerDesWithVersionIdGtLtIntMax() throws Exception {
        int delta = Math.abs(new Random().nextInt());
        long[] ids = {((long) Integer.MAX_VALUE + delta), // more than int max, should trigger VERSION_ID_AS_LONG_PROTOCOL
                ((long) Integer.MAX_VALUE - delta)}; // less than int max, should trigger VERSION_ID_AS_INT_PROTOCOL
        for (long id : ids) {
            testSerDes(id, SerDesProtocolHandlerRegistry.VERSION_ID_AS_INT_PROTOCOL);
        }
    }

    @Test
    public void testIntegerSerDesProtocolVersion() throws Exception {
        testSerDes(1L, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "3"})
    public void testStringSerDesProtocolVersion(String protocolVersion) throws Exception {
        testSerDes(1L, protocolVersion);
    }

    @ParameterizedTest
    @ValueSource(longs = {128, -10, 129, 260,
            3L + 2L * Integer.MAX_VALUE // test for integer overflow too
    })
    public void testSerDesProtocolVersionMustFitIntoAByte(long invalidValue) {
        AvroException exception = assertThrows(AvroException.class, () -> testSerDes(1L, invalidValue));

        assertEquals(
                "serdes.protocol.version value should be in [0, 127]",
                exception.getMessage());
    }

    @ValueSource(bytes = {10, 22, 33})
    @ParameterizedTest
    public void testNonExistingSerDesProtocolVersion(int invalidProtocolVersion) {
        AvroException exception = assertThrows(AvroException.class, () -> testSerDes(1L, invalidProtocolVersion));

        assertEquals(
                "SerDesProtocolHandler with protocol version " + invalidProtocolVersion + " does not exist",
                exception.getMessage());
    }

    private void testSerDes(Long id, Object serdesProtocolVersion) throws Exception {
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
        when(mockSchemaRegistryClient.getSchemaMetadataInfo(anyString())).thenReturn(new SchemaMetadataInfo(schemaMetadata));
        when(mockSchemaRegistryClient.addSchemaVersion(any(SchemaMetadata.class), any(SchemaVersion.class))).thenReturn(schemaIdVersion);
        when(mockSchemaRegistryClient.getSchemaVersionInfo(any(SchemaVersionKey.class))).thenReturn(schemaVersionInfo);
        when(mockSchemaRegistryClient.getSchemaVersionInfo(any(SchemaIdVersion.class))).thenReturn(schemaVersionInfo);

        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer(mockSchemaRegistryClient);
        serializer.init(Collections.singletonMap(SERDES_PROTOCOL_VERSION, serdesProtocolVersion));

        AvroSnapshotDeserializer deserializer = new AvroSnapshotDeserializer(mockSchemaRegistryClient);
        deserializer.init(Collections.emptyMap());

        byte[] serializedData = serializer.serialize(input, schemaMetadata);
        Object deserializedObj = deserializer.deserialize(new ByteArrayInputStream(serializedData), null);

        assertEquals(0, SpecificData.get().compare(input, deserializedObj, input.getSchema()));

        verify(mockSchemaRegistryClient, atMost(2)).getSchemaMetadataInfo(anyString());
        verify(mockSchemaRegistryClient, atMost(2)).addSchemaVersion(any(SchemaMetadata.class), any(SchemaVersion.class));
        verify(mockSchemaRegistryClient, atMost(2)).getSchemaVersionInfo(any(SchemaVersionKey.class));
    }
}
