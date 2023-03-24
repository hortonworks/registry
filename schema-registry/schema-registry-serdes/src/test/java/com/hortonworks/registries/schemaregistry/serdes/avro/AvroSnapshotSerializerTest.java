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
package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.serdes.SerDesProtocolHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;

import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AvroSnapshotSerializerTest {

    @Test
    public void currentProtocolVersionisUsedWhenNotConfigured() {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.init(Collections.emptyMap());
        assertEquals(SerDesProtocolHandlerRegistry.CURRENT_PROTOCOL, serializer.serDesProtocolHandler.getProtocolId());
    }

    @Test
    public void currentProtocolVersionisUsedWhenConfigValIsNull() {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.init(Collections.singletonMap(SERDES_PROTOCOL_VERSION, null));
        assertEquals(SerDesProtocolHandlerRegistry.CURRENT_PROTOCOL, serializer.serDesProtocolHandler.getProtocolId());
    }

    @Test
    public void invalidConfigResultsInNumberFormatException() {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();

        Assertions.assertThrows(NumberFormatException.class,
                () -> serializer.init(Collections.singletonMap(SERDES_PROTOCOL_VERSION, "[invalid]")));

    }

    @Test
    public void serializes() {
        SerDesProtocolHandler protocolHandler = Mockito.mock(SerDesProtocolHandler.class);
        byte protoVersion = 0x05;
        when(protocolHandler.getProtocolId()).thenReturn(protoVersion);
        SerDesProtocolHandlerRegistry.get().registerSerDesProtocolHandler(protocolHandler);

        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        serializer.init(Collections.singletonMap(SERDES_PROTOCOL_VERSION, protoVersion));
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(1L);
        String object = "object";
        serializer.doSerialize(object, schemaIdVersion);

        verify(protocolHandler).handleSchemaVersionSerialization(any(OutputStream.class), eq(schemaIdVersion));
        verify(protocolHandler).handlePayloadSerialization(any(OutputStream.class), refEq(object),
                anyMap());
    }

    @ArgumentsSource(ZeroProtocolVersionProvider.class)
    @ParameterizedTest
    public void serdesProtocolVersionShouldBeAccepted(Object protocolVersion) {
        AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();
        Map<String, Object> configs = Collections.singletonMap(SERDES_PROTOCOL_VERSION, protocolVersion);
        serializer.init(configs);
        assertEquals(Integer.valueOf(0).byteValue(), serializer.serDesProtocolHandler.getProtocolId());
    }


    private static class ZeroProtocolVersionProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.<Arguments>builder()
                    .add(Arguments.of(0))
                    .add(Arguments.of(0L))
                    .add(Arguments.of(Byte.valueOf("0")))
                    .add(Arguments.of("0"))
                    .build();
        }
    }
}