/**
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
 **/
package com.hortonworks.registries.schemaregistry.serdes.json.kafka;

import com.fasterxml.jackson.databind.node.TextNode;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaJsonDeserializerTest {

    @Test
    void deserializesIntoAJsonNode() throws IOException, SchemaNotFoundException {
        long schemaVersionId = 3L;
        StringSchema jsonSchema = StringSchema.builder().build();
        ISchemaRegistryClient schemaRegistryClient = mockSchemaRegistryClient(schemaVersionId, jsonSchema.toString());
        KafkaJsonDeserializer kafkaJsonDeserializer = new KafkaJsonDeserializer(schemaRegistryClient);
        kafkaJsonDeserializer.configure(Collections.emptyMap(), false);

        String stringJsonLiteral = "[string-value]";
        byte[] jsonAsBytes = writeToStream(quote(stringJsonLiteral), schemaVersionId).toByteArray();
        Object deserialized = kafkaJsonDeserializer.deserialize("[topic]", jsonAsBytes);

        assertEquals(TextNode.valueOf(stringJsonLiteral), deserialized);
    }

    @Test
    void readsNullAsNull() {
        ISchemaRegistryClient schemaRegistryClient = Mockito.mock(ISchemaRegistryClient.class);
        KafkaJsonDeserializer kafkaJsonDeserializer = new KafkaJsonDeserializer(schemaRegistryClient);
        kafkaJsonDeserializer.configure(Collections.emptyMap(), false);

        assertNull(kafkaJsonDeserializer.deserialize("topic", null));
    }

    private String quote(String stringJsonLiteral) {
        return "\"" + stringJsonLiteral + "\"";
    }

    private ISchemaRegistryClient mockSchemaRegistryClient(long schemaVersionId, String schemaText) throws SchemaNotFoundException {
        ISchemaRegistryClient mock = Mockito.mock(ISchemaRegistryClient.class);
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(schemaVersionId, "[name]", 1, schemaText, System.currentTimeMillis(), "[description]");
        Mockito.when(mock.getSchemaVersionInfo(Mockito.any(SchemaIdVersion.class))).thenReturn(schemaVersionInfo);
        Mockito.when(mock.getSchemaVersionInfo(Mockito.any(SchemaVersionKey.class))).thenReturn(schemaVersionInfo);
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("[name")
                .type("avro")
                .schemaGroup("[group]")
                .evolve(true)
                .description("[desc]")
                .build();
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata);
        Mockito.when(mock.getSchemaMetadataInfo(schemaVersionInfo.getName())).thenReturn(schemaMetadataInfo);
        return mock;
    }

    private ByteArrayOutputStream writeToStream(String json, long schemaVersionId) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(0x4);
        outputStream.write(ByteBuffer.allocate(4).putInt((int) schemaVersionId).array());
        outputStream.write(json.getBytes(StandardCharsets.UTF_8));
        return outputStream;
    }
}