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
package com.hortonworks.registries.schemaregistry.serdes.avro.kafka;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaAvroDeserializerTest {

    @DisplayName("Should deserialize even if topic name does not match the schema name")
    @Test
    public void shouldDeserializeEvenWhenSchemaNameDoesNotMatchTopicName() throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException, IOException {
        long schemaVersionId = 1L;
        String schemaName = "[schema-name]";
        int versionNth = 1;
        ISchemaRegistryClient client = mockClientThatDoesntFindMetadata(
                schemaVersionId,
                schemaName,
                schema.toString(),
                versionNth);
        KafkaAvroDeserializer deser = new KafkaAvroDeserializer(client);
        deser.configure(emptyMap(), false);
        String topicName = "[topic-name]";

        Object deserialized = deser.deserialize(topicName, avro(client, topicName, avroRecord));
        Assertions.assertEquals(avroRecord, deserialized);
        verify(client).getSchemaVersionInfo(eq(new SchemaIdVersion(schemaVersionId)));
        verify(client).getSchemaVersionInfo(eq(new SchemaVersionKey(schemaName, versionNth)));
        verify(client, never()).getSchemaMetadataInfo(anyString());
    }

    private ISchemaRegistryClient mockClientThatDoesntFindMetadata(long schemaVersionId,
                                                                   String schemaName,
                                                                   String schemaText,
                                                                   int versionNth) throws SchemaNotFoundException, IncompatibleSchemaException, InvalidSchemaException {
        ISchemaRegistryClient mock = Mockito.mock(ISchemaRegistryClient.class);
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(schemaVersionId,
                schemaName,
                versionNth,
                schemaText,
                currentTimeMillis(),
                "[description]");
        when(mock.getSchemaVersionInfo(Mockito.any(SchemaIdVersion.class))).thenReturn(schemaVersionInfo);
        when(mock.getSchemaVersionInfo(Mockito.any(SchemaVersionKey.class))).thenReturn(schemaVersionInfo);
        RegistryException exception = new RegistryException("[schema not found]");
        when(mock.getSchemaMetadataInfo(schemaVersionInfo.getName())).thenThrow(exception);
        when(mock.addSchemaVersion(any(SchemaMetadata.class), any(SchemaVersion.class))).thenReturn(new SchemaIdVersion(schemaVersionId));
        return mock;
    }

    private byte[] avro(ISchemaRegistryClient client, String topicName, Object toSerialize) throws IOException {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(client);
        serializer.configure(emptyMap(), false);
        return serializer.serialize(topicName, toSerialize);
    }

    private Schema schema = SchemaBuilder
            .record("myrecord")
            .fields()
            .optionalString("field")
            .endRecord();

    private GenericRecord avroRecord = new GenericRecordBuilder(schema)
            .set("field", new Utf8("[string-value]"))
            .build();
}