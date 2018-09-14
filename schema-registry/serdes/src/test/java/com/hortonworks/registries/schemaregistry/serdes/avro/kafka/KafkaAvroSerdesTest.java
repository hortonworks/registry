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
package com.hortonworks.registries.schemaregistry.serdes.avro.kafka;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.MockSchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSerDesHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.DefaultAvroSerDesHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.TestRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class KafkaAvroSerdesTest {

    private ISchemaRegistryClient schemaRegistryClient;
    private final String topic = "topic";
    private final Schema schema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.hortonworks.registries.schemaregistry.serdes.avro\",\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":\"field2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");

    @Before
    public void setup() {
        schemaRegistryClient = new MockSchemaRegistryClient();
    }

    @Test
    public void testGenericSerializedSpecificDeserialized() {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(Collections.emptyMap(), false);

        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();

        byte[] payload = kafkaAvroSerializer.serialize(topic, record);
        Object o = kafkaAvroDeserializer.deserialize(topic, payload);
        checkGenericSerializedSpecificDeserializedEquals(record, o);

        Headers headers = new RecordHeaders();
        payload = kafkaAvroSerializer.serialize(topic, headers, record);
        o = kafkaAvroDeserializer.deserialize(topic, headers, payload);
        checkGenericSerializedSpecificDeserializedEquals(record, o);
    }

    private void checkGenericSerializedSpecificDeserializedEquals(GenericRecord expected, Object o) {
        Assert.assertEquals(o.getClass(), TestRecord.class);
        TestRecord actual = (TestRecord) o;
        Assert.assertEquals(expected.get("field1"), actual.getField1());
        Assert.assertEquals(expected.get("field2"), actual.getField2());
    }

    @Test
    public void testSpecificSerializedSpecificDeserialized() {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(Collections.emptyMap(), false);

        TestRecord record = new TestRecord();
        record.setField1("some value");
        record.setField1("some other value");

        byte[] bytes = kafkaAvroSerializer.serialize(topic, record);
        Object o = kafkaAvroDeserializer.deserialize(topic, bytes);
        checkSpecificSerializedSpecificDeserializedEquals(record, o);

        Headers headers = new RecordHeaders();
        bytes = kafkaAvroSerializer.serialize(topic, headers, record);
        o = kafkaAvroDeserializer.deserialize(topic, headers, bytes);
        checkSpecificSerializedSpecificDeserializedEquals(record, o);
    }

    private void checkSpecificSerializedSpecificDeserializedEquals(TestRecord expected, Object o) {
        Assert.assertEquals(o.getClass(), TestRecord.class);
        TestRecord actual = (TestRecord) o;
        Assert.assertEquals(expected.getField1(), actual.getField1());
        Assert.assertEquals(expected.getField2(), actual.getField2());
    }

    @Test
    public void testSpecificSerializedGenericDeserialized() {
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, false);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(config, false);

        TestRecord record = new TestRecord();
        record.setField1("some value");
        record.setField1("some other value");

        byte[] bytes = kafkaAvroSerializer.serialize(topic, record);
        Object o = kafkaAvroDeserializer.deserialize(topic, bytes);
        checkSpecificSerializedGenericDeserializedEquals(record, o);

        Headers headers = new RecordHeaders();
        bytes = kafkaAvroSerializer.serialize(topic, headers, record);
        o = kafkaAvroDeserializer.deserialize(topic, headers, bytes);
        checkSpecificSerializedGenericDeserializedEquals(record, o);
    }

    private void checkSpecificSerializedGenericDeserializedEquals(TestRecord expected, Object o) {
        Assert.assertEquals(o.getClass(), GenericData.Record.class);
        GenericRecord result = (GenericRecord) o;
        Assert.assertEquals(expected.getField1(), result.get("field1"));
        Assert.assertEquals(expected.getField2(), result.get("field2"));
    }

    @Test
    public void testGenericSerializedGenericDeserialized() {
        String topic = "topic";
        Map<String, Object> config = new HashMap<>();
        config.put(AvroSnapshotDeserializer.SPECIFIC_AVRO_READER, false);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(config, false);

        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();

        byte[] bytes = kafkaAvroSerializer.serialize(topic , record);
        Object o = kafkaAvroDeserializer.deserialize(topic, bytes);
        checkGenericSerializedGenericDeserializedEquals(record, o);

        Headers headers = new RecordHeaders();
        bytes = kafkaAvroSerializer.serialize(topic, headers, record);
        o = kafkaAvroDeserializer.deserialize(topic, headers, bytes);
        checkGenericSerializedGenericDeserializedEquals(record, o);
    }

    private void checkGenericSerializedGenericDeserializedEquals(GenericRecord expected, Object o) {
        Assert.assertEquals(o.getClass(), GenericData.Record.class);
        GenericRecord actual = (GenericRecord) o;
        Assert.assertEquals(expected.get("field1"), actual.get("field1"));
        Assert.assertEquals(expected.get("field2"), actual.get("field2"));
    }

    @Test
    public void testDefaultSchemaHeaderNames() {
        testSchemaHeaderNames(KafkaAvroSerde.DEFAULT_KEY_SCHEMA_VERSION_ID, KafkaAvroSerde.DEFAULT_VALUE_SCHEMA_VERSION_ID);
    }

    @Test
    public void testCustomSchemaHeaderNames() {
        testSchemaHeaderNames("ksid", "vsid");
    }

    private void testSchemaHeaderNames(String customKeySchemaHeaderName,
                                       String customValueSchemaHeaderName) {
        TestRecord record = new TestRecord();
        record.setField1("Hello");
        record.setField2("World");

        Map<String, Object> configs = new HashMap<>();
        configs.put(KafkaAvroSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, customKeySchemaHeaderName);
        configs.put(KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, customValueSchemaHeaderName);
        configs.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER, "true");
        configs.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AvroSerDesHandler handler = new DefaultAvroSerDesHandler();
        handler.handlePayloadSerialization(outputStream, record);

        for (Boolean isKey : Arrays.asList(true, false)) {
            KafkaAvroSerde serde = new KafkaAvroSerde(schemaRegistryClient);
            final ExtendedSerializer<Object> serializer = serde.extendedSerializer();
            serializer.configure(configs, isKey);

            Headers headers = new RecordHeaders();
            final byte[] bytes = serializer.serialize(topic, headers, record);
            Assert.assertArrayEquals(outputStream.toByteArray(), bytes);
            Assert.assertEquals(isKey, headers.lastHeader(customKeySchemaHeaderName) != null);
            Assert.assertEquals(!isKey, headers.lastHeader(customValueSchemaHeaderName) != null);

            final ExtendedDeserializer<Object> deserializer = serde.extendedDeserializer();
            deserializer.configure(configs, isKey);
            final TestRecord actual = (TestRecord) deserializer.deserialize(topic, headers, bytes);
            Assert.assertEquals(record, actual);
        }
    }

    @Test
    public void testToggleStoringSchemaInHeader() {
        TestRecord record = new TestRecord();
        record.setField1("Hello");
        record.setField2("World");
        String keySchemaHeaderName = KafkaAvroSerde.DEFAULT_KEY_SCHEMA_VERSION_ID;

        for (Boolean storeScheamIdInHeader : Arrays.asList(true, false)) {
            Map<String, Object> configs = new HashMap<>();
            configs.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER, storeScheamIdInHeader.toString());
            configs.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);

            KafkaAvroSerde serde = new KafkaAvroSerde(schemaRegistryClient);
            final ExtendedSerializer<Object> serializer = serde.extendedSerializer();
            serializer.configure(configs, true);

            Headers headers = new RecordHeaders();
            final byte[] bytes = serializer.serialize(topic, headers, record);
            Assert.assertEquals(storeScheamIdInHeader, headers.lastHeader(keySchemaHeaderName) != null);

            final ExtendedDeserializer<Object> deserializer = serde.extendedDeserializer();
            deserializer.configure(configs, true);
            final TestRecord actual = (TestRecord) deserializer.deserialize(topic, headers, bytes);
            Assert.assertEquals(record, actual);
        }
    }
}
