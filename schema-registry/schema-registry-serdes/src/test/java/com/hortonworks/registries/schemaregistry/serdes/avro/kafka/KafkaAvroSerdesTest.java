/*
 * Copyright 2016-2019 Cloudera, Inc.
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

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.MockSchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSerDesHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.DefaultAvroSerDesHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.TestRecord;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 *
 */
public class KafkaAvroSerdesTest {

    private ISchemaRegistryClient schemaRegistryClient;
    private final String topic = "topic";
    private final Schema schema = new Schema.Parser().parse(
            "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.hortonworks.registries.schemaregistry.serdes.avro\"," +
                    "\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}," +
                    "{\"name\":\"field2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");

    @BeforeEach
    public void setup() {
        schemaRegistryClient = new MockSchemaRegistryClient();
    }

    @Test
    public void testLogicalTypeConversionWithRecordSchema() {

        Map<String, Object> serializerConfig = new HashMap<>();
        serializerConfig.put("logical.type.conversion.enabled", true);
        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(serializerConfig, false);

        Map<String, Object> deserializerConfig = new HashMap<>();
        deserializerConfig.put("logical.type.conversion.enabled", true);
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(deserializerConfig, false);

        Schema schemaWithLogicalTypes = SchemaBuilder
                .record("schema_name").namespace("ns")
                .fields()
                .name("decimal_bytes")
                .type(LogicalTypes.decimal(64).addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()
                .name("uuid_string")
                .type(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))).noDefault()
                .name("date_int")
                .type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                .name("time_millis_int")
                .type(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                .name("time_micros_long")
                .type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("timestamp_millis_long")
                .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("timestamp_micros_long")
                .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("local_timestamp_millis_long")
                .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .name("local_timestamp_micros_long")
                .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                .endRecord();

        GenericRecord avroRecord = new GenericRecordBuilder(schemaWithLogicalTypes)
                .set("decimal_bytes", new BigDecimal(987654321))
                .set("uuid_string", UUID.fromString("1bae91b3-f1ef-48ff-802a-3ded9c1ccf15"))
                .set("date_int", LocalDate.of(2023, 1, 1))
                .set("time_millis_int", LocalTime.of(1, 2, 3))
                .set("time_micros_long", LocalTime.of(1, 2, 3, 4000))
                .set("timestamp_millis_long", Instant.ofEpochMilli(123456789))
                .set("timestamp_micros_long", Instant.ofEpochSecond(1, 2000))
                .set("local_timestamp_millis_long", LocalDateTime.of(2023, 1, 1, 1, 1, 1))
                .set("local_timestamp_micros_long", LocalDateTime.of(2023, 1, 2, 3, 4, 5, 6000))
                .build();

        byte[] avroData = kafkaAvroSerializer.serialize(topic, avroRecord);
        GenericData.Record deserialized = (GenericData.Record) kafkaAvroDeserializer.deserialize(topic, avroData);

        Assertions.assertEquals(new BigDecimal(987654321), deserialized.get("decimal_bytes"));
        Assertions.assertEquals(UUID.fromString("1bae91b3-f1ef-48ff-802a-3ded9c1ccf15"),
                deserialized.get("uuid_string"));
        Assertions.assertEquals(LocalDate.of(2023, 1, 1), deserialized.get("date_int"));
        Assertions.assertEquals(LocalTime.of(1, 2, 3), deserialized.get("time_millis_int"));
        Assertions.assertEquals(LocalTime.of(1, 2, 3, 4000),
                deserialized.get("time_micros_long"));
        Assertions.assertEquals(Instant.ofEpochMilli(123456789), deserialized.get("timestamp_millis_long"));
        Assertions.assertEquals(Instant.ofEpochSecond(1, 2000), deserialized.get("timestamp_micros_long"));
        Assertions.assertEquals(LocalDateTime.of(2023, 1, 1, 1, 1, 1),
                deserialized.get("local_timestamp_millis_long"));
        Assertions.assertEquals(LocalDateTime.of(2023, 1, 2, 3, 4, 5, 6000),
                deserialized.get("local_timestamp_micros_long"));

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
        Assertions.assertEquals(TestRecord.class, o.getClass());
        TestRecord actual = (TestRecord) o;
        Assertions.assertEquals(expected.get("field1"), actual.getField1());
        Assertions.assertEquals(expected.get("field2"), actual.getField2());
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
        Assertions.assertEquals(TestRecord.class, o.getClass());
        TestRecord actual = (TestRecord) o;
        Assertions.assertEquals(expected.getField1(), actual.getField1());
        Assertions.assertEquals(expected.getField2(), actual.getField2());
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
        Assertions.assertEquals(GenericData.Record.class, o.getClass());
        GenericRecord result = (GenericRecord) o;
        Assertions.assertEquals(expected.getField1(), result.get("field1"));
        Assertions.assertEquals(expected.getField2(), result.get("field2"));
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

        byte[] bytes = kafkaAvroSerializer.serialize(topic, record);
        Object o = kafkaAvroDeserializer.deserialize(topic, bytes);
        checkGenericSerializedGenericDeserializedEquals(record, o);

        Headers headers = new RecordHeaders();
        bytes = kafkaAvroSerializer.serialize(topic, headers, record);
        o = kafkaAvroDeserializer.deserialize(topic, headers, bytes);
        checkGenericSerializedGenericDeserializedEquals(record, o);
    }

    private void checkGenericSerializedGenericDeserializedEquals(GenericRecord expected, Object o) {
        Assertions.assertEquals(GenericData.Record.class, o.getClass());
        GenericRecord actual = (GenericRecord) o;
        Assertions.assertEquals(expected.get("field1"), actual.get("field1"));
        Assertions.assertEquals(expected.get("field2"), actual.get("field2"));
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
        handler.handlePayloadSerialization(outputStream, record, false);

        for (Boolean isKey : Arrays.asList(true, false)) {
            KafkaAvroSerde serde = new KafkaAvroSerde(schemaRegistryClient);
            final Serializer<Object> serializer = serde.serializer();
            serializer.configure(configs, isKey);

            Headers headers = new RecordHeaders();
            final byte[] bytes = serializer.serialize(topic, headers, record);
            Assertions.assertArrayEquals(outputStream.toByteArray(), bytes);
            Assertions.assertEquals(isKey, headers.lastHeader(customKeySchemaHeaderName) != null);
            Assertions.assertEquals(!isKey, headers.lastHeader(customValueSchemaHeaderName) != null);

            final Deserializer<Object> deserializer = serde.deserializer();
            deserializer.configure(configs, isKey);
            final TestRecord actual = (TestRecord) deserializer.deserialize(topic, headers, bytes);
            Assertions.assertEquals(record, actual);
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
            final Serializer<Object> serializer = serde.serializer();
            serializer.configure(configs, true);

            Headers headers = new RecordHeaders();
            final byte[] bytes = serializer.serialize(topic, headers, record);
            Assertions.assertEquals(storeScheamIdInHeader, headers.lastHeader(keySchemaHeaderName) != null);

            final Deserializer<Object> deserializer = serde.deserializer();
            deserializer.configure(configs, true);
            final TestRecord actual = (TestRecord) deserializer.deserialize(topic, headers, bytes);
            Assertions.assertEquals(record, actual);
        }
    }

    @Test
    public void testNullWithoutPassthrough() {
        String topic = "topic";
        Map<String, Object> config = new HashMap<>();
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(config, false);

        byte[] data = kafkaAvroSerializer.serialize(topic, null);
        assertNotNull(data);
        assertNotEquals(0, data.length);

        Object deserialized = kafkaAvroDeserializer.deserialize(topic, data);
        assertNull(deserialized);
    }

    @Test
    public void testNullWithPassthrough() {
        String topic = "topic";
        Map<String, Object> config = new HashMap<>();
        config.put(KafkaAvroSerializer.NULL_PASS_THROUGH_ENABLED, "true");
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        kafkaAvroDeserializer.configure(config, false);

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        kafkaAvroSerializer.configure(config, false);

        byte[] data = kafkaAvroSerializer.serialize(topic, null);
        assertNull(data);

        Object deserialized = kafkaAvroDeserializer.deserialize(topic, data);
        assertNull(deserialized);
    }
}
