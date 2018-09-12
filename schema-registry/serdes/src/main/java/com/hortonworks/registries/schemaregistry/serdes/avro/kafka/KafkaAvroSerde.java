/**
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.serdes.avro.kafka;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ExtendedDeserializer;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaAvroSerde implements Serde<Object> {

    public static final String KEY_SCHEMA_HEADER_NAME = "key_schema_header_name";
    public static final String VALUE_SCHEMA_HEADER_NAME = "value_schema_header_name";

    private final ExtendedSerializer<Object> ser;
    private final ExtendedDeserializer<Object> deser;

    public KafkaAvroSerde() {
        ser = new KafkaAvroSerializer();
        deser = new KafkaAvroDeserializer();
    }

    public KafkaAvroSerde(ISchemaRegistryClient client) {
        ser = new KafkaAvroSerializer(client);
        deser = new KafkaAvroDeserializer(client);
    }

    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        ser.configure(configs, isKey);
        deser.configure(configs, isKey);
    }

    public void close(){
        ser.close();
        deser.close();
    }

    public Serializer<Object> serializer() {
        return ser;
    }

    public Deserializer<Object> deserializer() {
        return deser;
    }

    public ExtendedSerializer<Object> extendedSerializer() {
        return ser;
    }

    public ExtendedDeserializer<Object> extendedDeserializer() {
        return deser;
    }
}
