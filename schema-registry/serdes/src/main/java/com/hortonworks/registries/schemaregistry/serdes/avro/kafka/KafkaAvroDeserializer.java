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
 **/
package com.hortonworks.registries.schemaregistry.serdes.avro.kafka;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.Utils;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageAndMetadata;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageAndMetadataAvroDeserializer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Map;

/**
 * This class can be configured as key or value deserializer for kafka consumer. This can be used like below with kafka consumers.
 *
 * <pre>{@code
        // consumer configs
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        // schema registry config
        props.putAll(Collections.singletonMap(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl));

        // configure reader versions for topics to be consumed.
        Map<String, Integer> readerVersions = new HashMap<>();
        readerVersions.put("clicks", 2);
        readerVersions.put("users", 1);
        props.put(KafkaAvroDeserializer.READER_VERSIONS, readerVersions);

        // key deserializer
        // current props are passed to KafkaAvroDeserializer instance by invoking #configure(Map, boolean) method.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // value deserializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // If STORE_SCHEMA_VERSION_ID_IN_HEADER is enabled in {@code {@link KafkaAvroSerializer}} and
        // KEY_SCHEMA_VERSION_ID_HEADER_NAME / VALUE_SCHEMA_VERSION_ID_HEADER_NAME is renamed,
        // then the updated name should be specified here
        props.put(KafkaAvroSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, "ksvid");
        props.put(KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, "vsvid");

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props)) {
            ...
        }

 * }</pre>
 *
 */
public class KafkaAvroDeserializer implements Deserializer<Object> {

    /**
     * This property represents the version of a reader schema to be used in deserialization for each topic in
     * {@link #deserialize(String, byte[])}. This property should be passed with configs argument to {@link #configure(Map, boolean)}.
     * <p>
     * For example, to set reader version of different topics which should be handled by this Deserializer.
     * <pre>{@code
     * Map<String, Object> configs = ...
     * Map<String, Integer> readerVersions = new HashMap<>();
     * readerVersions.put("clicks", 2);
     * readerVersions.put("users", 1);
     * configs.put(READER_VERSIONS, readerVersions);
     * }</pre>
     */
    public static final String READER_VERSIONS = "schemaregistry.reader.schema.versions";

    private boolean isKey;
    private Map<String, Integer> readerVersions;

    private final AvroSnapshotDeserializer avroSnapshotDeserializer;
    private final MessageAndMetadataAvroDeserializer messageAndMetadataAvroDeserializer;
    private String keySchemaVersionIdHeaderName;
    private String valueSchemaVersionIdHeaderName;

    public KafkaAvroDeserializer() {
        avroSnapshotDeserializer = new AvroSnapshotDeserializer();
        messageAndMetadataAvroDeserializer = new MessageAndMetadataAvroDeserializer();
    }

    public KafkaAvroDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        avroSnapshotDeserializer = new AvroSnapshotDeserializer(schemaRegistryClient);
        messageAndMetadataAvroDeserializer = new MessageAndMetadataAvroDeserializer(schemaRegistryClient);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.keySchemaVersionIdHeaderName = Utils.getOrDefault(configs, KafkaAvroSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, KafkaAvroSerde.DEFAULT_KEY_SCHEMA_VERSION_ID);
        if (keySchemaVersionIdHeaderName == null || keySchemaVersionIdHeaderName.isEmpty()) {
            throw new IllegalArgumentException("keySchemaVersionIdHeaderName should not be null or empty");
        }

        this.valueSchemaVersionIdHeaderName = Utils.getOrDefault(configs, KafkaAvroSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, KafkaAvroSerde.DEFAULT_VALUE_SCHEMA_VERSION_ID);
        if (valueSchemaVersionIdHeaderName == null || valueSchemaVersionIdHeaderName.isEmpty()) {
            throw new IllegalArgumentException("valueSchemaVersionIdHeaderName should not be null or empty");
        }

        Map<String, Integer> versions = (Map<String, Integer>) ((Map<String, Object>) configs).get(READER_VERSIONS);
        readerVersions = versions != null ? versions : Collections.emptyMap();

        avroSnapshotDeserializer.init(configs);
        messageAndMetadataAvroDeserializer.init(configs);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(data), readerVersions.get(topic));
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        if (headers != null) {
            final Header header = headers.lastHeader(isKey ? keySchemaVersionIdHeaderName : valueSchemaVersionIdHeaderName);
            if (header != null) {
                return messageAndMetadataAvroDeserializer.deserialize(new MessageAndMetadata(header.value(), data), readerVersions.get(topic));
            }
        }
        return deserialize(topic, data);
    }

    @Override
    public void close() {
        try {
            Utils.closeAll(avroSnapshotDeserializer, messageAndMetadataAvroDeserializer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
