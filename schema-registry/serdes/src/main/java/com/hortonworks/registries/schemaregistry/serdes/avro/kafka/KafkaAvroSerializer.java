/**
 * Copyright 2016 Hortonworks.
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

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageContext;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageContextBasedAvroSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;

import java.util.Map;

/**
 *
 */
public class KafkaAvroSerializer implements ExtendedSerializer<Object> {

    /**
     * Compatibility property to be set on configs for registering message's schema to schema registry.
     */
    public static final String SCHEMA_COMPATIBILITY = "schema.compatibility";

    public static final String SCHEMA_GROUP = "schema.group";
    public static final String SCHEMA_NAME_KEY_SUFFIX_ = "schema.name.key.suffix";
    public static final String SCHEMA_NAME_VALUE_SUFFIX_= "schema.name.value.suffix";
    public static final String STORE_SCHEMA_IN_HEADER = "store.schema.in.header";

    public static final String DEFAULT_SCHEMA_GROUP = "kafka";
    public static final String DEFAULT_SCHEMA_NAME_KEY_SUFFIX = ":k";
    public static final String DEFAULT_SCHEMA_NAME_VALUE_SUFFIX = null;
    public static final String DEFAULT_STORE_SCHEMA_IN_HEADER = "true";

    private boolean isKey;
    private final AvroSnapshotSerializer avroSnapshotSerializer;

    private final MessageContextBasedAvroSerializer messageContextBasedAvroSerializer;
    private String keySchemaHeaderName;
    private String valueSchemaHeaderName;
    private boolean useRecordHeader;

    private SchemaCompatibility compatibility;

    private String schemaGroup;
    private String schemaNameKeySuffix;
    private String schemaNameValueSuffix;

    public KafkaAvroSerializer() {
        avroSnapshotSerializer = new AvroSnapshotSerializer();
        messageContextBasedAvroSerializer = new MessageContextBasedAvroSerializer();
    }

    public KafkaAvroSerializer(ISchemaRegistryClient schemaRegistryClient) {
        avroSnapshotSerializer = new AvroSnapshotSerializer(schemaRegistryClient);
        messageContextBasedAvroSerializer = new MessageContextBasedAvroSerializer(schemaRegistryClient);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        compatibility = (SchemaCompatibility) configs.get(SCHEMA_COMPATIBILITY);

        schemaGroup = getOrDefault(configs, SCHEMA_GROUP, DEFAULT_SCHEMA_GROUP);
        schemaNameKeySuffix = getOrDefault(configs, SCHEMA_NAME_KEY_SUFFIX_, DEFAULT_SCHEMA_NAME_KEY_SUFFIX);
        schemaNameValueSuffix = getOrDefault(configs, SCHEMA_NAME_VALUE_SUFFIX_, DEFAULT_SCHEMA_NAME_VALUE_SUFFIX);

        this.isKey = isKey;
        keySchemaHeaderName = getOrDefault(configs, KafkaAvroSerde.KEY_SCHEMA_HEADER_NAME, KafkaAvroSerde.KEY_SCHEMA_HEADER_NAME);
        valueSchemaHeaderName = getOrDefault(configs, KafkaAvroSerde.VALUE_SCHEMA_HEADER_NAME, KafkaAvroSerde.VALUE_SCHEMA_HEADER_NAME);
        useRecordHeader = Boolean.valueOf(getOrDefault(configs, STORE_SCHEMA_IN_HEADER, DEFAULT_STORE_SCHEMA_IN_HEADER));

        avroSnapshotSerializer.init(configs);
        messageContextBasedAvroSerializer.init(configs);
    }

    private static String getOrDefault(Map<String, ?> configs, String key, String defaultValue) {
        String value = (String) configs.get(key);
        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }
        return value;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return avroSnapshotSerializer.serialize(data, createSchemaMetadata(topic));
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        if (useRecordHeader) {
            final MessageContext context = messageContextBasedAvroSerializer.serialize(data, createSchemaMetadata(topic));
            headers.add(isKey ? keySchemaHeaderName : valueSchemaHeaderName, context.metadata());
            return context.payload();
        } else {
            return serialize(topic, data);
        }
    }

    private SchemaMetadata createSchemaMetadata(String topic) {
        SchemaMetadata schemaMetadata = getSchemaKey(topic, isKey);
        String description = "Schema registered by KafkaAvroSerializer for topic: [" + topic + "] iskey: [" + isKey + "]";
        return new SchemaMetadata.Builder(schemaMetadata).description(description).compatibility(compatibility).build();
    }

    public SchemaMetadata getSchemaKey(String topic, boolean isKey) {
        String name = topic;
        if(isKey) {
            if (schemaNameKeySuffix != null ) {
                name += schemaNameKeySuffix;
            }
        } else {
            if (schemaNameValueSuffix != null ) {
                name += schemaNameValueSuffix;
            }
        }
        // there wont be any naming collisions as kafka does not allow character `:` in a topic name.
        return new SchemaMetadata.Builder(name).type(AvroSchemaProvider.TYPE).schemaGroup(schemaGroup).build();
    }

    @Override
    public void close() {
        try {
            avroSnapshotSerializer.close();
            messageContextBasedAvroSerializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}