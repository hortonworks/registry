/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.serdes.avro.kafka;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 *
 */
public class KafkaAvroSerializer implements Serializer<Object> {

    /**
     * Compatibility property to be set on configs for registering message's schema to schema registry.
     */
    public static final String SCHEMA_COMPATIBILITY = "schema.compatibility";

    private final AvroSnapshotSerializer avroSnapshotSerializer;
    private boolean isKey;
    private SchemaCompatibility compatibility;

    public KafkaAvroSerializer() {
        avroSnapshotSerializer = new AvroSnapshotSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        compatibility = (SchemaCompatibility) configs.get(SCHEMA_COMPATIBILITY);
        this.isKey = isKey;

        avroSnapshotSerializer.init(configs);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return avroSnapshotSerializer.serialize(data, createSchemaMetadata(topic));
    }

    private SchemaMetadata createSchemaMetadata(String topic) {
        SchemaMetadata schemaMetadata = getSchemaKey(topic, isKey);
        String description = "Schema registered by KafkaAvroSerializer for topic: [" + topic + "] iskey: [" + isKey + "]";
        return new SchemaMetadata.Builder(schemaMetadata).description(description).compatibility(compatibility).build();
    }

    protected SchemaMetadata getSchemaKey(String topic, boolean isKey) {
        return Utils.getSchemaKey(topic, isKey);
    }

    @Override
    public void close() {
        try {
            avroSnapshotSerializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
