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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaMetadataKey;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaMetadata;

import java.util.Map;

/**
 *
 */
public class KafkaSerializer implements org.apache.kafka.common.serialization.Serializer<Object> {
    private static final String GROUP_ID = "kafka";

    private final AvroSnapshotSerializer avroSnapshotSerializer;
    private boolean isKey;
    private SchemaProvider.Compatibility compatibility;

    public KafkaSerializer(AvroSnapshotSerializer avroSnapshotSerializer) {
        this.avroSnapshotSerializer = avroSnapshotSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        compatibility = (SchemaProvider.Compatibility) configs.get("compatibility");
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return avroSnapshotSerializer.serialize(data, createSchemaMetadataInfo(topic, data));
    }

    private SchemaMetadata createSchemaMetadataInfo(String topic, Object data) {
        String schemaName = getSchemaName(topic, data);
        return new SchemaMetadata(new SchemaMetadataKey(AvroSnapshotSerializer.TYPE, GROUP_ID, schemaName), schemaName, compatibility);
    }

    protected String getSchemaName(String topic, Object data) {
        return topic + ":" + (isKey ? "k" : "v");
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
