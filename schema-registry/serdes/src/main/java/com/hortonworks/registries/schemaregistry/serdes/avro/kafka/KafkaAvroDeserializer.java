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

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 *
 */
public class KafkaAvroDeserializer implements Deserializer<Object> {
    private final AvroSnapshotDeserializer avroSnapshotDeserializer;

    private boolean isKey;
    private Integer readerVersion;

    public KafkaAvroDeserializer() {
        avroSnapshotDeserializer = new AvroSnapshotDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        avroSnapshotDeserializer.init(configs);
        readerVersion = (Integer) configs.get(SnapshotDeserializer.READER_VERSION);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        SchemaMetadata schemaMetadata = getSchemaKey(topic, isKey);
        return avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(data),
                schemaMetadata,
                readerVersion);
    }

    protected SchemaMetadata getSchemaKey(String topic, boolean isKey) {
        return Utils.getSchemaKey(topic, isKey);
    }

    @Override
    public void close() {
        try {
            avroSnapshotDeserializer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
