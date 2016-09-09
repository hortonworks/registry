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
package com.hortonworks.registries.schemaregistry.avro.kafka;

import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.avro.AvroSnapshotDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 *
 */
public class KafkaAvroDeserializer implements Deserializer<Object> {
    private final AvroSnapshotDeserializer avroSnapshotDeserializer;

    private boolean isKey;
    private SchemaProvider.Compatibility compatibility;

    public KafkaAvroDeserializer() {
        avroSnapshotDeserializer = new AvroSnapshotDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        avroSnapshotDeserializer.init(configs);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(data),
                                                    Utils.getSchemaMetadataKey(topic, isKey),
                                                    null);
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
