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

import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 *
 */
public class KafkaAvroDeserializer implements Deserializer<Object> {

    /**
     * This property represents the version of a reader schema to be used in deserialization. This property can be
     * passed to {@link #configure(Map, boolean)}.
     */
    String READER_VERSION = "schemaregistry.reader.schema.version";

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
        readerVersion = (Integer) configs.get(READER_VERSION);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        SchemaMetadata schemaMetadata = getSchemaKey(topic, isKey);
        return avroSnapshotDeserializer.deserialize(new ByteArrayInputStream(data),
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
