/*
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.serdes.json.kafka;

import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.Utils;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageAndMetadata;
import com.hortonworks.registries.schemaregistry.serdes.json.JsonSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.json.MessageAndMetadataJsonDeserializer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Map;

public class KafkaJsonDeserializer implements Deserializer<Object> {

  public static final String READER_VERSIONS = "schemaregistry.reader.schema.versions";

  private boolean isKey;
  private Map<String, Integer> readerVersions;

  private final JsonSnapshotDeserializer jsonSnapshotDeserializer;
  private final MessageAndMetadataJsonDeserializer messageAndMetadataJsonDeserializer;
  private String keySchemaVersionIdHeaderName;
  private String valueSchemaVersionIdHeaderName;

  public KafkaJsonDeserializer() {
    jsonSnapshotDeserializer = new JsonSnapshotDeserializer();
    messageAndMetadataJsonDeserializer = new MessageAndMetadataJsonDeserializer();
  }

  public KafkaJsonDeserializer(ISchemaRegistryClient schemaRegistryClient) {
    jsonSnapshotDeserializer = new JsonSnapshotDeserializer(schemaRegistryClient);
    messageAndMetadataJsonDeserializer = new MessageAndMetadataJsonDeserializer(schemaRegistryClient);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.keySchemaVersionIdHeaderName = Utils.getOrDefault(configs,
        KafkaJsonSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, KafkaJsonSerde.DEFAULT_KEY_SCHEMA_VERSION_ID);
    if (keySchemaVersionIdHeaderName == null || keySchemaVersionIdHeaderName.isEmpty()) {
      throw new IllegalArgumentException("keySchemaVersionIdHeaderName should not be null or empty");
    }

    this.valueSchemaVersionIdHeaderName = Utils.getOrDefault(configs,
        KafkaJsonSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, KafkaJsonSerde.DEFAULT_VALUE_SCHEMA_VERSION_ID);
    if (valueSchemaVersionIdHeaderName == null || valueSchemaVersionIdHeaderName.isEmpty()) {
      throw new IllegalArgumentException("valueSchemaVersionIdHeaderName should not be null or empty");
    }

    Map<String, Integer> versions = (Map<String, Integer>) ((Map<String, Object>) configs).get(READER_VERSIONS);
    readerVersions = versions != null ? versions : Collections.emptyMap();

    jsonSnapshotDeserializer.init(configs);
    messageAndMetadataJsonDeserializer.init(configs);
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    return jsonSnapshotDeserializer.deserialize(new ByteArrayInputStream(data), readerVersions.get(topic));
  }

  @Override
  public Object deserialize(String topic, Headers headers, byte[] data) {
    if (headers != null) {
      final Header header = headers.lastHeader(isKey ? keySchemaVersionIdHeaderName : valueSchemaVersionIdHeaderName);
      if (header != null) {
        return messageAndMetadataJsonDeserializer.deserialize(new MessageAndMetadata(header.value(), data), readerVersions.get(topic));
      }
    }
    return deserialize(topic, data);
  }

  @Override
  public void close() {
    try {
      Utils.closeAll(jsonSnapshotDeserializer, messageAndMetadataJsonDeserializer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
