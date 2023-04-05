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

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.json.JsonSchemaProvider;
import com.hortonworks.registries.schemaregistry.serdes.Utils;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageAndMetadata;
import com.hortonworks.registries.schemaregistry.serdes.json.JsonSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.json.MessageAndMetadataJsonSerializer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaJsonSerializer  implements Serializer<Object> {

  /**
   * Compatibility property to be set on configs for registering message's schema to schema registry.
   */
  public static final String SCHEMA_COMPATIBILITY = "schema.compatibility";

  public static final String SCHEMA_GROUP = "schema.group";
  public static final String SCHEMA_NAME_KEY_SUFFIX = "schema.name.key.suffix";
  public static final String SCHEMA_NAME_VALUE_SUFFIX = "schema.name.value.suffix";
  public static final String STORE_SCHEMA_VERSION_ID_IN_HEADER = "store.schema.version.id.in.header";

  public static final String DEFAULT_SCHEMA_GROUP = "kafka";
  public static final String DEFAULT_SCHEMA_NAME_KEY_SUFFIX = ":k";
  public static final String DEFAULT_SCHEMA_NAME_VALUE_SUFFIX = null;
  public static final String DEFAULT_STORE_SCHEMA_VERSION_ID_IN_HEADER = "false";

  private boolean isKey;
  private final JsonSnapshotSerializer jsonSnapshotSerializer;

  private final MessageAndMetadataJsonSerializer messageAndMetadataJsonSerializer;
  private String keySchemaVersionIdHeaderName;
  private String valueSchemaVersionIdHeaderName;
  private boolean useRecordHeader;

  private SchemaCompatibility compatibility;

  private String schemaGroup;
  private String schemaNameKeySuffix;
  private String schemaNameValueSuffix;

  public KafkaJsonSerializer() {
    jsonSnapshotSerializer = new JsonSnapshotSerializer();
    messageAndMetadataJsonSerializer = new MessageAndMetadataJsonSerializer();
  }

  public KafkaJsonSerializer(ISchemaRegistryClient schemaRegistryClient) {
    jsonSnapshotSerializer = new JsonSnapshotSerializer(schemaRegistryClient);
    messageAndMetadataJsonSerializer = new MessageAndMetadataJsonSerializer(schemaRegistryClient);
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String compatSetting = Utils.getOrDefaultAsString(configs, SCHEMA_COMPATIBILITY, "").toUpperCase();
    if (!"".equals(compatSetting)) {
      compatibility = SchemaCompatibility.valueOf(compatSetting);
    }

    schemaGroup = Utils.getOrDefaultAsString(configs, SCHEMA_GROUP, DEFAULT_SCHEMA_GROUP);
    schemaNameKeySuffix = Utils.getOrDefaultAsString(configs, SCHEMA_NAME_KEY_SUFFIX, DEFAULT_SCHEMA_NAME_KEY_SUFFIX);
    schemaNameValueSuffix = Utils.getOrDefaultAsString(configs, SCHEMA_NAME_VALUE_SUFFIX, DEFAULT_SCHEMA_NAME_VALUE_SUFFIX);

    this.isKey = isKey;
    keySchemaVersionIdHeaderName = Utils.getOrDefaultAsString(configs,
        KafkaJsonSerde.KEY_SCHEMA_VERSION_ID_HEADER_NAME, KafkaJsonSerde.DEFAULT_KEY_SCHEMA_VERSION_ID);
    if (keySchemaVersionIdHeaderName == null || keySchemaVersionIdHeaderName.isEmpty()) {
      throw new IllegalArgumentException("keySchemaVersionIdHeaderName should not be null or empty");
    }

    valueSchemaVersionIdHeaderName = Utils.getOrDefaultAsString(configs,
        KafkaJsonSerde.VALUE_SCHEMA_VERSION_ID_HEADER_NAME, KafkaJsonSerde.DEFAULT_VALUE_SCHEMA_VERSION_ID);
    if (valueSchemaVersionIdHeaderName == null || valueSchemaVersionIdHeaderName.isEmpty()) {
      throw new IllegalArgumentException("valueSchemaVersionIdHeaderName should not be null or empty");
    }

    useRecordHeader = Boolean.valueOf(Utils.getOrDefaultAsString(configs,
        STORE_SCHEMA_VERSION_ID_IN_HEADER, DEFAULT_STORE_SCHEMA_VERSION_ID_IN_HEADER));

    jsonSnapshotSerializer.init(configs);
    messageAndMetadataJsonSerializer.init(configs);
  }

  @Override
  public byte[] serialize(String topic, Object data) {
    if (data == null) {
      return null;
    }
    return jsonSnapshotSerializer.serialize(data, createSchemaMetadata(topic));
  }

  @Override
  public byte[] serialize(String topic, Headers headers, Object data) {
    if (data == null) {
      return null;
    }
    if (useRecordHeader) {
      final MessageAndMetadata context = messageAndMetadataJsonSerializer.serialize(data, createSchemaMetadata(topic));
      headers.add(isKey ? keySchemaVersionIdHeaderName : valueSchemaVersionIdHeaderName, context.metadata());
      return context.payload();
    } else {
      return serialize(topic, data);
    }
  }

  private SchemaMetadata createSchemaMetadata(String topic) {
    SchemaMetadata schemaMetadata = getSchemaKey(topic, isKey);
    String description = "Schema registered by KafkaJsonSerializer for topic: [" + topic + "] iskey: [" + isKey + "]";
    SchemaMetadata.Builder builder = new SchemaMetadata.Builder(schemaMetadata).description(description);
    if (compatibility != null) {
      builder.compatibility(compatibility);
    }
    return builder.build();
  }

  public SchemaMetadata getSchemaKey(String topic, boolean isKey) {
    String name = topic;
    if (isKey) {
      if (schemaNameKeySuffix != null) {
        name += schemaNameKeySuffix;
      }
    } else {
      if (schemaNameValueSuffix != null) {
        name += schemaNameValueSuffix;
      }
    }
    // there wont be any naming collisions as kafka does not allow character `:` in a topic name.
    return new SchemaMetadata.Builder(name).type(JsonSchemaProvider.TYPE).schemaGroup(schemaGroup).build();
  }

  @Override
  public void close() {
    try {
      Utils.closeAll(jsonSnapshotSerializer, messageAndMetadataJsonSerializer);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
