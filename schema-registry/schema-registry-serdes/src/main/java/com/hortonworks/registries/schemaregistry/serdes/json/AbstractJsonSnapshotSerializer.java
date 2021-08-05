/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.serdes.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.json.JsonUtils;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig;
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft;
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator;

import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractJsonSnapshotSerializer<O> extends AbstractSnapshotSerializer<Object, O> {

  protected final JsonSerDesProtocolHandler protocolHandler = new JsonSerDesProtocolHandler();
  protected final ObjectMapper objectMapper = JsonUtils.getObjectMapper();

  public AbstractJsonSnapshotSerializer() { }

  public AbstractJsonSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  @Override
  protected String getSchemaText(Object input) {
    try {
      JsonSchemaConfig config = JsonSchemaConfig.nullableJsonSchemaDraft4().withJsonSchemaDraft(JsonSchemaDraft.DRAFT_07);
      JsonSchemaGenerator jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, config);
      JsonNode jsonSchema = jsonSchemaGenerator.generateJsonSchema(input.getClass());

      String jsonSchemaAsString = objectMapper.writeValueAsString(jsonSchema);
      checkNotNull(jsonSchemaAsString);

      return jsonSchemaAsString;
    } catch (Exception ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  protected void serializeSchemaVersion(OutputStream os, SchemaIdVersion schemaIdVersion) throws SerDesException {
    protocolHandler.handleSchemaVersionSerialization(os, schemaIdVersion);
  }

  protected void serializePayload(OutputStream os, Object input) throws SerDesException {
    protocolHandler.handlePayloadSerialization(os, input);
  }

}
