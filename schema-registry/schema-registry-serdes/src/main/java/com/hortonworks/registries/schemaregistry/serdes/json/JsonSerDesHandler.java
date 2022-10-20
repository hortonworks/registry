/**
 * Copyright 2016-2022 Cloudera, Inc.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.hortonworks.registries.schemaregistry.json.JsonUtils;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import org.everit.json.schema.Schema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonSerDesHandler {

  private final ObjectMapper objectMapper = JsonUtils.getObjectMapper();

  public void handlePayloadSerialization(OutputStream outputStream, Object input) {
    checkNotNull(outputStream, "outputStream");
    checkNotNull(input, "input");
    try {
      outputStream.write(objectMapper.writeValueAsString(input).getBytes(StandardCharsets.UTF_8));
    } catch (IOException iex) {
      throw new RuntimeException("Could not generate schema for input: " + input, iex);
    }
  }

  public JsonNode handlePayloadDeserialization(InputStream inputStream, Schema jsonSchema) throws SerDesException {
    checkNotNull(inputStream, "inputStream");
    checkNotNull(jsonSchema, "jsonSchema");

    try {
      JsonNode jsonNode = objectMapper.readTree(inputStream);
      validate(jsonSchema, jsonNode);
      return jsonNode;
    } catch (IOException iex) {
      throw new RuntimeException("Could not read input.", iex);
    } catch (IllegalArgumentException iax) {
      throw new SerDesException("Error while validating the JSON input with the schema.", iax);
    }
  }

  private void validate(Schema jsonSchema, JsonNode jsonNode) throws JsonProcessingException {
    switch (jsonNode.getNodeType()) {
      case NULL:
        jsonSchema.validate(JSONObject.NULL);
        return;
      case ARRAY:
        jsonSchema.validate(objectMapper.treeToValue(jsonNode, JSONArray.class));
        return;
      case BINARY:
        // should not encounter this since reading from the input stream will just produce a STRING text node
        BinaryNode binaryNode = (BinaryNode) jsonNode;
        jsonSchema.validate(binaryNode.asText());
        return;
      case NUMBER:
        jsonSchema.validate(jsonNode.numberValue());
        return;
      case BOOLEAN:
        jsonSchema.validate(jsonNode.booleanValue());
        return;
      case STRING:
        jsonSchema.validate(jsonNode.textValue());
        return;
      case OBJECT:
      default:
        jsonSchema.validate(objectMapper.treeToValue(jsonNode, JSONObject.class));
        return;
    }
  }
}
