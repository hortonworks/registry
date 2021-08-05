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
import com.hortonworks.registries.schemaregistry.json.JsonUtils;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import org.everit.json.schema.Schema;
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

  public Object handlePayloadDeserialization(InputStream inputStream, Schema jsonSchema) throws SerDesException {
    checkNotNull(inputStream, "inputStream");
    checkNotNull(jsonSchema, "jsonSchema");

    try {
      JsonNode jsonNode = objectMapper.readTree(inputStream);
      JSONObject jsonObject = objectMapper.treeToValue(jsonNode, JSONObject.class);
      jsonSchema.validate(jsonObject);
      return jsonNode;
    } catch (IOException iex) {
      throw new RuntimeException("Could not read input.", iex);
    } catch (IllegalArgumentException iax) {
      throw new SerDesException("Error while validating the JSON input with the schema.", iax);
    }
  }

}
