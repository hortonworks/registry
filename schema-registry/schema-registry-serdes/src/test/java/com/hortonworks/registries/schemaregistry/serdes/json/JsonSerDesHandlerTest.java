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
package com.hortonworks.registries.schemaregistry.serdes.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.json.JsonUtils;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonSerDesHandlerTest {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDesHandlerTest.class);

  private final JsonSerDesHandler handler = new JsonSerDesHandler();
  private final ObjectMapper objectMapper = JsonUtils.getObjectMapper();

  @Test
  void handlePayloadSerializationSchemaMetadata() {
    //given
    OutputStream outputStream = new ByteArrayOutputStream();
    SchemaMetadata input = new SchemaMetadata.Builder("serdes").type("json").build();
    String expected = "{\"type\":\"json\",\"schemaGroup\":null,\"name\":\"serdes\",\"description\":null,\"compatibility\":\"BACKWARD\",\"validationLevel\":\"ALL\",\"evolve\":true}";
    
    //when
    handler.handlePayloadSerialization(outputStream, input);
    
    //then
    assertEquals(expected, outputStream.toString());
  }

  @Test
  void handlePayloadSerializationThrowException() {
    //given
    OutputStream outputStream = new ByteArrayOutputStream();
    SchemaMetadata input = new SchemaMetadata.Builder("serdes").type("json").build();

    //when
    handler.handlePayloadSerialization(outputStream, input);

    //then
    
  }

  @Test
  void handlePayloadDeserialization() throws Exception {
    //given
    JsonNode schemaTreeNode = objectMapper.readTree(getClass().getResourceAsStream("/SchemaVersionKey.json"));
    JSONObject jsonObject = objectMapper.treeToValue(schemaTreeNode, JSONObject.class);
    Schema schema = SchemaLoader.load(jsonObject);
    LOG.info("Loaded JSON schema: {}", schemaTreeNode);

    SchemaVersionKey versionKey = new SchemaVersionKey("deserialize", 1);
    byte[] bytes = objectMapper.writeValueAsString(versionKey).getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    
    //when
    Object deserialization = handler.handlePayloadDeserialization(byteArrayInputStream, schema);

    //then
    SchemaVersionKey svk = objectMapper.convertValue(deserialization, SchemaVersionKey.class);
    assertEquals(svk, versionKey);
  }

  @Test
  void handleInvalidPayloadDeserialization() throws Exception {
    //given
    JsonNode schemaTreeNode = objectMapper.readTree(getClass().getResourceAsStream("/SchemaVersionKey.json"));
    JSONObject jsonObject = objectMapper.treeToValue(schemaTreeNode, JSONObject.class);
    Schema schema = SchemaLoader.load(jsonObject);
    LOG.info("Loaded JSON schema: {}", schemaTreeNode);

    InvalidPayload invalidPayload = new InvalidPayload(1999, "schema", 77.22);
    byte[] bytes = objectMapper.writeValueAsString(invalidPayload).getBytes(StandardCharsets.UTF_8);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);

    //when
    assertThrows(Exception.class, () -> handler.handlePayloadDeserialization(byteArrayInputStream, schema));
  }

}

class InvalidPayload {

  private int year;
  private String second;
  private double third;

  public InvalidPayload() { }

  public InvalidPayload(int year, String second, double third) {
    this.year = year;
    this.second = second;
    this.third = third;
  }

  public int getYear() {
    return year;
  }

  public void setYear(int year) {
    this.year = year;
  }

  public String getSecond() {
    return second;
  }

  public void setSecond(String second) {
    this.second = second;
  }

  public double getThird() {
    return third;
  }

  public void setThird(double third) {
    this.third = third;
  }
}