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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.json.JsonUtils;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonSerDesHandlerTest {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerDesHandlerTest.class);

  private final JsonSerDesHandler handler = new JsonSerDesHandler();
  private final ObjectMapper objectMapper = JsonUtils.getObjectMapper();

  @Nested
  class Serialization {

    @Test
    void shouldSerializeObject() throws IOException {
      //given
      OutputStream outputStream = new ByteArrayOutputStream();

      //when
      SchemaMetadata input = new SchemaMetadata.Builder("serdes").type("json").build();
      handler.handlePayloadSerialization(outputStream, input);

      //then
      String expected = "{\"type\":\"json\",\"schemaGroup\":null,\"name\":\"serdes\",\"description\":null,\"compatibility\":\"BACKWARD\",\"validationLevel\":\"ALL\",\"evolve\":true}";
      JsonNode actualTree = objectMapper.readTree(outputStream.toString());
      JsonNode expectedTree = objectMapper.readTree(expected);

      assertEquals(expectedTree, actualTree);
    }

    @Test
    void serializeString() {
      String stringValue = "[a string]";

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      handler.handlePayloadSerialization(outputStream, stringValue);

      String serializedAsString = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      assertEquals(TextNode.valueOf(stringValue).toString(), serializedAsString);
    }
  }

  @Nested
  class Deserialization {

    @Test
    void shouldDeserializeObject() throws Exception {
      //given
      Schema schema = getSchema("/SchemaVersionKey.json");
      SchemaVersionKey versionKey = new SchemaVersionKey("deserialize", 1);

      //when
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream(versionKey), schema);

      //then
      SchemaVersionKey svk = objectMapper.convertValue(deserialized, SchemaVersionKey.class);
      assertEquals(svk, versionKey);
    }

    @Test
    void shouldDeserializeString() throws Exception {
      //given
      Schema schema = getSchema(toInputStream(StringSchema.builder().build().toString()));
      String stringValue = "[a string]";

      //when
      String stringValueAsJon = "\"" + stringValue + "\"";
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream(stringValueAsJon), schema);

      //then
      assertEquals(TextNode.valueOf(stringValue), deserialized);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldDeserializeBoolean(Boolean boolValue) {
      //given
      Schema schema = BooleanSchema.builder().build();

      //when
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream(boolValue.toString()), schema);

      //then
      assertEquals(BooleanNode.valueOf(boolValue), deserialized);
    }

    @Test
    void shouldDeserializeInt() {
      //given
      int value = 1;
      Schema schema = NumberSchema.builder().build();

      //when
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream(String.valueOf(value)), schema);

      //then
      assertEquals(IntNode.valueOf(value), deserialized);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.999", "1.2"})
    void shouldDeserializeDecimals(String valueAsString) {
      //given
      Schema schema = NumberSchema.builder().build();

      //when
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream(valueAsString), schema);

      //then
      assertEquals(DecimalNode.valueOf(new BigDecimal(valueAsString)), deserialized);
    }

    @Test
    void shouldDeserializeNull() {
      //given
      Schema schema = NullSchema.builder().build();

      //when
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream("null"), schema);

      //then
      assertEquals(NullNode.getInstance(), deserialized);
    }

    @Test
    void shouldDeserializeArrays() {
      //given
      Schema schema = ArraySchema.builder()
              .addItemSchema(NumberSchema.builder().build())
              .build();

      //when
      JsonNode deserialized = handler.handlePayloadDeserialization(toInputStream("[1, 2, 3]"), schema);

      //then
      ArrayNode expectedNode = JsonNodeFactory.instance.arrayNode()
              .add(1)
              .add(2)
              .add(3);
      assertEquals(expectedNode, deserialized);
    }

    @Test
    void shouldThrowWhenObjectDoesNotMatchSchema() throws Exception {
      //given
      Schema schema = getSchema("/SchemaVersionKey.json");

      InvalidPayload invalidPayload = new InvalidPayload("someValue");

      //when
      assertThrows(Exception.class,
              () -> handler.handlePayloadDeserialization(toInputStream(invalidPayload), schema)
      );
    }
  }

  private InputStream toInputStream(String string) {
    return new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream toInputStream(Object jsonObject) throws JsonProcessingException {
    return toInputStream(objectMapper.writeValueAsString(jsonObject));
  }

  private Schema getSchema(String schemaFileName) throws IOException {
    return getSchema(getClass().getResourceAsStream(schemaFileName));
  }

  private Schema getSchema(InputStream schemaStream) throws IOException {
    JsonNode schemaTreeNode = objectMapper.readTree(schemaStream);
    JSONObject jsonObject = objectMapper.treeToValue(schemaTreeNode, JSONObject.class);
    Schema schema = SchemaLoader.load(jsonObject);
    LOG.info("Loaded JSON schema: {}", schemaTreeNode);
    return schema;
  }

  private static class InvalidPayload {

    public String field;

    public InvalidPayload(String field) {
      this.field = field;
    }
  }
}