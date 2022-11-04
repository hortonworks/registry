/*
 * Copyright 2016-2022 Cloudera, Inc.
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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.json.JsonSchemaResolver;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonException;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonRetryableException;
import org.everit.json.schema.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

class JsonSnapshotDeserializerTest {
  private final SchemaVersionRetriever schemaVersionRetrieverMock = Mockito.mock(SchemaVersionRetriever.class);
  private final JsonSchemaResolver jsonSchemaResolver = new JsonSchemaResolver(schemaVersionRetrieverMock);
  private final JsonSerDesHandler jsonSerDesHandlerMock = Mockito.mock(JsonSerDesHandler.class);
  private final JsonSnapshotDeserializer underTest = new JsonSnapshotDeserializer(jsonSchemaResolver, jsonSerDesHandlerMock);
  private final InputStream inputStream = Mockito.mock(InputStream.class);

  @Test
  void doDeserializePerson() throws Exception {
    //given
    String json = "{\n" +
            "  \"$id\": \"https://example.com/person.schema.json\",\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema\",\n" +
            "  \"title\": \"Person\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"firstName\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"description\": \"The person's first name.\"\n" +
            "    },\n" +
            "    \"lastName\": {\n" +
            "      \"type\": \"string\",\n" +
            "      \"description\": \"The person's last name.\"\n" +
            "    },\n" +
            "    \"age\": {\n" +
            "      \"description\": \"Age in years which must be equal to or greater than zero.\",\n" +
            "      \"type\": \"integer\",\n" +
            "      \"minimum\": 0\n" +
            "    }\n" +
            "  }\n" +
            "}";
    InputStream is = new ByteArrayInputStream(json.getBytes());
    SchemaMetadata metadata = new SchemaMetadata.Builder("name").type("json").build();
    SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(1L, "name", 1, json, System.currentTimeMillis(), "desc");
    Mockito.when(schemaVersionRetrieverMock.retrieveSchemaVersion(any(SchemaVersionKey.class))).thenReturn(schemaVersionInfo);
    ObjectMapper objectMapper = new ObjectMapper();
    Person person = new Person("[first]", "[last]", 4);
    JsonNode jsonNodeDeserialized = objectMapper.readTree(objectMapper.writeValueAsString(person));
    Mockito.when(jsonSerDesHandlerMock.handlePayloadDeserialization(any(InputStream.class), any(Schema.class))).thenReturn(jsonNodeDeserialized);
    
    //when
    Object deserialized = underTest.doDeserialize(is, Byte.parseByte("4"), metadata.getName(), 1, 0);
    
    //then
    ObjectNode expected = JsonNodeFactory.instance.objectNode()
            .put("firstName", person.firstName)
            .put("lastName", person.lastName)
            .put("age", person.age);
    assertEquals(expected, deserialized);
  }

  @Test
  void retrieveProtocolIdThrowIOException() throws Exception {
    //given
    Mockito.doThrow(IOException.class).when(inputStream).read();
    
    //when
    assertThrows(JsonRetryableException.class, () ->
        underTest.retrieveProtocolId(inputStream));
    
  }

  @Test
  void retrieveProtocolIdThrowJsonException() throws Exception {
    //given
    Mockito.when(inputStream.read()).thenReturn(-1);

    //when
    assertThrows(JsonException.class, () ->
        underTest.retrieveProtocolId(inputStream));
  }

  @Test
  void retrieveProtocolIdTest() throws Exception {
    //given
    Mockito.when(inputStream.read()).thenReturn(4);

    //when
    byte actual = underTest.retrieveProtocolId(inputStream);
    
    //then
    assertEquals(4, actual);

  }

  @Test
  void retrieveSchemaIdVersion() {
    //given
    SchemaIdVersion expected = new SchemaIdVersion(0L);
    
    //when
    SchemaIdVersion actualSchemaIdVersion = underTest.retrieveSchemaIdVersion(Byte.parseByte("4"), inputStream);

    //then
    assertEquals(expected, actualSchemaIdVersion);
  }
  
  @Test
  void retrieveSchemaIdVersionThrowSerDesException() throws Exception {
    //given
    Mockito.doThrow(IOException.class).when(inputStream).read(any());

    //when
    assertThrows(SerDesException.class, () ->
        underTest.retrieveSchemaIdVersion(Byte.parseByte("4"), inputStream));
    
  }

  @Test
  void getParsedSchemaTest() throws Exception {
    //given
    SchemaVersionKey versionKey = new SchemaVersionKey("schema", 2);
    String expectedSchema = "{\"type\":\"object\",\"additionalProperties\":false,\"title\":\"Schema Version Key\",\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"properties\":{\"schemaName\":{\"oneOf\":[{\"type\":\"null\"," +
      "\"title\":\"Not included\"},{\"type\":\"string\"}]},\"version\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"integer\"}]}}}";
    SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(1L, "schema", 2, expectedSchema, System.currentTimeMillis(), "desc");

    Mockito.when(schemaVersionRetrieverMock.retrieveSchemaVersion(versionKey)).thenReturn(schemaVersionInfo);
    
    //when
    Schema actual = underTest.getParsedSchema(versionKey);

    //then
    assertEquals(expectedSchema, actual.toString());
  }
  
  private static final class Person {
    public String firstName;
    public String lastName;
    public int age;

    public Person(String firstName, String lastName, int age) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.age = age;
    }
  }
}