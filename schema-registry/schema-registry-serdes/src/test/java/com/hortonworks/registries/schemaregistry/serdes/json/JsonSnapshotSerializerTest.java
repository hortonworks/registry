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

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonSnapshotSerializerTest {
  private AbstractJsonSnapshotSerializer<byte[]> underTest = new JsonSnapshotSerializer();
  
  @Test
  void getSchemaTextTest() {
    //given
    SchemaMetadata metadata = new SchemaMetadata.Builder("name").description("description").schemaGroup("kafka").type("json").build();
    String expectedSchema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"title\":\"Schema Metadata\",\"type\":\"object\",\"additionalProperties\":false,\"properties\":{\"type\":{\"oneOf\":[{\"type\":\"null\"," +
        "\"title\":\"Not included\"},{\"type\":\"string\"}]},\"schemaGroup\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},\"name\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"}," +
        "{\"type\":\"string\"}]},\"description\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"}]},\"compatibility\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\"," +
        "\"enum\":[\"NONE\",\"BACKWARD\",\"FORWARD\",\"BOTH\"]}]},\"validationLevel\":{\"oneOf\":[{\"type\":\"null\",\"title\":\"Not included\"},{\"type\":\"string\",\"enum\":[\"LATEST\",\"ALL\"]}]},\"evolve\":{\"type\":\"boolean\"}}," +
        "\"required\":[\"evolve\"]}";
    //when
    String actual = underTest.getSchemaText(metadata);

    //then
    assertEquals(expectedSchema, actual);
  }

}