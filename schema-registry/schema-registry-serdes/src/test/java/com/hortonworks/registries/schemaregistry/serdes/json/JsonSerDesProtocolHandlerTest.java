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

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerDesProtocolHandlerTest {
  
  private JsonSerDesHandler serDesHandlerMock = Mockito.mock(JsonSerDesHandler.class);
  private JsonSerDesProtocolHandler underTest = new JsonSerDesProtocolHandler(serDesHandlerMock);
  
  @BeforeEach
  void setup() {
    Mockito.reset(serDesHandlerMock);
  }

  @Test
  void handleSchemaVersionSerializationTest() throws Exception {
    //given
    SchemaIdVersion versionId = new SchemaIdVersion(1L, 2, 3L);
    int schemaVersionId = 3;
    OutputStream outputStream = new ByteArrayOutputStream();
    OutputStream expected = new ByteArrayOutputStream();
    expected.write(0x4);
    expected.write(ByteBuffer.allocate(4).putInt(schemaVersionId).array(), 0, 4);
    
    //when
    underTest.handleSchemaVersionSerialization(outputStream, versionId);
    
    //then
    assertEquals(expected.toString(), outputStream.toString());
    
    expected.flush();
    expected.close();
    outputStream.flush();
    outputStream.close();
    
  }
  
  @Test
  void handleSchemaVersionDeserialization() throws Exception {
    //given
    SchemaIdVersion expected = new SchemaIdVersion(5L);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(ByteBuffer.allocate(4).putInt(5).array());
    InputStream inputStream = new ByteArrayInputStream(baos.toByteArray());
    
    //when
    SchemaIdVersion actual = underTest.handleSchemaVersionDeserialization(inputStream);
    
    //then
    assertEquals(expected, actual);
    baos.close();
    inputStream.close();

  }

  @Test
  void getProtocolId() {
    int expected = 4;
    assertEquals(expected, underTest.getProtocolId().byteValue());
  }
}