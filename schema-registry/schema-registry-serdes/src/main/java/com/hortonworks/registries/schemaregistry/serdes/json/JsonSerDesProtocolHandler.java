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
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.SerDesProtocolHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonRetryableException;
import org.everit.json.schema.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class JsonSerDesProtocolHandler implements SerDesProtocolHandler {

  public static final String JSON_SCHEMA = "json.schema";

  private final Byte protocolId = SerDesProtocolHandlerRegistry.JSON_PROTOCOL;
  private final JsonSerDesHandler jsonSerDesHandler;

  public JsonSerDesProtocolHandler() {
    this(new JsonSerDesHandler());
  }

  public JsonSerDesProtocolHandler(JsonSerDesHandler jsonSerDesHandler) {
    this.jsonSerDesHandler = jsonSerDesHandler;
  }

  @Override
  public void handleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) {
    try {
      outputStream.write(new byte[] { protocolId });
      outputStream.write(ByteBuffer.allocate(4).putInt(schemaIdVersion.getSchemaVersionId().intValue()).array());
    } catch (IOException e) {
      throw new JsonRetryableException(e);
    }
  }

  @Override
  public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) throws SerDesException {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    try {
      inputStream.read(byteBuffer.array());
    } catch (IOException e) {
      throw new JsonRetryableException(e);
    }

    int schemaVersionId = byteBuffer.getInt();
    return new SchemaIdVersion((long) schemaVersionId);
  }

  @Override
  public Byte getProtocolId() {
    return protocolId;
  }

  @Override
  public void handlePayloadSerialization(OutputStream outputStream, Object input) {
    jsonSerDesHandler.handlePayloadSerialization(outputStream, input);
  }

  @Override
  public Object handlePayloadDeserialization(InputStream payloadInputStream, Map<String, Object> context) throws SerDesException {
    Schema jsonSchema = (Schema) context.get(JSON_SCHEMA);
    return jsonSerDesHandler.handlePayloadDeserialization(payloadInputStream, jsonSchema);
  }
}
