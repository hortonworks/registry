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

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageAndMetadata;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonException;

import java.io.ByteArrayInputStream;

public class MessageAndMetadataJsonDeserializer extends AbstractJsonSnapshotDeserializer<MessageAndMetadata> {

  private final JsonSerDesProtocolHandler protocolHandler = new JsonSerDesProtocolHandler();

  public MessageAndMetadataJsonDeserializer() { }

  public MessageAndMetadataJsonDeserializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }
  
  @Override
  public Object deserialize(MessageAndMetadata context, Integer readerSchemaVersion) throws SerDesException {
    byte protocolId = retrieveProtocolId(context);

    if (protocolId != SerDesProtocolHandlerRegistry.JSON_PROTOCOL) {
      throw new SerDesException("Unsupported protocol: " + protocolId);
    }

    SchemaIdVersion schemaIdVersion = retrieveSchemaIdVersion(protocolId, context);
    try {
      SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaIdVersion);
      return doDeserialize(
              context,
              protocolId,
              schemaVersionInfo.getName(),
              schemaVersionInfo.getVersion(),
              readerSchemaVersion);
    } catch (Exception e) {
      throw new RegistryException(e);
    }
  }

  @Override
  protected Object doDeserialize(MessageAndMetadata context,
                                 byte protocolId,
                                 String schemaName,
                                 Integer writerSchemaVersion,
                                 Integer readerSchemaVersion) throws SerDesException {

    if (protocolId != SerDesProtocolHandlerRegistry.JSON_PROTOCOL) {
      throw new SerDesException("Illegal protocol: " + protocolId);
    }

    return buildDeserializedObject(protocolId, new ByteArrayInputStream(context.payload()), schemaName,
        writerSchemaVersion);
  }

  @Override
  protected byte retrieveProtocolId(MessageAndMetadata context) throws SerDesException, JsonException {
    final byte[] metadata = context.metadata();
    byte protocolId = metadata[0];
    checkProtocolHandlerExists(protocolId);
    return protocolId;
  }

  private void checkProtocolHandlerExists(byte protocolId) throws JsonException {
    if (SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId) == null) {
      throw new JsonException("Unknown protocol id [" + protocolId + "] received while de-serializing the header");
    }
  }

  @Override
  protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, MessageAndMetadata context) throws SerDesException {
    final byte[] metadata = context.metadata();
    return protocolHandler
        .handleSchemaVersionDeserialization(new ByteArrayInputStream(metadata, 1, metadata.length));
  }
}
