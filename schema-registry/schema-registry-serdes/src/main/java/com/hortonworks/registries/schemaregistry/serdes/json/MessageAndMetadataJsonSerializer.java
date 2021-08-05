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
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.MessageAndMetadata;
import com.hortonworks.registries.schemaregistry.serdes.json.exceptions.JsonRetryableException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MessageAndMetadataJsonSerializer extends AbstractJsonSnapshotSerializer<MessageAndMetadata> {

  public MessageAndMetadataJsonSerializer() { }

  public MessageAndMetadataJsonSerializer(ISchemaRegistryClient schemaRegistryClient) {
    super(schemaRegistryClient);
  }

  @Override
  protected MessageAndMetadata doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      serializeSchemaVersion(baos, schemaIdVersion);
      byte[] serializedSchemaVersion = baos.toByteArray();
      baos.reset();

      serializePayload(baos, input);
      byte[] payload = baos.toByteArray();
      return new MessageAndMetadata(serializedSchemaVersion, payload);
    } catch (IOException ex) {
      throw new JsonRetryableException(ex);
    }
  }
}
