/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MessageAndMetadataAvroSerializer extends AbstractAvroSnapshotSerializer<MessageAndMetadata> {

    public MessageAndMetadataAvroSerializer() {
    }

    public MessageAndMetadataAvroSerializer(ISchemaRegistryClient schemaRegistryClient) {
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
            throw new AvroRetryableException(ex);
        }
    }

}
