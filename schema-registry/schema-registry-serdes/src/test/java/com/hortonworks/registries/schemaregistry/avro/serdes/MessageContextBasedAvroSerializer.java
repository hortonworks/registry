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
package com.hortonworks.registries.schemaregistry.avro.serdes;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MessageContextBasedAvroSerializer extends AbstractAvroSnapshotSerializer<MessageContext> {

    @Override
    protected MessageContext doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
        Map<String, Object> headers = new HashMap<>();

        headers.put("protocol.id", getProtocolId());
        headers.put("schema.metadata.id", schemaIdVersion.getSchemaMetadataId());
        headers.put("schema.version", schemaIdVersion.getVersion());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(baos)) {
            serializePayload(bufferedOutputStream, input);
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        ByteArrayInputStream payload = new ByteArrayInputStream(baos.toByteArray());

        return new MessageContext(headers, payload);
    }
}
