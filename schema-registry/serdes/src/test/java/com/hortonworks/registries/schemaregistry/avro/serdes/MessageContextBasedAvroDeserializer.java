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
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;

/**
 *
 */
public class MessageContextBasedAvroDeserializer extends AbstractAvroSnapshotDeserializer<MessageContext> {

    @Override
    protected Object doDeserialize(MessageContext input,
                                   byte protocolId,
                                   SchemaMetadata schemaMetadata,
                                   Integer writerSchemaVersion,
                                   Integer readerSchemaVersion) throws SerDesException {
        return buildDeserializedObject(protocolId,
                input.payloadEntity,
                schemaMetadata,
                writerSchemaVersion, readerSchemaVersion);
    }

    @Override
    protected byte retrieveProtocolId(MessageContext input) throws SerDesException {
        return (byte) input.headers.get("protocol.id");
    }

    @Override
    protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, MessageContext input) throws SerDesException {
        Long id = (Long) input.headers.get("schema.metadata.id");
        Integer version = (Integer) input.headers.get("schema.version");
        return new SchemaIdVersion(id, version);
    }
}
