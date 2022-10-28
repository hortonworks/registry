 /*
 * Copyright 2017-2022 Cloudera, Inc.
 *
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
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.serde.AbstractSerDes;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;

public class MessageAndMetadataAvroDeserializer extends AbstractAvroSnapshotDeserializer<MessageAndMetadata> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSerDes.class);

    public MessageAndMetadataAvroDeserializer() {
    }

    public MessageAndMetadataAvroDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    @Override
    public Object deserialize(MessageAndMetadata context,
                              Integer readerSchemaVersion) throws SerDesException {
        byte protocolId = retrieveProtocolId(context);
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
        return buildDeserializedObject(protocolId, new ByteArrayInputStream(context.payload()), schemaName,
                writerSchemaVersion, readerSchemaVersion);
    }

    @Override
    protected byte retrieveProtocolId(MessageAndMetadata context) throws SerDesException {
        final byte[] metadata = context.metadata();
        byte protocolId = metadata[0];
        checkProtocolHandlerExists(protocolId);
        return protocolId;
    }

    private void checkProtocolHandlerExists(byte protocolId) {
        if (SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId) == null) {
            throw new AvroException("Unknown protocol id [" + protocolId + "] received while de-serializing the header");
        }
    }

    @Override
    protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, MessageAndMetadata context) throws SerDesException {
        final byte[] metadata = context.metadata();
        return SerDesProtocolHandlerRegistry.get()
                .getSerDesProtocolHandler(protocolId)
                .handleSchemaVersionDeserialization(new ByteArrayInputStream(metadata, 1, metadata.length));
    }

}
