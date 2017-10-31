/**
 * Copyright 2016 Hortonworks.
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
 **/
package com.hortonworks.registries.schemaregistry.serdes.avro;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;


/**
 * This is the default implementation of {@link AbstractAvroSnapshotDeserializer}.
 */
public class AvroSnapshotDeserializer extends AbstractAvroSnapshotDeserializer<InputStream> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSnapshotDeserializer.class);

    public AvroSnapshotDeserializer() {
    }

    public AvroSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, InputStream inputStream)
            throws SerDesException {
        return SerDesProtocolHandlerRegistry.get()
                                            .getSerDesProtocolHandler(protocolId)
                                            .handleSchemaVersionDeserialization(inputStream);
    }

    protected byte retrieveProtocolId(InputStream inputStream) throws SerDesException {
        // first byte is protocol version/id.
        // protocol format:
        // 1 byte  : protocol version
        byte protocolId;
        try {
            protocolId = (byte) inputStream.read();
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        }

        if (protocolId == -1) {
            throw new AvroException("End of stream reached while trying to read protocol id");
        }

        checkProtocolHandlerExists(protocolId);

        return protocolId;
    }

    private void checkProtocolHandlerExists(byte protocolId) {
        if (SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId) == null) {
            throw new AvroException("Unknown protocol id [" + protocolId + "] received while deserializing the payload");
        }
    }

    protected Object doDeserialize(InputStream payloadInputStream,
                                   byte protocolId,
                                   SchemaMetadata schemaMetadata,
                                   Integer writerSchemaVersion,
                                   Integer readerSchemaVersion) throws SerDesException {

        return buildDeserializedObject(protocolId, payloadInputStream, schemaMetadata, writerSchemaVersion, readerSchemaVersion);
    }
    
    
}
