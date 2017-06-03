/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This is the default implementation of {@link AbstractAvroSnapshotDeserializer}.
 */
public class AvroSnapshotDeserializer extends AbstractAvroSnapshotDeserializer<InputStream> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSnapshotDeserializer.class);
    
    public AvroSnapshotDeserializer() {
        super();
    }
    
    public AvroSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, InputStream inputStream) throws SerDesException {
        // 8 bytes : schema metadata Id
        // 4 bytes : schema version
        ByteBuffer byteBuffer = ByteBuffer.allocate(12);
        try {
            inputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        long schemaMetadataId = byteBuffer.getLong();
        int schemaVersion = byteBuffer.getInt();

        return new SchemaIdVersion(schemaMetadataId, schemaVersion);
    }

    protected byte retrieveProtocolId(InputStream inputStream) throws SerDesException {
        // first byte is protocol version/id.
        // protocol format:
        // 1 byte  : protocol version
        byte protocolId;
        try {
            protocolId = (byte) inputStream.read();
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        if(protocolId == -1) {
            throw new SerDesException("End of stream reached while trying to read protocol id");
        }

        if (protocolId != AvroSnapshotSerializer.CURRENT_PROTOCOL_VERSION) {
            throw new SerDesException("Unknown protocol id [" + protocolId + "] received while deserializing the payload");
        }

        return protocolId;
    }

    protected Object doDeserialize(InputStream payloadInputStream,
                                   byte protocolId,
                                   SchemaMetadata schemaMetadata,
                                   Integer writerSchemaVersion,
                                   Integer readerSchemaVersion) throws SerDesException {
        if (protocolId != AvroSnapshotSerializer.CURRENT_PROTOCOL_VERSION) {
            throw new SerDesException("Unknown protocol id [" + protocolId + "] received while deserializing the payload");
        }

        return buildDeserializedObject(payloadInputStream, schemaMetadata, writerSchemaVersion, readerSchemaVersion);
    }

}
