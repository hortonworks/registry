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

import com.hortonworks.registries.schemaregistry.avro.AvroSchemaResolver;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class AvroSnapshotDeserializer extends AbstractSnapshotDeserializer<Object, Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSnapshotDeserializer.class);

    private AvroSchemaResolver avroSchemaResolver;

    @Override
    public void init(Map<String, ?> config) {
        super.init(config);
        avroSchemaResolver = new AvroSchemaResolver(key -> schemaRegistryClient.getSchemaVersionInfo(key));
    }

    @Override
    protected Schema getParsedSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
        return new Schema.Parser().parse(avroSchemaResolver.resolveSchema(schemaVersionKey));
    }

    protected SchemaIdVersion retrieveSchemaIdVersion(InputStream payloadInputStream) throws SerDesException {
        // it can be enhanced to have respective protocol handlers for different versions
        // first byte is protocol version/id.
        // protocol format:
        // 1 byte  : protocol version
        // 8 bytes : schema metadata Id
        // 4 bytes : schema version
        ByteBuffer byteBuffer = ByteBuffer.allocate(13);
        try {
            payloadInputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        byte protocolId = byteBuffer.get();
        if (protocolId != AvroSchemaProvider.CURRENT_PROTOCOL_VERSION) {
            throw new SerDesException("Unknown protocol id [" + protocolId + "] received while deserializing the payload");
        }
        long schemaMetadataId = byteBuffer.getLong();
        int schemaVersion = byteBuffer.getInt();

        return new SchemaIdVersion(schemaMetadataId, schemaVersion);
    }

    protected Object doDeserialize(InputStream payloadInputStream,
                                   SchemaMetadata schemaMetadata,
                                   Integer writerSchemaVersion,
                                   Integer readerSchemaVersion) throws SerDesException {
        Object deserializedObj;
        String schemaName = schemaMetadata.getName();
        SchemaVersionKey writerSchemaVersionKey = new SchemaVersionKey(schemaName, writerSchemaVersion);
        LOG.debug("SchemaKey: [{}] for the received payload", writerSchemaVersionKey);
        try {
            Schema writerSchema = getSchema(writerSchemaVersionKey);
            if (writerSchema == null) {
                throw new SerDesException("No schema exists with metadata-key: " + schemaMetadata + " and writerSchemaVersion: " + writerSchemaVersion);
            }

            Schema.Type writerSchemaType = writerSchema.getType();
            if (Schema.Type.BYTES.equals(writerSchemaType)) {
                // serializer writes byte array directly without going through avro encoder layers.
                deserializedObj = IOUtils.toByteArray(payloadInputStream);
            } else if (Schema.Type.STRING.equals(writerSchemaType)) {
                // generate UTF-8 string object from the received bytes.
                deserializedObj = new String(IOUtils.toByteArray(payloadInputStream), AvroUtils.UTF_8);
            } else {
                int recordType = payloadInputStream.read();
                LOG.debug("Received record type: [{}]", recordType);
                GenericDatumReader datumReader = null;
                Schema readerSchema = readerSchemaVersion != null
                        ? getSchema(new SchemaVersionKey(schemaName, readerSchemaVersion)) : null;

                if (recordType == AvroUtils.GENERIC_RECORD) {
                    datumReader = readerSchema != null ? new GenericDatumReader(writerSchema, readerSchema)
                            : new GenericDatumReader(writerSchema);
                } else {
                    datumReader = readerSchema != null ? new SpecificDatumReader(writerSchema, readerSchema)
                            : new SpecificDatumReader(writerSchema);
                }
                deserializedObj = datumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
            }
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        return deserializedObj;
    }

}
