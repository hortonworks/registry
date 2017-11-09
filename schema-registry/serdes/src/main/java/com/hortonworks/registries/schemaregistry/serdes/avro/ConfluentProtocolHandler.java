/*
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
 */
package com.hortonworks.registries.schemaregistry.serdes.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Confluent compatible protocol handler.
 */
public class ConfluentProtocolHandler extends AbstractAvroSerDesProtocolHandler {

    public static final Logger LOG = LoggerFactory.getLogger(ConfluentProtocolHandler.class);

    public ConfluentProtocolHandler() {
        super(SerDesProtocolHandlerRegistry.CONFLUENT_VERSION_PROTOCOL, new ConfluentAvroSerDesHandler());
    }

    @Override
    protected void doHandleSchemaVersionSerialization(OutputStream outputStream,
                                                      SchemaIdVersion schemaIdVersion) {
        Long versionId = schemaIdVersion.getSchemaVersionId();
        if (versionId > Integer.MAX_VALUE) {
            throw new AvroException("Unsupported versionId, max id=" + Integer.MAX_VALUE + " , but was id=" + versionId);
        } else {
            // 4 bytes
            try {
                outputStream.write(ByteBuffer.allocate(4)
                                             .putInt(versionId.intValue()).array());
            } catch (IOException e) {
                throw new AvroRetryableException(e);
            }
        }
    }

    @Override
    public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        try {
            inputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new AvroRetryableException(e);
        }

        int schemaVersionId = byteBuffer.getInt();
        return new SchemaIdVersion((long) schemaVersionId);
    }

}
