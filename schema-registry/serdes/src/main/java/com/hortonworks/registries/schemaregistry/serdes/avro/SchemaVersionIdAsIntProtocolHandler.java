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

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class SchemaVersionIdAsIntProtocolHandler extends AbstractAvroSerDesProtocolHandler {
    public static final Logger LOG = LoggerFactory.getLogger(SchemaVersionIdAsIntProtocolHandler.class);

    private final SchemaVersionIdAsLongProtocolHandler delegate;

    public SchemaVersionIdAsIntProtocolHandler() {
        super(SerDesProtocolHandlerRegistry.VERSION_ID_AS_INT_PROTOCOL, new DefaultAvroSerDesHandler());
        delegate = new SchemaVersionIdAsLongProtocolHandler();
    }

    @Override
    public void handleSchemaVersionSerialization(OutputStream outputStream,
                                                 SchemaIdVersion schemaIdVersion) {
        Long versionId = schemaIdVersion.getSchemaVersionId();
        if (versionId > Integer.MAX_VALUE) {
            // if it is more than int max, fallback to SchemaVersionIdAsLongProtocolHandler
            LOG.debug("Upgraded to " + delegate + " as versionId is more than max integer");
            delegate.handleSchemaVersionSerialization(outputStream, schemaIdVersion);
        } else {
            // 4 bytes
            try {
                outputStream.write(new byte[]{protocolId});
                outputStream.write(ByteBuffer.allocate(4)
                                             .putInt(versionId.intValue()).array());
            } catch (IOException e) {
                throw new AvroException(e);
            }
        }
    }

    @Override
    protected void doHandleSchemaVersionSerialization(OutputStream outputStream, SchemaIdVersion schemaIdVersion) throws IOException {
        // ignore this as this would never be invoked.
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

    public Byte getProtocolId() {
        return protocolId;
    }
}
