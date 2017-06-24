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
import com.hortonworks.registries.schemaregistry.serde.SerDesException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class SchemaVersionIdAsLongProtocolHandler extends AbstractAvroSerDesProtocolHandler {

    public SchemaVersionIdAsLongProtocolHandler() {
        super(SerDesProtocolHandlerRegistry.VERSION_ID_AS_LONG_PROTOCOL);
    }

    @Override
    public void doHandleSchemaVersionSerialization(OutputStream outputStream,
                                                   SchemaIdVersion schemaIdVersion) throws IOException {
        Long versionId = schemaIdVersion.getSchemaVersionId();
        outputStream.write(ByteBuffer.allocate(8)
                                     .putLong(versionId).array());
    }

    @Override
    public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        try {
            inputStream.read(byteBuffer.array());
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        return new SchemaIdVersion(byteBuffer.getLong());
    }

    public Byte getProtocolId() {
        return protocolId;
    }
}
