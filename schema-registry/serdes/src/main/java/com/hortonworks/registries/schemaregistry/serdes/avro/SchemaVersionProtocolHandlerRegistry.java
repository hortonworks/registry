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
import com.hortonworks.registries.schemaregistry.serdes.AbstractSchemaVersionProtocolHandler;
import com.hortonworks.registries.schemaregistry.serdes.SchemaVersionProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registered ser/des protocol handlers which can be used in serializers/deserializers.
 */
public final class SchemaVersionProtocolHandlerRegistry {
    public static final byte METADATA_ID_VERSION_PROTOCOL = 0x1;
    public static final byte VERSION_ID_AS_LONG_PROTOCOL = 0x2;
    public static final byte VERSION_ID_AS_INT_PROTOCOL = 0x3;
    public static final byte CURRENT_PROTOCOL = VERSION_ID_AS_INT_PROTOCOL;

    private static final SchemaVersionProtocolHandlerRegistry instance = new SchemaVersionProtocolHandlerRegistry();

    private final Map<Byte, SchemaVersionProtocolHandler> protocolWithHandlers = new ConcurrentHashMap<>();

    public static SchemaVersionProtocolHandlerRegistry get() {
        return instance;
    }

    public SchemaVersionProtocolHandler getSerDesProtocolHandler(Byte protocolId) {
        return protocolWithHandlers.get(protocolId);
    }

    public Map<Byte, SchemaVersionProtocolHandler> getRegisteredSerDesProtocolHandlers() {
        return Collections.unmodifiableMap(protocolWithHandlers);
    }

    private SchemaVersionProtocolHandlerRegistry() {
        List<SchemaVersionProtocolHandler> inbuiltHandlers = Arrays.asList(new SchemaMetadataIdProtocolHandler(),
                new SchemaVersionIdAsIntProtocolHandler(), new SchemaVersionIdAsLongProtocolHandler());
        for (SchemaVersionProtocolHandler inbuiltHandler : inbuiltHandlers) {
            registerSerDesProtocolHandler(inbuiltHandler);
        }
    }

    /**
     * Register the given protocol handler with its protocol id.
     *
     * @param schemaVersionProtocolHandler
     */
    public void registerSerDesProtocolHandler(SchemaVersionProtocolHandler schemaVersionProtocolHandler) {
        SchemaVersionProtocolHandler existingHandler = protocolWithHandlers.putIfAbsent(schemaVersionProtocolHandler.getProtocolId(), schemaVersionProtocolHandler);
        if (existingHandler != null) {
            throw new IllegalArgumentException("SchemaVersionProtocolHandler is already registered with the given protocol id: " + schemaVersionProtocolHandler.getProtocolId());
        }
    }

    public static class SchemaMetadataIdProtocolHandler extends AbstractSchemaVersionProtocolHandler {

        public SchemaMetadataIdProtocolHandler() {
            super(METADATA_ID_VERSION_PROTOCOL);
        }

        @Override
        protected void doHandleSchemaVersionSerialization(OutputStream outputStream,
                                                          SchemaIdVersion schemaIdVersion) throws IOException {
            // 8 bytes : schema metadata Id
            // 4 bytes : schema version
            outputStream.write(ByteBuffer.allocate(12)
                    .putLong(schemaIdVersion.getSchemaMetadataId())
                    .putInt(schemaIdVersion.getVersion()).array());
        }

        @Override
        public SchemaIdVersion handleSchemaVersionDeserialization(InputStream inputStream) {
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

    }

    public static class SchemaVersionIdAsLongProtocolHandler extends AbstractSchemaVersionProtocolHandler {

        public SchemaVersionIdAsLongProtocolHandler() {
            super(VERSION_ID_AS_LONG_PROTOCOL);
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

    public static class SchemaVersionIdAsIntProtocolHandler extends AbstractSchemaVersionProtocolHandler {
        public static final Logger LOG = LoggerFactory.getLogger(SchemaVersionIdAsIntProtocolHandler.class);

        private final SchemaVersionIdAsLongProtocolHandler delegate;

        public SchemaVersionIdAsIntProtocolHandler() {
            super(VERSION_ID_AS_INT_PROTOCOL);
            delegate = new SchemaVersionIdAsLongProtocolHandler();
        }

        @Override
        public void handleSchemaVersionSerialization(OutputStream outputStream,
                                                     SchemaIdVersion schemaIdVersion) throws IOException {
            Long versionId = schemaIdVersion.getSchemaVersionId();
            if (versionId > Integer.MAX_VALUE) {
                // if it is more than int max, fallback to SchemaVersionIdAsLongProtocolHandler
                LOG.debug("Upgraded to " + delegate + " as versionId is more than max integer");
                delegate.handleSchemaVersionSerialization(outputStream, schemaIdVersion);
            } else {
                // 4 bytes
                outputStream.write(new byte[]{protocolId});
                outputStream.write(ByteBuffer.allocate(4)
                        .putInt(versionId.intValue()).array());
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
                throw new SerDesException(e);
            }

            int schemaVersionId = byteBuffer.getInt();
            return new SchemaIdVersion((long) schemaVersionId);
        }

        public Byte getProtocolId() {
            return protocolId;
        }
    }
}
