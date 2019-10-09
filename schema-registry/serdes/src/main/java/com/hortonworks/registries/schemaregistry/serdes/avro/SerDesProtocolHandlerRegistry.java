/*
 * Copyright 2016-2019 Cloudera, Inc.
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

import com.hortonworks.registries.schemaregistry.serdes.SerDesProtocolHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registered ser/des protocol handlers which can be used in serializers/deserializers.
 */
public final class SerDesProtocolHandlerRegistry {

    public static final byte CONFLUENT_VERSION_PROTOCOL = 0x0;
    public static final byte METADATA_ID_VERSION_PROTOCOL = 0x1;
    public static final byte VERSION_ID_AS_LONG_PROTOCOL = 0x2;
    public static final byte VERSION_ID_AS_INT_PROTOCOL = 0x3;
    public static final byte CURRENT_PROTOCOL = VERSION_ID_AS_INT_PROTOCOL;

    private static final SerDesProtocolHandlerRegistry instance = new SerDesProtocolHandlerRegistry();

    private final Map<Byte, SerDesProtocolHandler> protocolWithHandlers = new ConcurrentHashMap<>();

    public static SerDesProtocolHandlerRegistry get() {
        return instance;
    }

    public SerDesProtocolHandler getSerDesProtocolHandler(Byte protocolId) {
        return protocolWithHandlers.get(protocolId);
    }

    public Map<Byte, SerDesProtocolHandler> getRegisteredSerDesProtocolHandlers() {
        return Collections.unmodifiableMap(protocolWithHandlers);
    }

    private SerDesProtocolHandlerRegistry() {
        List<SerDesProtocolHandler> inbuiltHandlers = Arrays.asList(new ConfluentProtocolHandler(), new SchemaMetadataIdProtocolHandler(),
                                                                    new SchemaVersionIdAsIntProtocolHandler(), new SchemaVersionIdAsLongProtocolHandler());
        for (SerDesProtocolHandler inbuiltHandler : inbuiltHandlers) {
            registerSerDesProtocolHandler(inbuiltHandler);
        }
    }

    /**
     * Registers the given protocol handler with it's protocol id.
     *
     * @param serDesProtocolHandler handler to be registered.
     */
    public void registerSerDesProtocolHandler(SerDesProtocolHandler serDesProtocolHandler) {
        SerDesProtocolHandler existingHandler = protocolWithHandlers.putIfAbsent(serDesProtocolHandler.getProtocolId(), serDesProtocolHandler);
        if (existingHandler != null) {
            throw new IllegalArgumentException("SerDesProtocolHandler is already registered with the given protocol id: " + serDesProtocolHandler.getProtocolId());
        }
    }

}
