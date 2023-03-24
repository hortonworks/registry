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
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.SerDesProtocolHandler;
import com.hortonworks.registries.schemaregistry.serdes.avro.exceptions.AvroException;
import org.apache.avro.Schema;
import org.apache.commons.collections.MapUtils;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSerDesProtocolHandler.LOGICAL_TYPE_CONVERSION_ENABLED;

/**
 * The below example describes how to extend this serializer with user supplied representation like MessageContext class.
 * Respective {@link SnapshotDeserializer} implementation is done extending {@link AbstractSnapshotDeserializer}.
 * <p>
 * <pre>{@code
 * public class MessageContext {
 * final Map<String, Object> headers;
 * final InputStream payloadEntity;
 *
 * public MessageContext(Map<String, Object> headers, InputStream payloadEntity) {
 * this.headers = headers;
 * this.payloadEntity = payloadEntity;
 * }
 * }
 *
 * public class MessageContextBasedAvroSerializer extends AbstractAvroSnapshotSerializer<MessageContext> {
 *
 * {@literal @}Override
 * protected MessageContext doSerialize(Object input, SchemaIdVersion schemaIdVersion) throws SerDesException {
 * Map<String, Object> headers = new HashMap<>();
 *
 * headers.put("protocol.id", 0x1);
 * headers.put("schema.metadata.id", schemaIdVersion.getSchemaMetadataId());
 * headers.put("schema.version", schemaIdVersion.getVersion());
 *
 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
 *
 * try(BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(baos)) {
 * writeContentPayload(input, bufferedOutputStream);
 * } catch (IOException e) {
 * throw new SerDesException(e);
 * }
 *
 * ByteArrayInputStream payload = new ByteArrayInputStream(baos.toByteArray());
 *
 * return new MessageContext(headers, payload);
 * }
 * }
 * }</pre>
 *
 * @param <O> serialized output type. For ex: byte[], String etc.
 */
public abstract class AbstractAvroSnapshotSerializer<O> extends AbstractSnapshotSerializer<Object, O> {

    public AbstractAvroSnapshotSerializer() {
        super();
    }

    public AbstractAvroSnapshotSerializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    /**
     * Property name for protocol version to be set with {@link #init(Map)}.
     */
    public static final String SERDES_PROTOCOL_VERSION = "serdes.protocol.version";

    protected SerDesProtocolHandler serDesProtocolHandler;

    private Map<String, Object> serializationContext = new HashMap<>();

    @Override
    public void doInit(Map<String, ?> config) {

        long protocolVersion = SerDesProtocolHandlerRegistry.CURRENT_PROTOCOL;

        Object configValue = config.get(SERDES_PROTOCOL_VERSION);
        if (configValue != null) {
            protocolVersion = longValue(configValue);
        }

        validateSerdesProtocolVersion(protocolVersion);

        SerDesProtocolHandler serDesProtocolHandler = SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler((byte) protocolVersion);
        if (serDesProtocolHandler == null) {
            throw new AvroException("SerDesProtocolHandler with protocol version " + protocolVersion + " does not exist");
        }

        this.serDesProtocolHandler = serDesProtocolHandler;
        boolean logicalTypeConversionEnabled = MapUtils.getBooleanValue(config, LOGICAL_TYPE_CONVERSION_ENABLED, false);
        serializationContext.put(LOGICAL_TYPE_CONVERSION_ENABLED, logicalTypeConversionEnabled);
    }

    private long longValue(Object configValue) {
        if (configValue instanceof Number) {
            return ((Number) configValue).longValue();
        } else {
            return Long.parseLong(configValue.toString());
        }
    }

    private void validateSerdesProtocolVersion(long protocolVersion) {
        if (protocolVersion < 0 || protocolVersion > Byte.MAX_VALUE) {
            throw new AvroException(SERDES_PROTOCOL_VERSION + " value should be in [0, " + Byte.MAX_VALUE + "]");
        }
    }

    /**
     * @param input avro object
     * @return textual representation of the schema of the given {@code input} avro object
     */
    @Override
    protected String getSchemaText(Object input) {
        Schema schema = AvroUtils.computeSchema(input);
        return schema.toString();
    }

    protected void serializeSchemaVersion(OutputStream os, SchemaIdVersion schemaIdVersion) throws SerDesException {
        serDesProtocolHandler.handleSchemaVersionSerialization(os, schemaIdVersion);
    }

    protected void serializePayload(OutputStream os, Object input) throws SerDesException {
        serDesProtocolHandler.handlePayloadSerialization(os, input, serializationContext);
    }

    protected Byte getProtocolId() {
        return serDesProtocolHandler.getProtocolId();
    }
}
