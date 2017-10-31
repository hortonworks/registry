/*
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

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSerDesProtocolHandler.READER_SCHEMA;
import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSerDesProtocolHandler.WRITER_SCHEMA;

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaResolver;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serdes.SerDesProtocolHandler;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This class implements most of the required functionality for an avro deserializer by extending {@link AbstractSnapshotDeserializer}
 * and implementing the required methods.
 * <p>
 * <p>
 * The below example describes how to extend this deserializer with user supplied representation like MessageContext.
 * Default deserialization of avro payload is implemented in {@link #buildDeserializedObject(byte, InputStream, SchemaMetadata, Integer, Integer)}
 * and it can be used while implementing {@link #doDeserialize(Object, byte, SchemaMetadata, Integer, Integer)} as given
 * below.
 * </p>
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
 * public class MessageContextBasedDeserializer extends AbstractAvroSnapshotDeserializer<MessageContext> {
 *
 * {@literal @}Override
 * protected Object doDeserialize(MessageContext input,
 * byte protocolId,
 * SchemaMetadata schemaMetadata,
 * Integer writerSchemaVersion,
 * Integer readerSchemaVersion) throws SerDesException {
 * return buildDeserializedObject(protocolId,
 * input.payloadEntity,
 * schemaMetadata,
 * writerSchemaVersion,
 * readerSchemaVersion);
 * }
 *
 * {@literal @}Override
 * protected byte retrieveProtocolId(MessageContext input) throws SerDesException {
 * return (byte) input.headers.get("protocol.id");
 * }
 *
 * {@literal @}Override
 * protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, MessageContext input) throws SerDesException {
 * Long id = (Long) input.headers.get("schema.metadata.id");
 * Integer version = (Integer) input.headers.get("schema.version");
 * return new SchemaIdVersion(id, version);
 * }
 * }
 *
 * }</pre>
 *
 * @param <I> representation of the received input payload
 */
public abstract class AbstractAvroSnapshotDeserializer<I> extends AbstractSnapshotDeserializer<I, Object, Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAvroSnapshotDeserializer.class);

    public static final String SPECIFIC_AVRO_READER = "specific.avro.reader";

    private AvroSchemaResolver avroSchemaResolver;

    protected boolean useSpecificAvroReader = false;

    public AbstractAvroSnapshotDeserializer() {
        super();
    }

    public AbstractAvroSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }

    @Override
    public void doInit(Map<String, ?> config) {
        super.doInit(config);
        SchemaVersionRetriever schemaVersionRetriever = createSchemaVersionRetriever();
        avroSchemaResolver = new AvroSchemaResolver(schemaVersionRetriever);
        useSpecificAvroReader = (boolean) getValue(config, SPECIFIC_AVRO_READER, false);
    }

    private SchemaVersionRetriever createSchemaVersionRetriever() {
        return new SchemaVersionRetriever() {
            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                return schemaRegistryClient.getSchemaVersionInfo(key);
            }

            @Override
            public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                return schemaRegistryClient.getSchemaVersionInfo(key);
            }
        };
    }

    @Override
    protected Schema getParsedSchema(SchemaVersionKey schemaVersionKey) throws InvalidSchemaException, SchemaNotFoundException {
        return new Schema.Parser().parse(avroSchemaResolver.resolveSchema(schemaVersionKey));
    }

    /**
     * Builds the deserialized object from the given {@code payloadInputStream} and applying writer and reader schemas
     * from the respective given versions.
     *
     * @param protocolId          protocol id
     * @param payloadInputStream  payload
     * @param schemaMetadata      metadata about schema
     * @param writerSchemaVersion schema version of the writer
     * @param readerSchemaVersion schema version to be applied for reading or projection
     * @return the deserialized object
     * @throws SerDesException when any ser/des error occurs
     */
    protected Object buildDeserializedObject(byte protocolId,
                                             InputStream payloadInputStream,
                                             SchemaMetadata schemaMetadata,
                                             Integer writerSchemaVersion,
                                             Integer readerSchemaVersion) throws SerDesException {
        Object deserializedObj;
        String schemaName = schemaMetadata.getName();
        SchemaVersionKey writerSchemaVersionKey = new SchemaVersionKey(schemaName, writerSchemaVersion);
        LOG.debug("SchemaKey: [{}] for the received payload", writerSchemaVersionKey);
        Schema writerSchema = getSchema(writerSchemaVersionKey);
        if (writerSchema == null) {
            throw new RegistryException("No schema exists with metadata-key: " + schemaMetadata + " and writerSchemaVersion: " + writerSchemaVersion);
        }
        Schema readerSchema = readerSchemaVersion != null ? getSchema(new SchemaVersionKey(schemaName, readerSchemaVersion)) : null;

        deserializedObj = deserializePayloadForProtocol(protocolId, payloadInputStream, writerSchema, readerSchema);

        return deserializedObj;
    }

    protected Object deserializePayloadForProtocol(byte protocolId,
                                                   InputStream payloadInputStream,
                                                   Schema writerSchema,
                                                   Schema readerSchema) throws SerDesException  {
        Map<String, Object> props = new HashMap<>();
        props.put(SPECIFIC_AVRO_READER, useSpecificAvroReader);
        props.put(WRITER_SCHEMA, writerSchema);
        props.put(READER_SCHEMA, readerSchema);
        SerDesProtocolHandler serDesProtocolHandler = SerDesProtocolHandlerRegistry.get().getSerDesProtocolHandler(protocolId);

        return serDesProtocolHandler.handlePayloadDeserialization(payloadInputStream, props);
    }
}
