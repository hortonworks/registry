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

import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaResolver;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serde.AbstractSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements most of the required functionality for an avro deserializer by extending {@link AbstractSnapshotDeserializer}
 * and implementing the required methods.
 *
 * <p>
 * The below example describes how to extend this deserializer with user supplied representation like MessageContext.
 * Default deserialization of avro payload is implemented in {@link #buildDeserializedObject(InputStream, SchemaMetadata, Integer, Integer)}
 * and it can be used while implementing {@link #doDeserialize(Object, byte, SchemaMetadata, Integer, Integer)} as given
 * below.
 * </p>
 *
 * <pre>{@code
    public class MessageContext {
        final Map<String, Object> headers;
        final InputStream payloadEntity;

        public MessageContext(Map<String, Object> headers, InputStream payloadEntity) {
            this.headers = headers;
            this.payloadEntity = payloadEntity;
        }
    }

    public class MessageContextBasedDeserializer extends AbstractAvroSnapshotDeserializer<MessageContext> {

        {@literal @}Override
        protected Object doDeserialize(MessageContext input,
                                       byte protocolId,
                                       SchemaMetadata schemaMetadata,
                                       Integer writerSchemaVersion,
                                       Integer readerSchemaVersion) throws SerDesException {
            return buildDeserializedObject(input.payloadEntity,
                                             schemaMetadata,
                                             writerSchemaVersion,
                                             readerSchemaVersion);
        }

        {@literal @}Override
        protected byte retrieveProtocolId(MessageContext input) throws SerDesException {
            return (byte) input.headers.get("protocol.id");
        }

        {@literal @}Override
        protected SchemaIdVersion retrieveSchemaIdVersion(byte protocolId, MessageContext input) throws SerDesException {
            Long id = (Long) input.headers.get("schema.metadata.id");
            Integer version = (Integer) input.headers.get("schema.version");
            return new SchemaIdVersion(id, version);
        }
    }

  }</pre>
 *
 * @param <I> representation of the received input payload
 */
public abstract class AbstractAvroSnapshotDeserializer<I> extends AbstractSnapshotDeserializer<I, Object, Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAvroSnapshotDeserializer.class);
    
    public static final String SPECIFIC_AVRO_READER = "specific.avro.reader";

    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap();

    private AvroSchemaResolver avroSchemaResolver;

    protected boolean useSpecificAvroReader = false;

    public AbstractAvroSnapshotDeserializer() {
        super();
    }
    
    public AbstractAvroSnapshotDeserializer(ISchemaRegistryClient schemaRegistryClient) {
        super(schemaRegistryClient);
    }
    
    @Override
    public void init(Map<String, ?> config) {
        super.init(config);
        SchemaVersionRetriever schemaVersionRetriever = createSchemaVersionRetriever();
        avroSchemaResolver = new AvroSchemaResolver(schemaVersionRetriever);
        this.useSpecificAvroReader = (boolean) getValue(config, SPECIFIC_AVRO_READER, false);
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
     * @param payloadInputStream payload
     * @param schemaMetadata metadata about schema
     * @param writerSchemaVersion schema version of the writer
     * @param readerSchemaVersion schema version to be applied for reading or projection
     * @return
     * @throws SerDesException when any ser/des error occurs
     */
    protected Object buildDeserializedObject(InputStream payloadInputStream,
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
                Schema readerSchema = readerSchemaVersion != null ? getSchema(new SchemaVersionKey(schemaName, readerSchemaVersion)) : null;

                DatumReader datumReader = getDatumReader(writerSchema, readerSchema);
                deserializedObj = datumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
            }
        } catch (IOException e) {
            throw new SerDesException(e);
        }

        return deserializedObj;
    }


    private DatumReader getDatumReader(Schema writerSchema, Schema readerSchema) {
        if(this.useSpecificAvroReader) {
            if(readerSchema == null) {
                readerSchema = this.getReaderSchema(writerSchema);
            }

            return new SpecificDatumReader(writerSchema, readerSchema);
        } else {
            return readerSchema == null ? new GenericDatumReader(writerSchema) : new GenericDatumReader(writerSchema, readerSchema);
        }
    }

    private Schema getReaderSchema(Schema writerSchema) {
        Schema readerSchema = this.readerSchemaCache.get(writerSchema.getFullName());
        if(readerSchema == null) {
            Class readerClass = SpecificData.get().getClass(writerSchema);
            if(readerClass == null) {
                throw new SerDesException("Could not find class " + writerSchema.getFullName() + " specified in writer\'s schema whilst finding reader\'s schema for a SpecificRecord.");
            }
            try {
                readerSchema = ((SpecificRecord)readerClass.newInstance()).getSchema();
            } catch (InstantiationException var5) {
                throw new SerDesException(writerSchema.getFullName() + " specified by the " + "writers schema could not be instantiated to find the readers schema.");
            } catch (IllegalAccessException var6) {
                throw new SerDesException(writerSchema.getFullName() + " specified by the " + "writers schema is not allowed to be instantiated to find the readers schema.");
            }

            this.readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
        }

        return readerSchema;
    }
}
