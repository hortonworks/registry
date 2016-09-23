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
package com.hortonworks.registries.schemaregistry.avro;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AvroSnapshotDeserializer implements SnapshotDeserializer<InputStream, Object, SchemaKey, Schema> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSnapshotDeserializer.class);

    public static final String DESERIALIZER_CACHE_MAX_SIZE = "schemaregistry.deserializer.cache.size";
    public static final Integer DEFAULT_SCHEMA_CACHE_SIZE = 1024;
    public static final String DESERIALIZER_CACHE_EXPIRY_IN_MILLS = "schemaregistry.deserializer.cache.expiry";
    public static final Long DEFAULT_DESERIALIZER_CACHE_EXPIRY_IN_MILLS = 60 * 5 * 1000L;

    private SchemaRegistryClient schemaRegistryClient;
    protected LoadingCache<SchemaVersionKey, Schema> schemaCache;

    @Override
    public void init(Map<String, ?> config) {
        LOG.debug("Initialized with config: [{}]", config);
        schemaRegistryClient = new SchemaRegistryClient(config);

        schemaCache = CacheBuilder.newBuilder()
                .maximumSize(getCacheMaxSize(config))
                .expireAfterAccess(getCacheExpiryInMillis(config), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<SchemaVersionKey, Schema>() {
                    @Override
                    public Schema load(SchemaVersionKey schemaVersionKey) throws Exception {
                        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaVersionKey);
                        return schemaVersionInfo != null ? new Schema.Parser().parse(schemaVersionInfo.getSchemaText()) : null;
                    }
                });
    }

    private Long getCacheExpiryInMillis(Map<String, ?> config) {
        Long value = (Long) getValue(config, DESERIALIZER_CACHE_EXPIRY_IN_MILLS, DEFAULT_DESERIALIZER_CACHE_EXPIRY_IN_MILLS);
        if (value < 0) {
            throw new IllegalArgumentException("Property: " + DESERIALIZER_CACHE_EXPIRY_IN_MILLS + "must be non negative.");
        }
        return value;
    }

    private Integer getCacheMaxSize(Map<String, ?> config) {
        Integer value = (Integer) getValue(config, DESERIALIZER_CACHE_MAX_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
        if (value < 0) {
            throw new IllegalArgumentException("Property: " + DESERIALIZER_CACHE_MAX_SIZE + "must be non negative.");
        }
        return value;
    }

    private Object getValue(Map<String, ?> config, String key, Object defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    @Override
    public Object deserialize(InputStream payloadInputStream, SchemaKey writerSchemaKey, Schema readerSchema) throws SerDesException {
        Object deserializedObj = null;
        try {
            int version = readVersion(payloadInputStream);
            SchemaVersionKey schemaVersionKey = new SchemaVersionKey(writerSchemaKey, version);
            LOG.debug("SchemaKey: [{}] for the received payload", schemaVersionKey);
            Schema writerSchema = schemaCache.get(schemaVersionKey);
            if (writerSchema == null) {
                throw new SerDesException("No schema exists with metadata-key: " + writerSchemaKey + " and version: " + version);
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
                if (recordType == AvroUtils.GENERIC_RECORD) {
                    datumReader = readerSchema != null ? new GenericDatumReader(writerSchema, readerSchema)
                            : new GenericDatumReader(writerSchema);
                } else {
                    datumReader = readerSchema != null ? new SpecificDatumReader(writerSchema, readerSchema)
                            : new SpecificDatumReader(writerSchema);
                }
                deserializedObj = datumReader.read(null, DecoderFactory.get().binaryDecoder(payloadInputStream, null));
            }
        } catch (IOException | ExecutionException e) {
            throw new SerDesException(e);
        }

        return deserializedObj;
    }

    private int readVersion(InputStream payloadInputStream) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        payloadInputStream.read(byteBuffer.array());
        return byteBuffer.getInt();
    }


    @Override
    public void close() throws Exception {
        schemaRegistryClient.close();
    }
}
