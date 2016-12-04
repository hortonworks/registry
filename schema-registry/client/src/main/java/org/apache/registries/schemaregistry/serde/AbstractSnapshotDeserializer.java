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
package org.apache.registries.schemaregistry.serde;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaMetadataInfo;
import org.apache.registries.schemaregistry.SchemaVersionInfo;
import org.apache.registries.schemaregistry.SchemaVersionKey;
import org.apache.registries.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @param <O> deserialized representation of the received payload
 * @param <S> parsed schema representation to be stored in local cache
 */
public abstract class AbstractSnapshotDeserializer<O, S> implements SnapshotDeserializer<InputStream, O, SchemaMetadata, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSnapshotDeserializer.class);
    /**
     * Maximum inmemory cache size maintained in deserializer instance.
     */
    public static final String DESERIALIZER_SCHEMA_CACHE_MAX_SIZE = "schemaregistry.deserializer.schema.cache.size";
    /**
     * Default schema cache max size.
     */
    public static final Integer DEFAULT_SCHEMA_CACHE_SIZE = 1024;
    /**
     * Expiry interval(in milli seconds) after an access for an entry in schema cache
     */
    public static final String DESERIALIZER_SCHEMA_CACHE_EXPIRY_IN_SECS = "schemaregistry.deserializer.schema.cache.expiry.secs";
    /**
     * Default schema cache entry access expiration interval
     */
    public static final Long DEFAULT_DESERIALIZER_SCHEMA_CACHE_EXPIRY_IN_SECS = 60 * 5L;

    private LoadingCache<SchemaVersionKey, S> schemaCache;
    private LoadingCache<Long, SchemaMetadata> schemaMetadataCache;
    private SchemaRegistryClient schemaRegistryClient;

    @Override
    public void init(Map<String, ?> config) {
        LOG.debug("Initialized with config: [{}]", config);
        schemaRegistryClient = new SchemaRegistryClient(config);

        schemaCache = CacheBuilder.newBuilder()
                .maximumSize(getCacheMaxSize(config))
                .expireAfterAccess(getCacheExpiryInMillis(config), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<SchemaVersionKey, S>() {
                    @Override
                    public S load(SchemaVersionKey schemaVersionKey) throws Exception {
                        SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(schemaVersionKey);
                        return schemaVersionInfo != null ? getParsedSchema(schemaVersionInfo) : null;
                    }
                });

        schemaMetadataCache = CacheBuilder.newBuilder()
                .maximumSize(getCacheMaxSize(config))
                .expireAfterAccess(getCacheExpiryInMillis(config), TimeUnit.MILLISECONDS)
                .build(new CacheLoader<Long, SchemaMetadata>() {
                    @Override
                    public SchemaMetadata load(Long schemaVersionKey) throws Exception {
                        return schemaRegistryClient.getSchemaMetadataInfo(schemaVersionKey).getSchemaMetadata();
                    }
                });

    }

    private Long getCacheExpiryInMillis(Map<String, ?> config) {
        Long value = (Long) getValue(config, DESERIALIZER_SCHEMA_CACHE_EXPIRY_IN_SECS, DEFAULT_DESERIALIZER_SCHEMA_CACHE_EXPIRY_IN_SECS);
        if (value < 0) {
            throw new IllegalArgumentException("Property: " + DESERIALIZER_SCHEMA_CACHE_EXPIRY_IN_SECS + "must be non negative.");
        }
        return value;
    }

    private Integer getCacheMaxSize(Map<String, ?> config) {
        Integer value = (Integer) getValue(config, DESERIALIZER_SCHEMA_CACHE_MAX_SIZE, DEFAULT_SCHEMA_CACHE_SIZE);
        if (value < 0) {
            throw new IllegalArgumentException("Property: " + DESERIALIZER_SCHEMA_CACHE_MAX_SIZE + "must be non negative.");
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

    protected abstract S getParsedSchema(SchemaVersionInfo schemaVersionInfo);

    @Override
    public O deserialize(InputStream payloadInputStream,
                         SchemaMetadata schemaMetadata,
                         Integer readerSchemaVersion) throws SerDesException {
        try {
            SchemaIdVersion schemaIdVersion = retrieveSchemaIdVersion(payloadInputStream);

            if(schemaMetadata == null) {
                schemaMetadata = schemaMetadataCache.get(schemaIdVersion.getSchemaMetadataId());
            }
            return doDeserialize(payloadInputStream, schemaMetadata, schemaIdVersion.getVersion(), readerSchemaVersion);
        } catch (ExecutionException e) {
            throw new SerDesException(e);
        }
    }

    protected abstract O doDeserialize(InputStream payloadInputStream,
                                       SchemaMetadata schemaMetadata,
                                       Integer writerSchemaVersion,
                                       Integer readerSchemaVersion) throws SerDesException;

    protected abstract SchemaIdVersion retrieveSchemaIdVersion(InputStream payloadInputStream) throws SerDesException;

    protected S getSchema(SchemaVersionKey schemaVersionKey) {
        try {
            return schemaCache.get(schemaVersionKey);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        schemaRegistryClient.close();
    }
}
