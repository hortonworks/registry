/**
 * Copyright 2016-2023 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.schemaregistry.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.cache.SchemaRegistryCacheType;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.cache.RemovalCause.REPLACED;

/**
 * Loading cache for {@link Key} with values {@link SchemaVersionInfo}.
 */
public class SchemaVersionInfoCache implements AbstractCache {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionInfoCache.class);

    private final Cache<Long, SchemaVersionInfo> cache;
    private final ConcurrentMap<SchemaVersionKey, Long> metadataNameVersion2Id;
    private final ConcurrentMap<SchemaIdVersion, Long> metadataIdVersion2Id;
    private final SchemaVersionRetriever schemaVersionRetriever;

    private final SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher;

    SchemaVersionInfoCache(final SchemaVersionRetriever schemaVersionRetriever,
                                  final SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher,
                                  final int schemaCacheSize,
                                  final long schemaCacheExpiryInMilliSecs,
                           ConcurrentMap<SchemaVersionKey, Long> metadataNameVersion2Id,
                           ConcurrentMap<SchemaIdVersion, Long> metadataIdVersion2Id
                           ) {
        this.metadataNameVersion2Id = metadataNameVersion2Id;
        this.metadataIdVersion2Id = metadataIdVersion2Id;
        this.schemaVersionRetriever = schemaVersionRetriever;
        this.schemaMetadataFetcher = schemaMetadataFetcher;
        cache = createCache(schemaCacheSize, schemaCacheExpiryInMilliSecs);
    }

    public SchemaVersionInfoCache(final SchemaVersionRetriever schemaVersionRetriever,
                                  final SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher,
                                  final int schemaCacheSize,
                                  final long schemaCacheExpiryInMilliSecs) {
        this(schemaVersionRetriever, schemaMetadataFetcher, schemaCacheSize, schemaCacheExpiryInMilliSecs,
            new ConcurrentHashMap<>(schemaCacheSize),
            new ConcurrentHashMap<>(schemaCacheSize));
    }

    public SchemaVersionInfo getSchema(SchemaVersionInfoCache.Key key) throws SchemaNotFoundException {
        try {
            LOG.debug("Trying to load entry for cache with key [{}] from target service", key);
            SchemaVersionInfo schemaVersionInfo = null;
            if (key.schemaVersionKey != null) {
                schemaVersionInfo = getSchemaVersionInfoFromSchemaVersionKey(key.schemaVersionKey);
            } else if (key.schemaIdVersion != null) {
                schemaVersionInfo = getSchemaVersionInfoFromSchemaIdVersion(key.schemaIdVersion);
            } else {
                throw new IllegalArgumentException("Given argument is not valid: " + key);
            }
            LOG.trace("Result: {}", schemaVersionInfo);
            return schemaVersionInfo;
        } catch (SchemaNotFoundException snfe) {
            if (key.schemaVersionKey != null) {
                throw new SchemaNotFoundException(key.schemaVersionKey.toString(), snfe.getMessage());
            } else {
                throw snfe;
            }
        } catch (NotFoundException nfe) {
            throw new SchemaNotFoundException(key.toString(), nfe);
        }
    }

    @CheckForNull
    public SchemaVersionInfo getSchemaIfPresent(SchemaVersionInfoCache.Key key) {
        LOG.debug("Trying to get entry from cache if it is present in local cache with key [{}]", key);
        Long id = null;
        if (key.schemaVersionKey != null) {
             id = metadataNameVersion2Id.getOrDefault(key.schemaVersionKey, null);
        } else if (key.schemaIdVersion != null) {
             id = getIdFromSchemaIdVersionOrCache(key.schemaIdVersion);
        }
        if (id != null) {
            return cache.getIfPresent(id);
        }
        return null;
    }

    public void invalidateSchema(SchemaVersionInfoCache.Key key) {
        LOG.debug("Invalidating cache entry for key [{}]", key);
        if (key.schemaVersionKey != null) {
            invalidateBySchemaVersionKey(key.schemaVersionKey);
        } else if (key.schemaIdVersion != null) {
            invalidateBySchemaIdVersion(key.schemaIdVersion);
        } else {
            throw new IllegalArgumentException("Given argument is not valid: " + key);
        }
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public SchemaRegistryCacheType getCacheType() {
        return SchemaRegistryCacheType.SCHEMA_VERSION_CACHE;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Key {

        @JsonProperty
        private SchemaVersionKey schemaVersionKey;

        @JsonProperty
        private SchemaIdVersion schemaIdVersion;

        public Key(SchemaVersionKey schemaVersionKey) {
            this.schemaVersionKey = schemaVersionKey;
        }

        public Key(SchemaIdVersion schemaIdVersion) {
            this.schemaIdVersion = schemaIdVersion;
        }

        public static Key of(SchemaVersionKey schemaVersionKey) {
            return new Key(schemaVersionKey);
        }

        public static Key of(SchemaIdVersion schemaIdVersion) {
            return new Key(schemaIdVersion);
        }

        // For JSON serialization/deserialization
        private Key() {

        }

        @Override
        public String toString() {
            return "Key {" +
                    "schemaVersionKey=" + schemaVersionKey +
                    ", schemaIdVersion=" + schemaIdVersion +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key key = (Key) o;

            if (schemaVersionKey != null ? !schemaVersionKey.equals(key.schemaVersionKey)
                                         : key.schemaVersionKey != null) {
                return false;
            }
            return schemaIdVersion != null ? schemaIdVersion.equals(key.schemaIdVersion) : key.schemaIdVersion == null;
        }

        @Override
        public int hashCode() {
            int result = schemaVersionKey != null ? schemaVersionKey.hashCode() : 0;
            result = 31 * result + (schemaIdVersion != null ? schemaIdVersion.hashCode() : 0);
            return result;
        }
    }

    private SchemaVersionInfo getSchemaVersionInfoFromSchemaIdVersion(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        Long id = getIdFromSchemaIdVersionOrCache(schemaIdVersion);
        if (id != null) {
            SchemaVersionInfo schemaVersionInfo = cache.getIfPresent(id);
            if (schemaVersionInfo != null) {
                return schemaVersionInfo;
            }
        }
        SchemaVersionInfo schemaVersionInfo = schemaVersionRetriever.retrieveSchemaVersion(schemaIdVersion);
        schemaVersionInfo = updateSchemaVersionInfoIfSchemaMetadataIdIsNull(schemaVersionInfo, schemaIdVersion.getSchemaMetadataId());
        updateCaches(schemaVersionInfo, createSchemaVersionKeyFromSchemaVersionInfo(schemaVersionInfo));
        return schemaVersionInfo;
    }

    private SchemaVersionInfo updateSchemaVersionInfoIfSchemaMetadataIdIsNull(SchemaVersionInfo schemaVersionInfo,
                                                                              Long schemaMetadataId) throws SchemaNotFoundException {
        if (schemaVersionInfo.getSchemaMetadataId() != null) {
            return schemaVersionInfo;
        }
        Long metadataId = schemaMetadataId;
        if (metadataId == null) {
            SchemaMetadataInfo schemaMetadataInfo = schemaMetadataFetcher.fetch(schemaVersionInfo.getName());
            if (schemaMetadataInfo == null) {
                throw new SchemaNotFoundException(schemaVersionInfo.getName(), "Could not find schema.");
            }
            metadataId = schemaMetadataInfo.getId();
        }
        return new SchemaVersionInfo(schemaVersionInfo.getId(),
            schemaVersionInfo.getName(),
            schemaVersionInfo.getVersion(),
            metadataId,
            schemaVersionInfo.getSchemaText(),
            schemaVersionInfo.getTimestamp(),
            schemaVersionInfo.getDescription(),
            schemaVersionInfo.getStateId()
        );
    }

    private void updateCaches(SchemaVersionInfo schemaVersionInfo, SchemaVersionKey schemaVersionKey) {
        long id = schemaVersionInfo.getId();
        metadataNameVersion2Id.put(schemaVersionKey, id);
        metadataIdVersion2Id.put(createSchemaIdVersionFromSchemaVersionInfo(schemaVersionInfo), id);
        cache.put(id, schemaVersionInfo);
    }

    private SchemaVersionInfo getSchemaVersionInfoFromSchemaVersionKey(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        SchemaVersionInfo schemaVersionInfo = getSchemaVersionInfoFromCache(schemaVersionKey);
        if (schemaVersionInfo != null) {
            return schemaVersionInfo;
        }
        schemaVersionInfo = schemaVersionRetriever.retrieveSchemaVersion(schemaVersionKey);
        schemaVersionInfo = updateSchemaVersionInfoIfSchemaMetadataIdIsNull(schemaVersionInfo, null);
        updateCaches(schemaVersionInfo, schemaVersionKey);

        return schemaVersionInfo;
    }

    private SchemaVersionInfo getSchemaVersionInfoFromCache(SchemaVersionKey schemaVersionKey) {
        Long id = metadataNameVersion2Id.getOrDefault(schemaVersionKey, null);
        if (id != null) {
            return cache.getIfPresent(id);
        }
        return null;
    }

    private void invalidateInternalCaches(SchemaVersionInfo schemaVersionInfo) {
        SchemaVersionKey schemaVersionKey = createSchemaVersionKeyFromSchemaVersionInfo(schemaVersionInfo);
        LOG.trace("Invalidate schemaVersionKey: {}", schemaVersionKey);
        metadataNameVersion2Id.remove(schemaVersionKey);
        SchemaIdVersion schemaIdVersion = createSchemaIdVersionFromSchemaVersionInfo(schemaVersionInfo);
        LOG.trace("Invalidate schemaIdVersion: {}", schemaIdVersion);
        metadataIdVersion2Id.remove(schemaIdVersion);
    }

    private static SchemaIdVersion createSchemaIdVersionFromSchemaVersionInfo(SchemaVersionInfo schemaVersionInfo) {
        return new SchemaIdVersion(schemaVersionInfo.getSchemaMetadataId(), schemaVersionInfo.getVersion());
    }

    private static SchemaVersionKey createSchemaVersionKeyFromSchemaVersionInfo(SchemaVersionInfo schemaVersionInfo) {
        return new SchemaVersionKey(schemaVersionInfo.getName(), schemaVersionInfo.getVersion());
    }

    private Cache<Long, SchemaVersionInfo> createCache(int schemaCacheSize,
                                                       long schemaCacheExpiryInMilliSecs) {
        return CacheBuilder.newBuilder()
            .maximumSize(schemaCacheSize)
            .expireAfterAccess(schemaCacheExpiryInMilliSecs, TimeUnit.MILLISECONDS)
            .removalListener((RemovalListener<Long, SchemaVersionInfo>) notification -> {
                if (notification.getCause() != REPLACED) {
                    SchemaVersionInfo id = notification.getValue();
                    if (id != null) {
                        invalidateInternalCaches(id);
                    }
                }
            })
            .build();
    }

    private void invalidateBySchemaIdVersion(SchemaIdVersion key) {
        Long id = getIdFromSchemaIdVersionOrCache(key);
        if (id != null) {
            cache.invalidate(id);
        }
    }

    private Long getIdFromSchemaIdVersionOrCache(SchemaIdVersion key) {
        Long id = key.getSchemaVersionId();
        if (id == null) {
            id = metadataIdVersion2Id.getOrDefault(key, null);
        }
        return id;
    }

    private void invalidateBySchemaVersionKey(SchemaVersionKey key) {
        Long id = metadataNameVersion2Id.getOrDefault(key, null);
        if (id != null) {
            cache.invalidate(id);
        }
    }
}
