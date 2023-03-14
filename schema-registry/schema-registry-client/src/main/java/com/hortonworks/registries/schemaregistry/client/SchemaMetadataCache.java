/**
 * Copyright 2016-2023 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.client;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SchemaMetadataCache {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaMetadataCache.class);

    private final LoadingCache<Key, SchemaMetadataInfo> loadingCache;
    private final BiMap<String, Long> schemaNameToIdMap;

    public SchemaMetadataCache(Long size, Long expiryInSecs, final SchemaMetadataFetcher schemaMetadataFetcher) {
        schemaNameToIdMap = Maps.synchronizedBiMap(HashBiMap.create());
        loadingCache = CacheBuilder.newBuilder()
                .maximumSize(size)
                .expireAfterAccess(expiryInSecs, TimeUnit.SECONDS)
                .build(new CacheLoader<Key, SchemaMetadataInfo>() {
                    @Override
                    public SchemaMetadataInfo load(Key key) throws Exception {
                        SchemaMetadataInfo schemaMetadataInfo;
                        Key otherKey;
                        if (key.getName() != null) {
                            schemaMetadataInfo = schemaMetadataFetcher.fetch(key.getName());
                            otherKey = Key.of(schemaMetadataInfo.getId());
                            schemaNameToIdMap.put(key.getName(), schemaMetadataInfo.getId());
                        } else if (key.getId() != null) {
                            schemaMetadataInfo = schemaMetadataFetcher.fetch(key.getId());
                            otherKey = Key.of(schemaMetadataInfo.getSchemaMetadata().getName());
                            schemaNameToIdMap.put(schemaMetadataInfo.getSchemaMetadata().getName(), schemaMetadataInfo.getId());
                        } else {
                            throw new RegistryException("Key should have name or id as non null");
                        }
                        loadingCache.put(otherKey, schemaMetadataInfo);
                        return schemaMetadataInfo;
                    }
                });
    }

    public SchemaMetadataInfo get(Key key) {
        SchemaMetadataInfo schemaMetadataInfo;
        try {
            schemaMetadataInfo = loadingCache.get(key);
        } catch (ExecutionException e) {
            LOG.error("Error occurred while retrieving schema metadata for [{}]", key, e);
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw new RegistryRetryableException(cause.getMessage(), cause);
            } else if (cause instanceof RuntimeException) {
                if (cause.getCause() instanceof IOException) {
                    throw new RegistryRetryableException(cause.getMessage(), cause);
                } else {
                    throw new RegistryException(cause.getMessage(), cause);
                }
            } else if (!(cause instanceof SchemaNotFoundException)) {
                throw new RegistryException(cause.getMessage(), cause);
            }
            schemaMetadataInfo = null;
        } catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // Do not expose cache implementation details to the caller
                throw (RuntimeException) cause;
            } else {
                // Should not happen, best option is to rethrow
                throw e;
            }
        }

        return schemaMetadataInfo;
    }

    public void invalidateSchemaMetadata(SchemaMetadataCache.Key key) {
        LOG.info("Invalidating cache entry for key [{}]", key);
        if (loadingCache.size() == 0) {
            return;
        }

        // If the cache doesn't have entry for the key, then no need to invalidate the cache
        if (loadingCache.getIfPresent(key) != null) {
            loadingCache.invalidate(key);
        }

        final Key otherKey;
        final Object id = key.id == null ? schemaNameToIdMap.get(key.name) : schemaNameToIdMap.inverse().get(key.id);
        if (id == null) {
            return;
        } else if (id instanceof Long) {
            otherKey = Key.of((Long) id);
        } else {
            otherKey = Key.of((String) id);
        }

        if (loadingCache.getIfPresent(otherKey) != null) {
            loadingCache.invalidate(otherKey);
        }
    }

    public void put(Key key, SchemaMetadataInfo schemaMetadataInfo) {
        loadingCache.put(key, schemaMetadataInfo);
    }

    public SchemaMetadataInfo getIfPresent(Key key) {
        return loadingCache.getIfPresent(key);
    }

    public interface SchemaMetadataFetcher {
        SchemaMetadataInfo fetch(String name) throws SchemaNotFoundException;

        SchemaMetadataInfo fetch(Long id) throws SchemaNotFoundException;
    }

    public static class Key {
        private String name;
        private Long id;

        private Key(String name) {
            Preconditions.checkNotNull(name, "name can not be null");
            this.name = name;
        }

        private Key(Long id) {
            Preconditions.checkNotNull(id, "id can not be null");
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public Long getId() {
            return id;
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

            if (name != null ? !name.equals(key.name) : key.name != null) {
                return false;
            }
            return id != null ? id.equals(key.id) : key.id == null;

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }

        public static Key of(String name) {
            return new Key(name);
        }

        public static Key of(Long id) {
            return new Key(id);
        }
    }
}
