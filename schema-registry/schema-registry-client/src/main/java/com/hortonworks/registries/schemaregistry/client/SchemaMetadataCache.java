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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.cache.RemovalCause.REPLACED;

/**
 *
 */
public class SchemaMetadataCache {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaMetadataCache.class);
    private final Cache<Long, SchemaMetadataInfo> cache;
    private final Map<String, Long> schemaNameToIdMap;
    private final SchemaMetadataFetcher schemaMetadataFetcher;

    SchemaMetadataCache(Long size, Long expiryInSecs, final SchemaMetadataFetcher schemaMetadataFetcher,
                        Map<String, Long> schemaNameToIdMap) {
        this.schemaMetadataFetcher = schemaMetadataFetcher;
        this.schemaNameToIdMap = schemaNameToIdMap;
        cache = CacheBuilder.newBuilder()
            .maximumSize(size)
            .expireAfterAccess(expiryInSecs, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<Long, SchemaMetadataInfo>() {
                @Override
                public void onRemoval(RemovalNotification<Long, SchemaMetadataInfo> notification) {
                    if (notification.getCause() != REPLACED) {
                        schemaNameToIdMap.remove(notification.getValue().getSchemaMetadata().getName());
                    }
                }
            })
            .build();
    }

    public SchemaMetadataCache(Long size, Long expiryInSecs, final SchemaMetadataFetcher schemaMetadataFetcher) {
        this(size, expiryInSecs, schemaMetadataFetcher, new ConcurrentHashMap<>());
    }

    public SchemaMetadataInfo get(Key key) {
        if (key == null) {
            return null;
        }
        SchemaMetadataInfo schemaMetadataInfo = null;
        try {
            schemaMetadataInfo = getSchemaMetadataInfoFromCache(key);
            if (schemaMetadataInfo == null) {
                schemaMetadataInfo = fetchFromRemote(key);
                updateCache(schemaMetadataInfo);
            }
        }  catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
                throw new RegistryRetryableException(e.getMessage(), e);
            }
            throw new RegistryException(e.getMessage(), e);
        } catch (SchemaNotFoundException e) {
            return null;
        }
        return schemaMetadataInfo;
    }

    public void invalidateSchemaMetadata(SchemaMetadataCache.Key key) {
        LOG.info("Invalidating cache entry for key [{}]", key);
        if (key == null) {
            return;
        }
        if (key.id != null) {
            cache.invalidate(key.id);
        } else if (key.name != null) {
            Long id = schemaNameToIdMap.get(key.name);
            if (id != null) {
                cache.invalidate(id);
            }
        }
    }

    // TODO if this is an internal class, please update the signature. Ensure
    // that schemaMetadataInfo.id cannot be null and key is not necessary.
    // Key can be inferred from schemaMetadataInfo.
    public void put(Key key, SchemaMetadataInfo schemaMetadataInfo) {
        LOG.info("Updating cache entry, key: [{}] value: [{}]", key, schemaMetadataInfo);
        if (schemaMetadataInfo == null) {
            LOG.error("SchemaMetadataInfo cannot be null.");
            throw new RegistryException("SchemaMetadataInfo cannot be null.");
        }
        if (schemaMetadataInfo.getId() == null) {
            LOG.error("SchemaMetadataInfo id is not available.");
            throw new RegistryException("SchemaMetadataInfo id is not available.");
        }
        updateCache(schemaMetadataInfo);
    }

    public SchemaMetadataInfo getIfPresent(Key key) {
        if (key == null) {
            return null;
        }
        return getSchemaMetadataInfoFromCache(key);
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

    private SchemaMetadataInfo getSchemaMetadataInfoFromCache(Key key) {
        if (key.id != null) {
            return cache.getIfPresent(key.id);
        }
        if (key.name != null) {
            Long id = schemaNameToIdMap.get(key.name);
            if (id != null) {
                return cache.getIfPresent(id);
            }
            return null;
        }
        throw new RegistryException("Key should have name or id as non null");
    }

    private void updateCache(SchemaMetadataInfo schemaMetadataInfo) {
        if (schemaMetadataInfo != null) {
            cache.put(schemaMetadataInfo.getId(), schemaMetadataInfo);
            schemaNameToIdMap.put(schemaMetadataInfo.getSchemaMetadata().getName(), schemaMetadataInfo.getId());
        }
    }

    private SchemaMetadataInfo fetchFromRemote(Key key) throws SchemaNotFoundException {
        if (key.id != null) {
            return schemaMetadataFetcher.fetch(key.id);
        }
        if (key.name != null) {
            return schemaMetadataFetcher.fetch(key.name);
        }
        return null;
    }
}
