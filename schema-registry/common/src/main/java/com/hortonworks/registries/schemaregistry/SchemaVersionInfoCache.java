/**
 * Copyright 2016 Hortonworks.
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
package com.hortonworks.registries.schemaregistry;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SchemaVersionInfoCache {

    private final LoadingCache<Key, SchemaVersionInfo> loadingCache;

    public SchemaVersionInfoCache(final SchemaVersionRetriever schemaRetriever, final long schemaCacheSize, final long schemaCacheExpiryInMilliSecs) {
        loadingCache = CacheBuilder.newBuilder()
                .maximumSize(schemaCacheSize)
                .expireAfterAccess(schemaCacheExpiryInMilliSecs, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<SchemaVersionInfoCache.Key, SchemaVersionInfo>() {
                    @Override
                    public SchemaVersionInfo load(SchemaVersionInfoCache.Key key) throws Exception {
                        if(key.schemaVersionKey != null) {
                            return schemaRetriever.retrieveSchemaVersion(key.schemaVersionKey);
                        } else if(key.schemaIdVersion != null) {
                            return schemaRetriever.retrieveSchemaVersion(key.schemaIdVersion);
                        } else {
                            throw new IllegalArgumentException("Given argument is not valid: " + key);
                        }
                    }
                });

    }

    public SchemaVersionInfo getSchema(SchemaVersionInfoCache.Key key) throws SchemaNotFoundException {
        try {
            return loadingCache.get(key);
        } catch (ExecutionException e) {
            if(e.getCause().getClass() == SchemaNotFoundException.class)
                throw (SchemaNotFoundException) e.getCause();
            throw new RuntimeException(e);
        }
    }

    public void invalidateSchema(SchemaVersionInfoCache.Key key) {
        loadingCache.invalidate(key);
    }

    public static class Key {

        private SchemaVersionKey schemaVersionKey;
        private SchemaIdVersion schemaIdVersion;

        public Key(SchemaVersionKey schemaVersionKey) {
            this.schemaVersionKey = schemaVersionKey;
        }

        public Key(SchemaIdVersion schemaIdVersion) {
            this.schemaIdVersion = schemaIdVersion;
        }

        public  static Key of(SchemaVersionKey schemaVersionKey) {
            return new Key(schemaVersionKey);
        }

        public static Key of(SchemaIdVersion schemaIdVersion) {
            return new Key(schemaIdVersion);
        }

        @Override
        public String toString() {
            return "Key{" +
                    "schemaVersionKey=" + schemaVersionKey +
                    ", schemaIdVersion=" + schemaIdVersion +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (schemaVersionKey != null ? !schemaVersionKey.equals(key.schemaVersionKey) : key.schemaVersionKey != null)
                return false;
            return schemaIdVersion != null ? schemaIdVersion.equals(key.schemaIdVersion) : key.schemaIdVersion == null;
        }

        @Override
        public int hashCode() {
            int result = schemaVersionKey != null ? schemaVersionKey.hashCode() : 0;
            result = 31 * result + (schemaIdVersion != null ? schemaIdVersion.hashCode() : 0);
            return result;
        }
    }

}
