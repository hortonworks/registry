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

    private final LoadingCache<SchemaVersionKey, SchemaVersionInfo> loadingCache;

    public SchemaVersionInfoCache(final SchemaVersionRetriever schemaRetriever, final long schemaCacheSize, final long schemaCacheExpiryInMilliSecs) {
        loadingCache = CacheBuilder.newBuilder()
                .maximumSize(schemaCacheSize)
                .expireAfterAccess(schemaCacheExpiryInMilliSecs, TimeUnit.MILLISECONDS)
                .build(new CacheLoader<SchemaVersionKey, SchemaVersionInfo>() {
                    @Override
                    public SchemaVersionInfo load(SchemaVersionKey key) throws Exception {
                        return schemaRetriever.retrieveSchemaVersion(key);
                    }
                });

    }

    public SchemaVersionInfo getSchema(SchemaVersionKey k) throws SchemaNotFoundException {
        try {
            return loadingCache.get(k);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
