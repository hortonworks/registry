/**
 * Copyright 2016-2019 Cloudera, Inc.
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

package com.hortonworks.registries.cache.view.io.loader;

import com.hortonworks.registries.cache.Cache;
import com.hortonworks.registries.cache.view.datastore.DataStoreReader;
import org.slf4j.Logger;

import java.util.Collection;

public abstract class CacheLoader<K,V> {
    protected final Cache<K,V> cache;
    protected final DataStoreReader<K, V> dataStoreReader;

    public CacheLoader(Cache<K, V> cache, DataStoreReader<K,V> dataStoreReader) {
        this.cache = cache;
        this.dataStoreReader = dataStoreReader;
    }

    public abstract void loadAll(Collection<? extends K> keys, CacheLoaderCallback<K,V> callback);

    protected void handleException(Collection<? extends K> keys, CacheLoaderCallback<K, V> callback, Exception e, Logger LOG) {
        final String msg = String.format("Exception occurred while loading keys [%s]", keys);
        callback.onCacheLoadingFailure(new Exception(msg, e));
        LOG.error(msg, e);
    }
}
