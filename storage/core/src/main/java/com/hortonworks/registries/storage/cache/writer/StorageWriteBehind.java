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

package com.hortonworks.registries.storage.cache.writer;

import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StorageWriteBehind implements StorageWriter {
    private static final int NUM_THREADS = 5;
    private final StorageManager dao;
    private final ExecutorService executorService;

    public StorageWriteBehind(StorageManager dao) {
        this(dao, Executors.newFixedThreadPool(NUM_THREADS));
    }

    public StorageWriteBehind(StorageManager dao, ExecutorService executorService) {
        this.dao = dao;
        this.executorService = executorService;
    }

    public void add(Storable storable) {
        executorService.submit(() -> dao.add(storable));
    }

    public void addOrUpdate(Storable storable) {
        executorService.submit(() -> dao.addOrUpdate(storable));
    }

    @Override
    public void update(Storable storable) {
        executorService.submit(() -> dao.update(storable));
    }

    public Object remove(StorableKey key) {
        return executorService.submit(() -> dao.remove(key));
    }
}
