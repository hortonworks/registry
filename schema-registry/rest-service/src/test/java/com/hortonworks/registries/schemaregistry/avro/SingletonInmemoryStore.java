/*
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
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This internally uses a single instance in a JVM, which can be used across multiple in-JVM local registry instances({@link com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer}).
 *
 */
public class SingletonInmemoryStore implements StorageManager {

    private static final InMemoryStorageManager inMemoryStorageManager = new InMemoryStorageManager();

    @Override
    public void init(Map<String, Object> properties) {
        inMemoryStorageManager.init(properties);
    }

    @Override
    public void add(Storable storable) throws StorageException {
        inMemoryStorageManager.add(storable);
    }

    @Override
    public <T extends Storable> T remove(StorableKey key) throws StorageException {
        return inMemoryStorageManager.remove(key);
    }

    @Override
    public void addOrUpdate(Storable storable) throws StorageException {
        inMemoryStorageManager.addOrUpdate(storable);
    }

    @Override
    public void update(Storable storable) {
        inMemoryStorageManager.update(storable);
    }

    @Override
    public <T extends Storable> T get(StorableKey key) throws StorageException {
        return inMemoryStorageManager.get(key);
    }

    @Override
    public boolean exists(StorableKey key) {
        return inMemoryStorageManager.exists(key);
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace, List<QueryParam> queryParams) throws StorageException {
        return inMemoryStorageManager.find(namespace, queryParams);
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace, List<QueryParam> queryParams, List<OrderByField> orderByFields) throws StorageException {
        return inMemoryStorageManager.find(namespace, queryParams, orderByFields);
    }

    @Override
    public <T extends Storable> Collection<T> search(SearchQuery searchQuery) {
        return inMemoryStorageManager.search(searchQuery);
    }

    @Override
    public <T extends Storable> Collection<T> list(String namespace) throws StorageException {
        return inMemoryStorageManager.list(namespace);
    }

    @Override
    public void cleanup() throws StorageException {
        inMemoryStorageManager.cleanup();
    }

    @Override
    public Long nextId(String namespace) throws StorageException {
        return inMemoryStorageManager.nextId(namespace);
    }

    @Override
    public void registerStorables(Collection<Class<? extends Storable>> classes) throws StorageException {
        inMemoryStorageManager.registerStorables(classes);
    }
}
