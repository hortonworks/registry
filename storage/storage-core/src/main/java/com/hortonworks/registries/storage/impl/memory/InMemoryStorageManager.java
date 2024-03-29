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
package com.hortonworks.registries.storage.impl.memory;


import com.google.common.collect.Lists;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.util.ReflectionHelper;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.exception.AlreadyExistsException;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.search.Predicate;
import com.hortonworks.registries.storage.search.PredicateCombinerPair;
import com.hortonworks.registries.storage.search.SearchQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

//TODO: The synchronization is broken right now, so all the methods don't guarantee the semantics as described in the interface.
public class InMemoryStorageManager implements StorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryStorageManager.class);

    private final ConcurrentHashMap<String, ConcurrentHashMap<PrimaryKey, Storable>> storageMap = 
            new ConcurrentHashMap<String, ConcurrentHashMap<PrimaryKey, Storable>>();
    private final ConcurrentHashMap<String, AtomicLong> sequenceMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Class<?>> nameSpaceClassMap = new ConcurrentHashMap<String, Class<?>>();

    @Override
    public void init(StorageProviderConfiguration properties) {

    }

    @Override
    public void add(Storable storable) throws AlreadyExistsException {
        final Storable existing = get(storable.getStorableKey());

        if (existing == null) {
            addOrUpdate(storable);
        } else if (!existing.equals(storable)) {
            throw new AlreadyExistsException("Another instance with same id = " + storable.getPrimaryKey()
                    + " exists with different value in namespace " + storable.getNameSpace()
                    + " Consider using addOrUpdate method if you always want to overwrite.");
        }
    }

    @Override
    public <T extends Storable> T remove(StorableKey key) throws StorageException {
        if (storageMap.containsKey(key.getNameSpace())) {
            return (T) storageMap.get(key.getNameSpace()).remove(key.getPrimaryKey());
        }
        return null;
    }

    @Override
    public void addOrUpdate(Storable storable) {
        String namespace = storable.getNameSpace();
        PrimaryKey id = storable.getPrimaryKey();
        if (!storageMap.containsKey(namespace)) {
            storageMap.putIfAbsent(namespace, new ConcurrentHashMap<PrimaryKey, Storable>());
            nameSpaceClassMap.putIfAbsent(namespace, storable.getClass());
        }
        if (!storageMap.get(namespace).containsKey(id)) {
            nextId(namespace);
        }
        storageMap.get(namespace).put(id, storable);
    }

    @Override
    public void update(Storable storable) {
        String namespace = storable.getNameSpace();
        PrimaryKey pk = storable.getPrimaryKey();
        if (!storageMap.containsKey(namespace)) {
            throw new StorageException("Row could not be updated");
        }
        storageMap.get(namespace).put(pk, storable);
    }

    @Override
    public <T extends Storable> T get(StorableKey key) throws StorageException {
        return storageMap.containsKey(key.getNameSpace())
                ? (T) storageMap.get(key.getNameSpace()).get(key.getPrimaryKey())
                : null;
    }

    /**
     * Uses reflection to query the field or the method. Assumes
     * a public getXXX method is available to get the field value.
     */
    private boolean matches(Storable val, List<QueryParam> queryParams) {
        Object fieldValue;
        boolean res = true;
            for (QueryParam qp : queryParams) {
                try {
                    fieldValue = ReflectionHelper.invokeGetter(qp.name, val);
                    if (!fieldValue.toString().equals(qp.value)) {
                        return false;
                    }
                } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                    LOG.error("FAILED to invoke getter for query param {} , is your param name correct?", qp.getName(), e);
                    return false;
                }
            }
        return res;
    }

    public <T extends Storable> Collection<T> find(final String namespace,
                                                   final List<QueryParam> queryParams) throws StorageException {
        return find(namespace, queryParams, Collections.emptyList());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Storable> Collection<T> find(final String namespace,
                                                   final List<QueryParam> queryParams,
                                                   final List<OrderByField> orderByFields) throws StorageException {

        List<T> storables = new ArrayList<>();
        if (queryParams == null) {
            Collection<T> collection = list(namespace);
            storables = Lists.newArrayList(collection);
        } else {
            Class<?> clazz = nameSpaceClassMap.get(namespace);
            if (clazz != null) {
                Map<PrimaryKey, Storable> storableMap = storageMap.get(namespace);
                if (storableMap != null) {
                    for (Storable val : storableMap.values()) {
                        if (matches(val, queryParams)) {
                            storables.add((T) val);
                        }
                    }
                }
            }
        }

        if (orderByFields != null && !orderByFields.isEmpty()) {
            storables.sort((storable1, storable2) -> {
                try {
                    for (OrderByField orderByField : orderByFields) {
                        Comparable value1 = ReflectionHelper.invokeGetter(orderByField.getFieldName(), storable1);
                        Comparable value2 = ReflectionHelper.invokeGetter(orderByField.getFieldName(), storable2);
                        int compareTo;
                        // same values continue
                        if (value1 == value2) {
                            continue;
                        } else if (value1 == null) {
                            // value2 is non null
                            compareTo = -1;
                        } else if (value2 == null) {
                            // value1 is non null
                            compareTo = 1;
                        } else {
                            // both value and value2 non null
                            compareTo = value1.compareTo(value2);
                        }

                        if (compareTo == 0) {
                            continue;
                        }
                        return orderByField.isDescending() ? -compareTo : compareTo;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                // all group by fields are matched means equal
                return 0;
            });
        }

        return storables;
    }

    @Override
    public <T extends Storable> Collection<T> search(SearchQuery searchQuery) {
        LOG.warn("This storage manager does not support search query in a meaningful way. Do not use it in production! Returning instances with the given namespace [{}]", searchQuery.getNameSpace());
        Collection<T> list = new ArrayList<>(list(searchQuery.getNameSpace()));
        Map<Storable, Map<String, Object>> pairs = new HashMap<>();
        for (Storable storable : list(searchQuery.getNameSpace())) {
            pairs.put(storable, storable.toMap());
        }

        Collection<T> result = new HashSet<>(list.size());
        for (PredicateCombinerPair predicateCombinerPair : searchQuery.getWhereClause().getPredicateCombinerPairs()) {
            for (T storable : list) {
                Map<String, Object> map = pairs.get(storable);
                final Predicate.Operation op = predicateCombinerPair.getPredicate().getOperation();
                switch (op) {
                    case EQ: case CONTAINS:
                        if (map.containsKey(predicateCombinerPair.getPredicate().getField())) {
                            Object value = map.get(predicateCombinerPair.getPredicate().getField());
                            if (value == null) {
                                continue;
                            } else if (op == Predicate.Operation.EQ && Objects.equals(value, predicateCombinerPair.getPredicate().getValue())) {
                                result.add(storable);
                            } else if (op == Predicate.Operation.CONTAINS && value.toString().toLowerCase().contains(predicateCombinerPair.getPredicate().getValue().toString().toLowerCase())) {
                                result.add(storable);
                            }
                        }
                        break;
                    default:
                        result.add(storable);
                        break;
                }
            }
        }

        return result;
    }

    @Override
    public <T extends Storable> Collection<T> list(String namespace) throws StorageException {
        return storageMap.containsKey(namespace)
                ? (Collection<T>) storageMap.get(namespace).values() : Collections.<T>emptyList();
    }

    @Override
    public void cleanup() throws StorageException {
        //no-op
    }

    /**
     * atomically increment and return the next id for the given namespace
     */
    @Override
    public Long nextId(String namespace) {
        AtomicLong cur = sequenceMap.get(namespace);
        if (cur == null) {
            AtomicLong zero = new AtomicLong();
            cur = sequenceMap.putIfAbsent(namespace, zero);
            if (cur == null) {
                cur = zero;
            }
        }
        return cur.incrementAndGet();
    }

    @Override
    public void registerStorables(Collection<Class<? extends Storable>> classes) throws StorageException {
    }

}
