/*
 * Copyright 2016-2021 Cloudera, Inc.
 * <p>
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
package com.hortonworks.registries.storage.impl.jdbc;


import com.google.common.base.Stopwatch;
import com.hortonworks.registries.common.QueryParam;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.PrimaryKey;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableFactory;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.exception.AlreadyExistsException;
import com.hortonworks.registries.storage.exception.IllegalQueryParameterException;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.impl.jdbc.provider.QueryExecutorFactory;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.SqlSelectQuery;
import com.hortonworks.registries.storage.impl.jdbc.sequences.NamespaceSequenceStorable;
import com.hortonworks.registries.storage.impl.jdbc.util.Columns;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.hortonworks.registries.storage.impl.jdbc.util.SchemaFields.needsSequence;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

//Use unique constraints on respective columns of a table for handling concurrent inserts etc.
public class JdbcStorageManager implements TransactionManager, StorageManager {
    private static final Logger log = LoggerFactory.getLogger(StorageManager.class);

    private final StorableFactory storableFactory;
    private QueryExecutor queryExecutor;
    private Long offsetMin;
    private Long offsetMax;

    public JdbcStorageManager() {
        storableFactory = new StorableFactory();
    }

    public JdbcStorageManager(QueryExecutor queryExecutor) {
        this(queryExecutor, new StorableFactory());
    }

    public JdbcStorageManager(QueryExecutor queryExecutor, StorableFactory storableFactory) {
        this.storableFactory = storableFactory;
        this.queryExecutor = queryExecutor;
        queryExecutor.setStorableFactory(storableFactory);
        registerStorables(Collections.singleton(NamespaceSequenceStorable.class));
    }

    @Override
    public void add(Storable storable) throws AlreadyExistsException {
        log.debug("Adding storable [{}]", storable);
        queryExecutor.insert(storable);
    }

    @Override
    public <T extends Storable> T remove(StorableKey key) throws StorageException {
        T oldVal = get(key);
        if (key != null) {
            log.debug("Removing storable key [{}]", key);
            queryExecutor.delete(key);
        }
        return oldVal;
    }

    @Override
    public void addOrUpdate(Storable storable) throws StorageException {
        log.debug("Adding or updating storable [{}]", storable);
        queryExecutor.insertOrUpdate(storable);
    }

    @Override
    public void update(Storable storable) {
        queryExecutor.update(storable);
    }

    @Override
    public <T extends Storable> T get(StorableKey key) throws StorageException {
        log.debug("Searching entry for storable key [{}]", key);

        final Collection<T> entries = queryExecutor.select(key);
        T entry = null;
        if (entries.size() > 0) {
            if (entries.size() > 1) {
                log.debug("More than one entry found for storable key [{}]", key);
            }
            entry = entries.iterator().next();
        }
        log.debug("Querying key = [{}]\n\t returned [{}]", key, entry);
        return entry;
    }

    @Override
    public boolean readLock(StorableKey key, Long time, TimeUnit timeUnit) {
        log.debug("Obtaining a read lock for entry with storable key [{}]", key);

        Supplier<Collection<Storable>> supplier = () -> queryExecutor.selectForShare(key);

        try {
            return getLock(supplier, time, timeUnit);
        } catch (InterruptedException e) {
            throw new StorageException("Failed to obtain a write lock for storable key : " + key);
        }
    }

    @Override
    public boolean writeLock(StorableKey key, Long time, TimeUnit timeUnit) {
        log.debug("Obtaining a write lock for entry with storable key [{}]", key);

        Supplier<Collection<Storable>> supplier = () -> queryExecutor.selectForUpdate(key);

        try {
            return getLock(supplier, time, timeUnit);
        } catch (InterruptedException e) {
            throw new StorageException("Failed to obtain a write lock for storable key : " + key);
        }
    }

    private boolean getLock(Supplier<Collection<Storable>> supplier, Long time, TimeUnit timeUnit) throws InterruptedException {
        long remainingTime = MILLISECONDS.convert(time, timeUnit);

        if (remainingTime < 0) {
            throw new IllegalArgumentException("Wait time for obtaining the lock can't be negative");
        }

        long startTime = currentTimeMillis();
        do {
            Collection<Storable> storables = supplier.get();
            if (storables != null && !storables.isEmpty()) {
                return true;
            } else {
                Thread.sleep(500);
            }
        } while ((currentTimeMillis() - startTime) < remainingTime);


        return false;
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace, List<QueryParam> queryParams)
            throws StorageException {
        log.debug("Searching for entries in table [{}] that match queryParams [{}]", namespace, queryParams);

        return find(namespace, queryParams, Collections.emptyList());
    }

    @Override
    public <T extends Storable> Collection<T> find(String namespace,
                                                   List<QueryParam> queryParams,
                                                   List<OrderByField> orderByFields) throws StorageException {

        log.debug("Searching for entries in table [{}] that match queryParams [{}] and order by [{}]", namespace, queryParams, orderByFields);

        if (queryParams == null || queryParams.isEmpty()) {
            return list(namespace, orderByFields);
        }

        Collection<T> entries = Collections.emptyList();
        try {
            StorableKey storableKey = buildStorableKey(namespace, queryParams);
            if (storableKey != null) {
                entries = queryExecutor.select(storableKey, orderByFields);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }

        log.debug("Querying table = [{}]\n\t filter = [{}]\n\t returned [{}]", namespace, queryParams, entries);

        return entries;
    }

    @Override
    public <T extends Storable> Collection<T> search(SearchQuery searchQuery) {
        return queryExecutor.select(searchQuery);
    }

    private <T extends Storable> Collection<T> list(String namespace, List<OrderByField> orderByFields) {
        log.debug("Listing entries for table [{}]", namespace);
        final Collection<T> entries = queryExecutor.select(namespace, orderByFields);
        log.debug("Querying table = [{}]\n\t returned [{}]", namespace, entries);
        return entries;
    }

    @Override
    public <T extends Storable> Collection<T> list(String namespace) throws StorageException {
        return list(namespace, Collections.emptyList());
    }

    @Override
    public void cleanup() throws StorageException {
        queryExecutor.cleanup();
    }

    private boolean belowMaxOffset(NamespaceSequenceStorable sequence) {
        return offsetMax == null || sequence.getNextId() < offsetMax;
    }

    @Override
    public final Long nextId(String namespace) {
        log.debug("Finding nextId for table [{}]", namespace);
        StorableKey keyForNamespace = new NamespaceSequenceStorable(namespace).getStorableKey();
        Stopwatch stopwatch = Stopwatch.createStarted();
        // tries to lock the row in the sequence table and updates with the sequence value incremented by 1
        if (writeLock(keyForNamespace, 3L, SECONDS)) {
            log.debug("Locked sequence row for namespace {} in {}ms", namespace, stopwatch.elapsed(MILLISECONDS));
            NamespaceSequenceStorable currentSequence = get(keyForNamespace);
            if (currentSequence != null) {
                NamespaceSequenceStorable incremented = currentSequence.increment();
                checkState(belowMaxOffset(incremented), "Sequence for namespace %s cannot go above max offset %s", namespace, offsetMax);
                update(incremented);

                if (offsetMax != null && incremented.getNextId() > offsetMax * 0.8) {
                    log.warn("Sequence value {} for namespace {} is getting close to offset max value {}", incremented.getNextId(), namespace, offsetMax);
                }
                return currentSequence.getNextId();
            } else {
                throw new IllegalStateException("Could not get the sequence after being locked: " + keyForNamespace);
            }
        } else {
            // Error when locking the sequence row is not successful unless it's not initialized yet, then initialize it.
            // (we should not see uninitialized sequences since they are initialized in the #initializeSequences method)
            if (get(keyForNamespace) == null) {
                add(new NamespaceSequenceStorable(namespace, 2L));
                log.debug("Added new sequence row for namespace {} in {}ms", namespace, stopwatch.elapsed(MILLISECONDS));
                return 1L;
            } else {
                throw new IllegalStateException("Could not lock sequence row for namespace: " + keyForNamespace);
            }
        }
    }

    @Override
    public void registerStorables(Collection<Class<? extends Storable>> classes) throws StorageException {
        storableFactory.addStorableClasses(classes);
        initializeSequences(classes);
    }

    private boolean sequenceNeedsToBeOffseted(NamespaceSequenceStorable sequence) {
        return offsetMin != null && sequence.getNextId() < offsetMin;
    }

    private long selectMaxId(Storable storable) throws SQLException {
        SelectMaxIdQuery selectMaxIdQuery = new SelectMaxIdQuery(storable);
        try (
                PreparedStatement selectMaxStatement = selectMaxIdQuery.prepareStatement(queryExecutor);
                ResultSet rs = selectMaxStatement.executeQuery()
        ) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    private NamespaceSequenceStorable withOffset(NamespaceSequenceStorable sequence) {
        long minVal = offsetMin != null ? offsetMin : 1;
        long nextVal = Math.max(sequence.getNextId(), minVal);
        sequence.setNextId(nextVal);
        return sequence;
    }

    private void initializeSequences(Collection<Class<? extends Storable>> classes) {
        for (Class<? extends Storable> storableClass : classes) {
            try {
                Storable storable = storableClass.newInstance();

                if (!needsSequence(storable)) {
                    continue;
                }

                NamespaceSequenceStorable sequenceStorable = new NamespaceSequenceStorable(storable.getNameSpace());
                NamespaceSequenceStorable existingSequence = get(sequenceStorable.getStorableKey());
                if (existingSequence != null) {
                    if (sequenceNeedsToBeOffseted(existingSequence)) {
                        NamespaceSequenceStorable withOffset = withOffset(sequenceStorable);
                        log.debug("Increasing sequence for namespace {} from existing value {} to {}", storable.getNameSpace(), existingSequence.getNextId(), withOffset.getNextId());
                        update(withOffset);
                    } else {
                        log.debug("Existing sequence value for namespace {} is {}", storable.getNameSpace(), existingSequence.getNextId());
                        continue;
                    }
                } else {
                    sequenceStorable.setNextId(selectMaxId(storable) + 1);
                    NamespaceSequenceStorable newSequence = withOffset(sequenceStorable);
                    log.debug("Initializing sequence for namespace {} to {}", storable.getNameSpace(), newSequence.getNextId());
                    add(newSequence);
                }
            } catch (InstantiationException | IllegalAccessException | SQLException e) {
                log.error("Cannot initialize sequence for storable " + storableClass.getSimpleName(), e);
                throw new StorageException("Cannot initialize sequence for storable " + storableClass.getSimpleName(), e);
            }
        }
    }

    // private helper methods

    /**
     * Query parameters are typically specified for a column or key in a database table or storage namespace. Therefore, we build
     * the {@link StorableKey} from the list of query parameters, and then can use {@link SqlSelectQuery} builder to generate the query using
     * the query parameters in the where clause
     *
     * @return {@link StorableKey} with all query parameters that match database columns <br/>
     * null if none of the query parameters specified matches a column in the DB
     */
    private StorableKey buildStorableKey(String namespace, List<QueryParam> queryParams) {
        final Map<Schema.Field, Object> fieldsToVal = new HashMap<>();
        StorableKey storableKey = null;

        try {
            Columns columns = queryExecutor.getColumns(namespace);
            for (QueryParam qp : queryParams) {
                Schema.Type type = columns.getType(qp.getName());
                if (type == null) {
                    log.warn("Query parameter [{}] does not exist for namespace [{}]. Query parameter ignored.", qp.getName(), namespace);
                } else {
                    fieldsToVal.put(new Schema.Field(qp.getName(), type),
                            type.getJavaType().getConstructor(String.class).newInstance(qp.getValue()));
                }
            }

            // it is empty when none of the query parameters specified matches a column in the DB
            if (!fieldsToVal.isEmpty()) {
                final PrimaryKey primaryKey = new PrimaryKey(fieldsToVal);
                storableKey = new StorableKey(namespace, primaryKey);
            }

            log.debug("Building StorableKey from QueryParam: \n\tnamespace = [{}]\n\t queryParams = [{}]\n\t StorableKey = [{}]",
                    namespace, queryParams, storableKey);
        } catch (Exception e) {
            log.debug("Exception occurred when attempting to generate StorableKey from QueryParam", e);
            throw new IllegalQueryParameterException(e);
        }

        return storableKey;
    }

    /**
     * Initializes this instance with {@link QueryExecutor} created from the given {@code properties}.
     * Some of these properties are jdbcDriverClass, jdbcUrl, queryTimeoutInSecs.
     *
     * @param configuration properties with name/value pairs
     */
    @Override
    public void init(StorageProviderConfiguration configuration) {

        if (StringUtils.isBlank(configuration.getProperties().getDbtype())) {
            throw new IllegalArgumentException("db.type should be set on jdbc properties");
        }

        DatabaseType type = DatabaseType.fromValue(configuration.getProperties().getDbtype());
        log.info("jdbc provider type: [{}]", type);

        this.queryExecutor = QueryExecutorFactory.get(type, configuration);
        this.queryExecutor.setStorableFactory(storableFactory);

        if (configuration.getProperties().getOffsetRange() != null) {
            this.offsetMin = configuration.getProperties().getOffsetRange().getMin();
            this.offsetMax = configuration.getProperties().getOffsetRange().getMax();
        }
    }

    @Override
    public void beginTransaction(TransactionIsolation transactionIsolationLevel) {
        queryExecutor.beginTransaction(transactionIsolationLevel);
    }

    @Override
    public void rollbackTransaction() {
        // The guarantee in interface is accomplished once the instance has an implementation of
        // QueryExecutor which extends AbstractQueryExecutor,
        // given that AbstractQueryExecutor.rollbackTransaction() guarantees the behavior.

        // Another implementations of QueryExecutor should provide a way of guaranteeing the
        // behavior, like call closeConnection() when rollbackTransaction() is failing.
        queryExecutor.rollbackTransaction();
    }

    @Override
    public void commitTransaction() {
        queryExecutor.commitTransaction();
    }
}
