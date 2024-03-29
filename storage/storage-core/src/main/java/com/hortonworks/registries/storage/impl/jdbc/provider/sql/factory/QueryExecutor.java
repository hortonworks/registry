/*
 * Copyright 2016-2021 Cloudera, Inc.
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

package com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableFactory;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.exception.NonIncrementalColumnException;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.util.Columns;
import com.hortonworks.registries.storage.search.SearchQuery;
import com.hortonworks.registries.storage.transaction.TransactionIsolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Exposes CRUD and other useful operations to the persistence storage
 */
public interface QueryExecutor {
    Logger log = LoggerFactory.getLogger(QueryExecutor.class);

    /**
     * Inserts the specified {@link Storable} in storage.
     */
    void insert(Storable storable);

    /**
     * Inserts or updates the specified {@link Storable} in storage
     */
    void insertOrUpdate(Storable storable);

    /**
     * Updates the specified storable in the storage
     *
     * @return the number of rows updated
     */
    int update(Storable storable);

    /**
     * Deletes the specified {@link StorableKey} from storage
     */
    void delete(StorableKey storableKey);

    /**
     * @return all entries in the given namespace
     */
    <T extends Storable> Collection<T> select(String namespace);

    <T extends Storable> Collection<T> select(String namespace, List<OrderByField> orderByFields);

    /**
     * @return all entries that match the specified {@link StorableKey}
     */
    <T extends Storable> Collection<T> select(StorableKey storableKey);

    <T extends Storable> Collection<T> select(StorableKey storableKey, List<OrderByField> orderByFields);

    /**
     *
     * @param namespace is the namespace to select from
     * @param field is the {@link Storable#getSchema()} field corresponding to the database column
     * @param aggregationFunction the standard SQL function with which the aggregation is done
     * @return field values aggregated with the aggregationFunction over the namespace
     * @throws ClassCastException if the field's type does not correspond to the type T
     */
    <T> Optional<T> selectAggregate(String namespace, Schema.Field field, String aggregationFunction);

    /**
     * @return The next available id for the autoincrement column in the specified {@code namespace}
     * @exception NonIncrementalColumnException if {@code namespace} has no autoincrement column
     *
     */
    Long nextId(String namespace);

    /**
     * @return an open connection to the underlying storage
     */
    Connection getConnection();

    void closeConnection(Connection connection);

    /**
     * cleanup
     */
    void cleanup();

    ExecutionConfig getConfig();

    void setStorableFactory(StorableFactory storableFactory);

    //todo unify all other select methods with this method as they are kind of special cases of SearchQuery
    <T extends Storable> Collection<T> select(SearchQuery searchQuery);

    /**
     *  @return returns set of columns for a given table
     */
    Columns getColumns(String namespace) throws SQLException;

    /**
     *  Begins the transaction
     */
    void beginTransaction(TransactionIsolation transactionIsolationLevel);


    /**
     *  Discards the changes made to the storage layer and reverts to the last committed point
     */
    void rollbackTransaction();


    /**
     *  Flushes the changes made to the storage layer
     */
    void commitTransaction();

    /**
     * @return all entries that match the specified {@link StorableKey} with share lock
     */
    <T extends Storable> Collection<T> selectForShare(StorableKey storableKey);

    /**
     * @return all entries that match the specified {@link StorableKey} with update lock
     */
    <T extends Storable> Collection<T> selectForUpdate(StorableKey storableKey);

}
