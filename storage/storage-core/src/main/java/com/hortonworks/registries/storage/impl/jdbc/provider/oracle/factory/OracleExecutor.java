/*
 * Copyright 2017-2021 Cloudera, Inc.
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

package com.hortonworks.registries.storage.impl.jdbc.provider.oracle.factory;


import com.google.common.cache.CacheBuilder;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleAggregateSqlQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleDeleteQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleInsertQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleInsertUpdateDuplicate;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleSelectForShareQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleSelectForUpdateQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleSelectQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleSequenceIdQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.query.OracleUpdateQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.statement.OracleDataTypeContext;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.AbstractQueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.SqlQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import com.hortonworks.registries.storage.impl.jdbc.util.Columns;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;


public class OracleExecutor extends AbstractQueryExecutor {

    private static final OracleDataTypeContext ORACLE_DATA_TYPE_CONTEXT = new OracleDataTypeContext();

    public OracleExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder) {
        super(config, connectionBuilder, ORACLE_DATA_TYPE_CONTEXT);
    }

    public OracleExecutor(ExecutionConfig config, 
                          ConnectionBuilder connectionBuilder, CacheBuilder<SqlQuery, PreparedStatementBuilder> cacheBuilder) {
        super(config, connectionBuilder, cacheBuilder, ORACLE_DATA_TYPE_CONTEXT);
    }


    @Override
    public void insert(Storable storable) {
        executeUpdate(new OracleInsertQuery(storable));
    }

    @Override
    public void insertOrUpdate(final Storable storable) {
        executeUpdate(new OracleInsertUpdateDuplicate(storable));
    }

    @Override
    public int update(Storable storable) {
        return executeUpdate(new OracleUpdateQuery(storable));
    }

    public void delete(StorableKey storableKey) {
        executeUpdate(new OracleDeleteQuery(storableKey));
    }

    @Override
    public <T extends Storable> Collection<T> select(SearchQuery searchQuery) {
        return executeQuery(searchQuery.getNameSpace(), new OracleSelectQuery(searchQuery, 
                storableFactory.create(searchQuery.getNameSpace()).getSchema()));
    }

    @Override
    public <T extends Storable> Collection<T> select(final String namespace) {
        return executeQuery(namespace, new OracleSelectQuery(namespace));
    }

    @Override
    public <T extends Storable> Collection<T> select(final StorableKey storableKey) {
        OracleSelectQuery oracleSelectQuery = new OracleSelectQuery(storableKey);
        try {
            return executeQuery(storableKey.getNameSpace(), oracleSelectQuery);
        } catch (StorageException e) {
            log.error("Error while running query : \"{}\"", oracleSelectQuery.getParametrizedSql(), e);
            throw e;
        }
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace, List<OrderByField> orderByFields) {
        return executeQuery(namespace, new OracleSelectQuery(namespace, orderByFields));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey, List<OrderByField> orderByFields) {
        return executeQuery(storableKey.getNameSpace(), new OracleSelectQuery(storableKey, orderByFields));
    }

    @Override
    public <T extends Storable> Collection<T> selectForShare(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new OracleSelectForShareQuery(storableKey));
    }

    @Override
    public <T extends Storable> Collection<T> selectForUpdate(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new OracleSelectForUpdateQuery(storableKey));
    }

    @Override
    public <T> Optional<T> selectAggregate(String namespace, Schema.Field field, String aggregationFunction) {
        return selectAggregate(namespace, field, new OracleAggregateSqlQuery(namespace, field, aggregationFunction));
    }

    @Override
    public Long nextId(String namespace) {
        OracleSequenceIdQuery oracleSequenceIdQuery = new OracleSequenceIdQuery(namespace, queryTimeoutSecs, ORACLE_DATA_TYPE_CONTEXT);
        Connection connection = null;
        try {
            connection = getConnection();
            Long id = oracleSequenceIdQuery.getNextID(connection);
            return id;
        } finally {
            if (!transactionBookKeeper.hasActiveTransaction(Thread.currentThread().getId())) {
                closeConnection(connection);
            }
        }
    }

    @Override
    public Columns getColumns(String namespace) throws SQLException {
        Columns columns = new Columns();
        Connection connection = null;
        try {
            connection = getConnection();
            final ResultSetMetaData rsMetadata = PreparedStatementBuilder.of(connection, 
                    new ExecutionConfig(queryTimeoutSecs), ORACLE_DATA_TYPE_CONTEXT,
                    new OracleSelectQuery(namespace)).getMetaData();
            for (int i = 1; i <= rsMetadata.getColumnCount(); i++) {
                columns.add(rsMetadata.getColumnName(i),
                        getType(rsMetadata.getColumnType(i), rsMetadata.getPrecision(i)));
            }
            return columns;
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if (!transactionBookKeeper.hasActiveTransaction(Thread.currentThread().getId())) {
                closeConnection(connection);
            }
        }
    }
}
