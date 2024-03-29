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

package com.hortonworks.registries.storage.impl.jdbc.provider.mysql.factory;

import com.google.common.cache.CacheBuilder;
import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.storage.OrderByField;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.exception.StorageException;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlAggregateSqlQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlInsertQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlInsertUpdateDuplicate;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectForShareQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectForUpdateQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MySqlSelectQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.query.MysqlUpdateQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.AbstractQueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.query.SqlQuery;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import com.hortonworks.registries.storage.search.SearchQuery;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * SQL query executor for MySQL DB.
 *
 * To issue the new ID to insert and get auto issued key in concurrent manner, MySqlExecutor utilizes MySQL's
 * auto increment feature and JDBC's getGeneratedKeys() which is described to MySQL connector doc:
 * https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-usagenotes-last-insert-id.html
 *
 * If the value of id is null, we let MySQL issue new ID and get the new ID. If the value of id is not null, we just use that value.
 */
public class MySqlExecutor extends AbstractQueryExecutor {

    /**
     * @param config Object that contains arbitrary configuration that may be needed for any of the steps of the query execution process
     * @param connectionBuilder Object that establishes the connection to the database
     */
    public MySqlExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder) {
        super(config, connectionBuilder);
    }

    /**
     * @param config Object that contains arbitrary configuration that may be needed for any of the steps of the query execution process
     * @param connectionBuilder Object that establishes the connection to the database
     * @param cacheBuilder Guava cache configuration. The maximum number of entries in cache (open connections)
     *                     must not exceed the maximum number of open database connections allowed
     */
    public MySqlExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder, CacheBuilder<SqlQuery, PreparedStatementBuilder> cacheBuilder) {
        super(config, connectionBuilder, cacheBuilder);
    }

    // ============= Public API methods =============

    @Override
    public void insert(Storable storable) {
        insertOrUpdateWithUniqueId(storable, new MySqlInsertQuery(storable));
    }

    @Override
    public void insertOrUpdate(final Storable storable) {
        insertOrUpdateWithUniqueId(storable, new MySqlInsertUpdateDuplicate(storable));
    }

    @Override
    public int update(Storable storable) {
        return executeUpdate(new MysqlUpdateQuery(storable));
    }

    @Override
    public Long nextId(String namespace) {
        // We intentionally return null. Please refer the class javadoc for more details.
        return null;
    }

    @Override
    public <T extends Storable> Collection<T> select(SearchQuery searchQuery) {
        return executeQuery(searchQuery.getNameSpace(), 
                new MySqlSelectQuery(searchQuery, storableFactory.create(searchQuery.getNameSpace()).getSchema()));
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace) {
        return executeQuery(namespace, new MySqlSelectQuery(namespace));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new MySqlSelectQuery(storableKey));
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace, List<OrderByField> orderByFields) {
        return executeQuery(namespace, new MySqlSelectQuery(namespace, orderByFields));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey, List<OrderByField> orderByFields) {
        return executeQuery(storableKey.getNameSpace(), new MySqlSelectQuery(storableKey, orderByFields));
    }

    @Override
    public <T extends Storable> Collection<T> selectForShare(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new MySqlSelectForShareQuery(storableKey));
    }

    @Override
    public <T extends Storable> Collection<T> selectForUpdate(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new MySqlSelectForUpdateQuery(storableKey));
    }

    @Override
    public <T> Optional<T> selectAggregate(String namespace, Schema.Field field, String aggregationFunction) {
        return selectAggregate(namespace, field, new MySqlAggregateSqlQuery(namespace, field, aggregationFunction));
    }

    private void insertOrUpdateWithUniqueId(final Storable storable, final SqlQuery sqlQuery) {
        try {
            Long id = storable.getId();
            if (id == null) {
                id = executeUpdateWithReturningGeneratedKey(sqlQuery);
                storable.setId(id);
            } else {
                executeUpdate(sqlQuery);
            }
        } catch (UnsupportedOperationException e) {
            executeUpdate(sqlQuery);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

}
