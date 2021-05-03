/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.storage.impl.jdbc.provider;

import com.google.common.collect.Lists;
import com.hortonworks.registries.storage.DbProperties;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.common.util.Constants;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.config.HikariConfigFactory;
import com.hortonworks.registries.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.factory.MySqlExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.oracle.factory.OracleExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.postgresql.factory.PostgresqlExecutor;
import com.hortonworks.registries.storage.impl.jdbc.provider.sql.factory.QueryExecutor;
import com.hortonworks.registries.storage.impl.jdbc.util.Util;
import com.zaxxer.hikari.HikariConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExecutorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QueryExecutorFactory.class);

    private QueryExecutorFactory() {

    }

    public static QueryExecutor get(DatabaseType type, StorageProviderConfiguration storageConfig) {

        HikariCPConnectionBuilder connectionBuilder = getHikariCPConnnectionBuilder(type, storageConfig);
        ExecutionConfig executionConfig = getExecutionConfig(type, storageConfig);

        QueryExecutor queryExecutor = null;
        switch (type) {
            case MYSQL:
                queryExecutor = new MySqlExecutor(executionConfig, connectionBuilder);
                break;
            case POSTGRESQL:
                queryExecutor = new PostgresqlExecutor(executionConfig, connectionBuilder);
                break;
            case ORACLE:
                queryExecutor = new OracleExecutor(executionConfig, connectionBuilder);
                break;
            default:
                throw new IllegalArgumentException("Unsupported storage provider type: " + type);
        }

        return queryExecutor;
    }

    private static HikariCPConnectionBuilder getHikariCPConnnectionBuilder(DatabaseType type, StorageProviderConfiguration storageConfig) {
        checkNotNull(storageConfig.getProperties().getProperties());
        DbProperties dbProperties = storageConfig.getProperties().getProperties();
        Util.validateJDBCProperties(dbProperties, Lists.newArrayList(Constants.DataSource.CLASS_NAME, Constants.DataSource.URL));

        if (LOG.isDebugEnabled()) {
            String dataSourceClassName = dbProperties.getDataSourceClassName();
            LOG.debug("data source class: [{}]", dataSourceClassName);

            String jdbcUrl = dbProperties.getDataSourceUrl();
            LOG.debug("dataSource.url is: [{}] ", jdbcUrl);
        }

        HikariConfig hikariConfig = HikariConfigFactory.get(type, dbProperties);

        return new HikariCPConnectionBuilder(hikariConfig);
    }

    private static ExecutionConfig getExecutionConfig(DatabaseType type, StorageProviderConfiguration storageConfig) {
        int queryTimeOutInSecs = -1;
        if (storageConfig.getProperties().getQueryTimeoutInSecs() != null) {
            queryTimeOutInSecs =  storageConfig.getProperties().getQueryTimeoutInSecs();
            if (queryTimeOutInSecs < 0) {
                throw new IllegalArgumentException("queryTimeoutInSecs property can not be negative");
            }
        }

        return new ExecutionConfig(queryTimeOutInSecs, type);
    }
}
