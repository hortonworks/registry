/**
 * Copyright 2016-2023 Cloudera, Inc.
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

package com.cloudera.dim.schemaregistry.testcontainers.env.db;

import com.hortonworks.registries.storage.common.DatabaseType;

public enum DbType {
    H2("mysql", DatabaseType.MYSQL, "org.h2.jdbcx.JdbcDataSource"),
    MYSQL5("mysql", DatabaseType.MYSQL, "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"),
    MYSQL8("mysql", DatabaseType.MYSQL, "com.mysql.cj.jdbc.MysqlDataSource"),
    POSTGRES("postgresql", DatabaseType.POSTGRESQL, "org.postgresql.ds.PGSimpleDataSource");

    private final String bootstrapDir;

    private final DatabaseType dbType;

    private final String connectorClassName;

    DbType(String bootstrapDir, DatabaseType dbType, String connectorClassName) {
        this.bootstrapDir = bootstrapDir;
        this.dbType = dbType;
        this.connectorClassName = connectorClassName;
    }

    public String getBootstrapDir() {
        return bootstrapDir;
    }

    public DatabaseType getDbType() {
        return dbType;
    }

    public String getConnectorClassName() {
        return connectorClassName;
    }

}
