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

package com.hortonworks.registries.storage.impl.jdbc.mysql;

import com.google.common.cache.CacheBuilder;
import com.hortonworks.registries.storage.impl.jdbc.JdbcStorageManagerIntegrationTest;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.config.HikariBasicConfig;
import com.hortonworks.registries.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.registries.storage.impl.jdbc.provider.mysql.factory.MySqlExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

@Tag("IntegrationTest")
public abstract class AbstractMySqlStorageManagerWithCacheIntegrationTest extends JdbcStorageManagerIntegrationTest {

    public AbstractMySqlStorageManagerWithCacheIntegrationTest() {
        //MySql DB Configuration. Useful for local testing
        //setFields(new HikariCPConnectionBuilder(HikariBasicConfig.getMySqlHikariConfig()), Database.MYSQL);
        //H2 DB Configuration. Useful for testing as part of the build
        setFields(new HikariCPConnectionBuilder(HikariBasicConfig.getH2HikariConfig()), Database.H2);
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
    }

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void setFields(ConnectionBuilder connectionBuilder, Database db) {
        JdbcStorageManagerIntegrationTest.connectionBuilder = connectionBuilder;
        jdbcStorageManager =  createJdbcStorageManager(new MySqlExecutor(new ExecutionConfig(-1), connectionBuilder, newGuavaCacheBuilder()));
        database = db;
    }

    private static CacheBuilder newGuavaCacheBuilder() {
        final long maxSize = 3;
        return  CacheBuilder.newBuilder().maximumSize(maxSize);
    }

}
