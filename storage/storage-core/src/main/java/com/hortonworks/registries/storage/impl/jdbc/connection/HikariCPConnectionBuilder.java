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
package com.hortonworks.registries.storage.impl.jdbc.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class HikariCPConnectionBuilder implements ConnectionBuilder<HikariConfig> {
    private Map<String, Object> configMap;
    private transient HikariDataSource dataSource;
    private HikariConfig config;

    public HikariCPConnectionBuilder(Map<String, Object> hikariCPConfig) {
        this.configMap = hikariCPConfig;
        prepare();
    }

    public HikariCPConnectionBuilder(HikariConfig hikariConfig) {
        this.config = hikariConfig;
        prepare();
    }

    @Override
    public synchronized void prepare() {
        if (dataSource == null) {
            if (configMap != null) {
                Properties properties = new Properties();
                properties.putAll(configMap);
                config = new HikariConfig(properties);
            }
            this.dataSource = new HikariDataSource(config);
        }
    }

    @Override
    public Connection getConnection() {
        try {
            return this.dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HikariConfig getConfig() {
        return config;
    }

    @Override
    public void cleanup() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
