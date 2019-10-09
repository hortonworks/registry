/**
 * Copyright 2017-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.registries.storage.impl.jdbc.config;

import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.common.util.Constants;
import com.zaxxer.hikari.HikariConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HikariConfigFactory {

    public static HikariConfig get(DatabaseType type, Map<String, Object> dbProperties) {

        switch (type) {
            case MYSQL:
                return mysqlConfig(dbProperties);
            case POSTGRESQL:
                return postgresqlConfig(dbProperties);
            case ORACLE:
                return oracleConfig(dbProperties);
            default:
                throw new IllegalArgumentException("Unsupported storage provider type: " + type);
        }
    }

    private static HikariConfig mysqlConfig(Map<String, Object> dbProperties) {
        Properties hikariProperties = new Properties();
        hikariProperties.putAll(dbProperties);
        return new HikariConfig(hikariProperties);
    }

    private static HikariConfig postgresqlConfig(Map<String, Object> props) {
        return mysqlConfig(props);
    }

    private static HikariConfig oracleConfig(Map<String, Object> dbProperties) {
        Map<String, Object> propsCopy = new HashMap<>(dbProperties);
        propsCopy.remove(Constants.DataSource.CONNECTION_PROPERTIES);
        HikariConfig hikariConfig = mysqlConfig(propsCopy);
        if (dbProperties.containsKey(Constants.DataSource.CONNECTION_PROPERTIES)) {
            Properties properties = new Properties();
            properties.putAll((Map<?, ?>) dbProperties.get(Constants.DataSource.CONNECTION_PROPERTIES));
            hikariConfig.addDataSourceProperty("connectionProperties", properties);
        }

        return hikariConfig;
    }
}
