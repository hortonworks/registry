/**
 * Copyright 2017-2021 Cloudera, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HikariConfigFactory {

    public static HikariConfig get(DatabaseType type, Map<String, Object> dbProperties) {

        switch (type) {
            case MYSQL:
                return new MySqlConfig(dbProperties);
            case POSTGRESQL:
                return new PostgresConfig(dbProperties);
            case ORACLE:
                return new OracleConfig(dbProperties);
            default:
                throw new IllegalArgumentException("Unsupported storage provider type: " + type);
        }
    }

}

class MySqlConfig extends HikariConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlConfig.class);

    // mysql connector v5.1.48
    static final String DRIVER_5 = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
    // mysql connector v8.0.22
    static final String DRIVER_8 = "com.mysql.cj.jdbc.MysqlDataSource";

    public MySqlConfig(Map<String, Object> dbProperties) {
        super(sanitizeConfig(dbProperties));
    }

    private static Properties sanitizeConfig(Map<String, Object> dbProperties) {
        Properties hikariProperties = new Properties();
        hikariProperties.putAll(dbProperties);

        // check if driver class is found on the classpath
        String className = (String) dbProperties.getOrDefault(Constants.DataSource.CLASS_NAME, DRIVER_5);
        try {
            Class.forName(className);
        } catch (ClassNotFoundException cnfex) {
            // attempt to load the "other" driver class
            if (DRIVER_5.equals(className)) {
                className = DRIVER_8;
            } else {
                className = DRIVER_5;
            }
            try {
                Class.forName(className);
            } catch (ClassNotFoundException fail2) {
                throw new RuntimeException("Could not initialize mysql database connector. " +
                        "JDBC connector classes not found: " + DRIVER_5 + ", " + DRIVER_8);
            }
        }
        hikariProperties.setProperty(Constants.DataSource.CLASS_NAME, className);
        if (!className.equals(dbProperties.get(Constants.DataSource.CLASS_NAME))) {
            LOG.warn("JDBC connector {} was not found on the classpath, falling back to {}. Please fix your configuration.",
                    dbProperties.get(Constants.DataSource.CLASS_NAME), className);
        }

        return hikariProperties;
    }
}

class PostgresConfig extends HikariConfig {

    public PostgresConfig(Map<String, Object> dbProperties) {
        super(sanitizeConfig(dbProperties));
    }

    private static Properties sanitizeConfig(Map<String, Object> dbProperties) {
        Properties hikariProperties = new Properties();
        hikariProperties.putAll(dbProperties);
        return hikariProperties;
    }
}

class OracleConfig extends HikariConfig {

    public OracleConfig(Map<String, Object> dbProperties) {
        super(sanitizeConfig(dbProperties));

        if (dbProperties.containsKey(Constants.DataSource.CONNECTION_PROPERTIES)) {
            Properties properties = new Properties();
            properties.putAll((Map<?, ?>) dbProperties.get(Constants.DataSource.CONNECTION_PROPERTIES));
            this.addDataSourceProperty("connectionProperties", properties);
        }
    }

    private static Properties sanitizeConfig(Map<String, Object> dbProperties) {
        Map<String, Object> propsCopy = new HashMap<>(dbProperties);
        propsCopy.remove(Constants.DataSource.CONNECTION_PROPERTIES);
        Properties hikariProperties = new Properties();
        hikariProperties.putAll(propsCopy);
        return hikariProperties;
    }
}