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
import com.hortonworks.registries.storage.ConnectionProperties;
import com.hortonworks.registries.storage.DbProperties;
import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.common.util.Constants;
import com.zaxxer.hikari.HikariConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hortonworks.registries.storage.impl.jdbc.config.HikariConfigFactory.putDbProperties;

public class HikariConfigFactory {

    private HikariConfigFactory() { }

    public static HikariConfig get(DatabaseType type, DbProperties dbProperties) {

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

    public static void putDbProperties(Properties hikariProperties, DbProperties dbProperties) {
        hikariProperties.put("dataSourceClassName", dbProperties.getDataSourceClassName());
        hikariProperties.put("dataSource.url", dbProperties.getDataSourceUrl());
        hikariProperties.put("dataSource.user", dbProperties.getDataSourceUser());
        hikariProperties.put("dataSource.password", dbProperties.getDataSourcePassword());
    }

}

class MySqlConfig extends HikariConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlConfig.class);

    // mysql connector v5.1.48
    static final String DRIVER_5 = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
    // mysql connector v8.0.22
    static final String DRIVER_8 = "com.mysql.cj.jdbc.MysqlDataSource";

    public MySqlConfig(DbProperties dbProperties) {
        super(sanitizeConfig(dbProperties));
    }

    private static Properties sanitizeConfig(DbProperties dbProperties) {
        Properties hikariProperties = new Properties();
        checkNotNull(dbProperties);
        putDbProperties(hikariProperties, dbProperties);

        // check if driver class is found on the classpath
        String className = dbProperties.getDataSourceClassName();
        if (StringUtils.isEmpty(className)) {
            className = DRIVER_5;
        }
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
        if (!className.equals(dbProperties.getDataSourceClassName())) {
            LOG.warn("JDBC connector {} was not found on the classpath, falling back to {}. Please fix your configuration.",
                    dbProperties.getDataSourceClassName(), className);
        }

        return hikariProperties;
    }
}

class PostgresConfig extends HikariConfig {

    public PostgresConfig(DbProperties dbProperties) {
        super(sanitizeConfig(dbProperties));
    }

    private static Properties sanitizeConfig(DbProperties dbProperties) {
        Properties hikariProperties = new Properties();
        checkNotNull(dbProperties);
        putDbProperties(hikariProperties, dbProperties);
        return hikariProperties;
    }
}

class OracleConfig extends HikariConfig {

    public OracleConfig(DbProperties dbProperties) {
        super(sanitizeConfig(dbProperties));
        checkNotNull(dbProperties.getConnectionProperties());
        Properties properties = new Properties();
        putConnectionProperties(properties, dbProperties.getConnectionProperties());
        this.addDataSourceProperty("connectionProperties", properties);
    }


    private void putConnectionProperties(Properties properties, ConnectionProperties connectionProperties) {
        checkNotNull(connectionProperties);
        properties.put("oracle.net.ssl_version", connectionProperties.getOracleNetSslVersion());
        properties.put("oracle.net.ssl_server_dn_match", connectionProperties.getOracleNetSslServerDnMatch());
        properties.put("javax.net.ssl.trustStore", connectionProperties.getTrustStore());
        properties.put("javax.net.ssl.trustStoreType", connectionProperties.getTrustStoreType());
        properties.put("javax.net.ssl.keyStore", connectionProperties.getKeyStore());
        properties.put("javax.net.ssl.keyStoreType", connectionProperties.getKeyStoreType());
    }

    private static Properties sanitizeConfig(DbProperties dbProperties) {
        checkNotNull(dbProperties);
        DbProperties propsCopy = dbProperties.createCopyWithoutConnectionProperties(dbProperties);
        Properties hikariProperties = new Properties();
        putDbProperties(hikariProperties, propsCopy);
        return hikariProperties;
    }
}