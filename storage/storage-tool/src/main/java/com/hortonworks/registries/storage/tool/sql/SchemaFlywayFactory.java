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

package com.hortonworks.registries.storage.tool.sql;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.internal.util.jdbc.DriverDataSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

public class SchemaFlywayFactory {

    private static final String ENCODING = StandardCharsets.UTF_8.name();
    private static final String META_DATA_TABLE_NAME = "DATABASE_CHANGE_LOG";
    private static final String SQL_MIGRATION_PREFIX = "v";
    private static final boolean OUT_OF_ORDER = false;
    private static final boolean BASELINE_ON_MIGRATE = true;
    private static final String BASELINE_VERSION = "000";
    private static final boolean CLEAN_ON_VALIDATION_ERROR = false;


    static Flyway get(StorageProviderConfiguration conf,
                      String scriptRootPath,
                      boolean validateOnMigrate) {
        switch (conf.getDbType()) {
            case MYSQL:
                return mysqlFlyway(conf, scriptRootPath, validateOnMigrate);
            case POSTGRESQL:
                return postgresqlFlyway(conf, scriptRootPath, validateOnMigrate);
            case ORACLE:
                return oracleFlyway(conf, scriptRootPath, validateOnMigrate);
            default:
                throw new IllegalArgumentException("Unknown database : " + conf.getDbType());
        }
    }

    private static Flyway mysqlFlyway(StorageProviderConfiguration conf,
                                      String scriptRootPath,
                                      boolean validateOnMigrate) {
        Flyway flyway = basicFlyway(conf, scriptRootPath, validateOnMigrate);
        flyway.setDataSource(conf.getUrl(), conf.getUser(), conf.getPassword());
        return flyway;
    }

    private static Flyway postgresqlFlyway(StorageProviderConfiguration conf,
                                           String scriptRootPath,
                                           boolean validateOnMigrate) {
        return mysqlFlyway(conf, scriptRootPath, validateOnMigrate);
    }

    private static Flyway oracleFlyway(StorageProviderConfiguration conf,
                                       String scriptRootPath,
                                       boolean validateOnMigrate) {
        Flyway flyway = basicFlyway(conf, scriptRootPath, validateOnMigrate);
        Map<String, Object> connectionProperties = conf.getConnectionProperties();

        if (connectionProperties != null && !connectionProperties.isEmpty()) {
            Properties properties = new Properties();
            properties.putAll(connectionProperties);
            DriverDataSource dataSource = new DriverDataSource(flyway.getClassLoader(),
                    null, conf.getUrl(), conf.getUser(), conf.getPassword(), properties);
            flyway.setDataSource(dataSource);
        } else {
            flyway.setDataSource(conf.getUrl(), conf.getUser(), conf.getPassword());
        }

        return flyway;
    }

    private static Flyway basicFlyway(StorageProviderConfiguration conf,
                                      String scriptRootPath,
                                      boolean validateOnMigrate) {
        Flyway flyway = new Flyway();

        String location = "filesystem:" + scriptRootPath + File.separator + conf.getDbType();
        flyway.setEncoding(ENCODING);
        flyway.setTable(META_DATA_TABLE_NAME);
        flyway.setSqlMigrationPrefix(SQL_MIGRATION_PREFIX);
        flyway.setValidateOnMigrate(validateOnMigrate);
        flyway.setOutOfOrder(OUT_OF_ORDER);
        flyway.setBaselineOnMigrate(BASELINE_ON_MIGRATE);
        flyway.setBaselineVersion(MigrationVersion.fromVersion(BASELINE_VERSION));
        flyway.setCleanOnValidationError(CLEAN_ON_VALIDATION_ERROR);
        flyway.setLocations(location);

        return flyway;
    }

    private SchemaFlywayFactory() { }
}
