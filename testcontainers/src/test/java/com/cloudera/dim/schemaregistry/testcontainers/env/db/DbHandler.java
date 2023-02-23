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

import com.cloudera.dim.schemaregistry.TestUtils;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

public class DbHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DbHandler.class);

    private Flyway flyway;

    @Getter
    private DataSource dataSource;

    private DbConnection dbConnection;


    public DbConnProps startDatabase(DbConnProps dbConnProps) {
        LOG.debug("Setting up " + dbConnProps.getDbType().toString() + " database ...");

        switch (dbConnProps.getDbType()) {
            case H2:
                // start in-memory H2 database
                dbConnection = new H2Connection();
                dbConnection.start();
                dataSource = prepareConnectionPool(dbConnection);
                break;
            case MYSQL5:
            case MYSQL8:
                dbConnection = new MysqlConnection(dbConnProps);
                dbConnection.start();
                dataSource = prepareConnectionPool(dbConnection);
                break;
            case POSTGRES:
                dbConnection = new PostgresConnection(dbConnProps);
                dbConnection.start();
                dataSource = prepareConnectionPool(dbConnection);
                break;
            default:
                throw new UnsupportedOperationException("not known type");
        }

        return dbConnection.getDbConnProps();
    }


    public DataSource prepareConnectionPool(DbConnection dbConn) {
        DbConnProps dbProps = dbConn.getDbConnProps();

        HikariConfig hikariConfig;

        switch (dbProps.getDbType()) {
            case H2:
                DbConnProps inMemoryConnectionH2DbProperties = H2Connection.getInMemoryConnectionH2DbProperties();
                hikariConfig = new HikariConfig();
                hikariConfig.setDriverClassName(DbType.H2.getConnectorClassName());
                hikariConfig.setJdbcUrl(H2Connection.getDbConnectionUrl(false, 0));
                hikariConfig.setUsername(inMemoryConnectionH2DbProperties.getUserName());
                hikariConfig.setPassword(inMemoryConnectionH2DbProperties.getUserPass());
                setCommonHikariConfigs(hikariConfig);
                break;
            case POSTGRES:
            case MYSQL5:
            case MYSQL8:
                Properties props = new Properties();
                props.setProperty("dataSourceClassName", dbProps.getDbType().getConnectorClassName());
                props.setProperty("dataSource.user", dbProps.getUserName());
                props.setProperty("dataSource.password", dbProps.getUserPass());
                props.setProperty("dataSource.databaseName", dbProps.getDbName());
                props.setProperty("dataSource.portNumber",
                        Integer.toString(dbProps.getDbConnectionPortForTest()));
                props.setProperty("dataSource.serverName", dbProps.getDbHostAddressForTest());
                props.put("dataSource.logWriter", new PrintWriter(System.out));

                hikariConfig = new HikariConfig(props);
                setCommonHikariConfigs(hikariConfig);
                break;
            default:
                throw new UnsupportedOperationException("not impl yet");

        }
        return new HikariDataSource(hikariConfig);
    }

    public void stop() {
        dbConnection.stop();
    }


    public void resetDatabase(DbConnProps dbConnProps, String tempDirPath) {
        LOG.info("Resetting the database ...");
        try {
            flyway = getFlyway(dbConnProps, getFinalBootstrapDir(dbConnProps.getDbType(),
                    TestUtils.getPathToBootstrap(dbConnProps.getDbType().getBootstrapDir()),
                    tempDirPath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (dbConnection.getDbConnProps().getDbType() == DbType.POSTGRES) {
            //flyway.clean() does not work for postgres versions above 10
            PostgresConnection.dropAllTables("public", dataSource);
        } else {
            flyway.clean();
        }
        flyway.migrate();
    }

    public void populateDatabase(DbConnProps dbConnProps, String tempDirPath) {
        try {
            flyway = getFlyway(dbConnProps, getFinalBootstrapDir(dbConnProps.getDbType(),
                    TestUtils.getPathToBootstrap(dbConnProps.getDbType().getBootstrapDir()),
                    tempDirPath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        flyway.migrate();
    }

    private void setCommonHikariConfigs(HikariConfig hikariConfig) {
        hikariConfig.setAutoCommit(true);
        hikariConfig.setConnectionInitSql("");
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
    }

    private String getFinalBootstrapDir(DbType dbType, File bootstrapDir, String tempDirPath) {
        String finalBootstrapDir;
        if (dbType.equals(DbType.H2)) {
            try {
                finalBootstrapDir = TestUtils.preprocessMigrationsForH2(bootstrapDir, tempDirPath).getAbsolutePath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            finalBootstrapDir = bootstrapDir.getAbsolutePath();
        }
        return finalBootstrapDir;
    }

    private Flyway getFlyway(DbConnProps dbConnProps, String location) {
        Flyway flyway = new Flyway();

        flyway.setEncoding("UTF-8");
        flyway.setTable("SCRIPT_CHANGE_LOG");
        flyway.setValidateOnMigrate(true);
        flyway.setOutOfOrder(false);
        flyway.setBaselineOnMigrate(true);
        flyway.setBaselineVersion(MigrationVersion.fromVersion("000"));
        flyway.setCleanOnValidationError(false);
        flyway.setLocations("filesystem:" + location);
        flyway.setSqlMigrationPrefix("v");
        flyway.setDataSource(
                DbConnection.getConnectionUrl(dbConnProps.getDbType(),
                        dbConnProps.getDbHostAddressForTest(),
                        dbConnProps.getDbConnectionPortForTest(),
                        dbConnProps.getDbName()),
                dbConnProps.getUserName(),
                dbConnProps.getUserPass());

        return flyway;
    }

}
