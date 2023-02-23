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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresConnection extends DbConnection {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresConnection.class);
    private static final int WAIT_AFTER_DB_PORT_OPENED_MS = 5000;

    private final DbConnProps dbConnProps;

    public PostgresConnection(DbConnProps dbConnProps) {
        this.dbConnProps = dbConnProps;
    }

    public static String getDbConnectionUrl(String hostAddress,
                                            int port,
                                            String dbName) {
        return "jdbc:postgresql://"
                + hostAddress + ":" + Integer.toString(port)
                + "/" + dbName;
    }


    @Override
    public void stop() {

    }

    @Override
    public void start() {
        try {
            // DB needs some time before attached to hikari pool, otherwise
            // org.postgresql.util.PSQLException: FATAL: the database system is starting up
            // or older postgres fails with Caused by: java.io.EOFException
            LOG.info("Starting postgres DB...");
            Thread.sleep(WAIT_AFTER_DB_PORT_OPENED_MS);
        } catch (InterruptedException e) {

        }
    }

    @Override
    public DbConnProps getDbConnProps() {
        return dbConnProps;
    }

    public static void dropAllTables(String schemaName, DataSource dataSource) {
        String queryToDropAllTables = "" +
                "DO $$ DECLARE\n" +
                "    r RECORD;\n" +
                "BEGIN\n" +
                "    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = '" + schemaName + "') LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';\n" +
                "    END LOOP;\n" +
                "END $$;" +
                "";

        try (
                PreparedStatement statement = dataSource
                        .getConnection()
                        .prepareStatement(queryToDropAllTables)
        ) {
            statement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
