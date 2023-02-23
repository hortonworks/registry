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

public class MysqlConnection extends DbConnection {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlConnection.class);
    private static final int WAIT_AFTER_DB_PORT_OPENED_MS = 7000;

    private final DbConnProps dbConnProps;

    public MysqlConnection(DbConnProps dbConnProps) {
        this.dbConnProps = dbConnProps;
    }

    @Override
    public void start() {
        try {
            LOG.info("Starting MySql DB...");
            // DB needs some time before attached to hikari pool, otherwise error can happen like
            // HikariPool$PoolInitializationException: Failed to initialize pool: Communications link failure
            // The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received
            // any packets from the server.
            Thread.sleep(WAIT_AFTER_DB_PORT_OPENED_MS);
        } catch (InterruptedException e) {

        }
    }

    @Override
    public void stop() {

    }

    @Override
    public DbConnProps getDbConnProps() {
        return dbConnProps;
    }

    public static String getDbConnectionUrl(String hostAddress,
                                            int port,
                                            String dbName) {
        return "jdbc:mysql://"
                + hostAddress + ":" + Integer.toString(port)
                + "/" + dbName;
    }
}
