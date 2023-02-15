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

import java.sql.SQLException;

public class H2Connection extends DbConnection {

    private static final String H2_CONNECTION_URL_TEMPLATE = "jdbc:h2:%s:test;MODE=MYSQL;DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;INIT=%s";
    private org.h2.tools.Server h2Server;
    private int h2Port;

    public static DbConnProps getInMemoryConnectionH2DbProperties() {
        DbConnProps props = new DbConnProps(
                DbType.H2,
                "sa",
                "",
                0,
                "localhost",
                "test");

        return props;
    }

    public static String getDbConnectionUrl(boolean withTcpConnection, int h2TcpPort) {
        String ret;
        if (withTcpConnection) {
            ret = String.format(H2_CONNECTION_URL_TEMPLATE, "tcp://localhost:" + h2TcpPort + "/mem", "");
        } else {
            ret = String.format(H2_CONNECTION_URL_TEMPLATE, "mem", "");
        }
        return ret;
    }

    public void openH2TcpPort() throws SQLException {
        h2Server = org.h2.tools.Server.createTcpServer("-tcp", "-tcpAllowOthers");
        h2Server.start();
        h2Port = h2Server.getPort();
    }

    @Override
    public void stop() {
        h2Server.shutdown();
        h2Server.stop();
    }

    @Override
    public void start() {
        try {
            openH2TcpPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DbConnProps getDbConnProps() {
        DbConnProps dbConnProps = getInMemoryConnectionH2DbProperties();

        return new DbConnProps(DbType.H2,
                dbConnProps.getUserName(),
                dbConnProps.getUserPass(),
                h2Port,
                "localhost",
                "test");
    }

}
