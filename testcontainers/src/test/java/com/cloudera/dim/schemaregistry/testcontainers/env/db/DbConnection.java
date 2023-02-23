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

public abstract class DbConnection {
    public abstract void stop();

    public abstract void start();

    public abstract DbConnProps getDbConnProps();

    public static String getConnectionUrl(DbType dbType,
                                          String hostAddress,
                                          int port,
                                          String dbName) {
        String ret;
        switch (dbType) {
            case MYSQL5:
            case MYSQL8:
                ret = MysqlConnection.getDbConnectionUrl(hostAddress, port, dbName);
                break;
            case POSTGRES:
                ret = PostgresConnection.getDbConnectionUrl(hostAddress, port, dbName);
                break;
            case H2:
                ret = H2Connection.getDbConnectionUrl(true, port);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return ret;
    }
}
