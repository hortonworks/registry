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

import com.hortonworks.registries.storage.DbProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers.TestcontainersUtils.*;

@AllArgsConstructor
@Getter
public class DbConnProps {
    private DbType dbType;
    private String userName;
    private String userPass;
    private int dbConnectionPortForTest;
    private String dbHostAddressForTest;
    private String dbName;

    public DbProperties getDbPropertiesForRegistryYaml(boolean isSrServerRunningInContainer) {
        DbProperties ret = new DbProperties();
        ret.setDataSourceUrl(
                DbConnection.getConnectionUrl(
                        dbType,
                        getDbHostAddressForRegistryYaml(isSrServerRunningInContainer),
                        getDbConnectionPortForRegistryYaml(isSrServerRunningInContainer),
                        dbName));
        ret.setDataSourceUser(userName);
        ret.setDataSourcePassword(userPass);
        ret.setDataSourceClassName(dbType.getConnectorClassName());
        return ret;
    }

    public int getDbConnectionPortForRegistryYaml(boolean isSrServerRunningInContainer) {
        switch (dbType) {
            case MYSQL5:
            case MYSQL8:
                return isSrServerRunningInContainer ? MYSQL_PORT_INSIDE_CONTAINER : dbConnectionPortForTest;
            case POSTGRES:
                return isSrServerRunningInContainer ? POSTGRES_PORT_INSIDE_CONTAINER : dbConnectionPortForTest;
            case H2:
                return dbConnectionPortForTest;
            default:
                throw new UnsupportedOperationException("not supported");
        }
    }

    public String getDbHostAddressForRegistryYaml(boolean isSrServerRunningInContainer) {
        switch (dbType) {
            case MYSQL5:
            case MYSQL8:
                return isSrServerRunningInContainer ? MYSQL_CONTAINER_NETWORK_ALIAS : dbHostAddressForTest;
            case POSTGRES:
                return isSrServerRunningInContainer ? POSTGRES_CONTAINER_NETWORK_ALIAS : dbHostAddressForTest;
            case H2:
                // not used for H2
                return "localhost";
            default:
                throw new UnsupportedOperationException("not supported");
        }
    }
}
