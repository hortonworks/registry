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

import com.hortonworks.registries.storage.common.DatabaseType;

import java.util.Collections;
import java.util.Map;

public class StorageProviderConfiguration {

    private DatabaseType dbType;

    private String url;
    private String user = "";
    private String password = "";

    private Map<String, Object> connectionProperties;

    private StorageProviderConfiguration(DatabaseType dbType, String url, String user, String password, Map<String, Object> connectionProperties) {
        this.dbType = dbType;
        this.url = url;
        this.user = user;
        this.password = password;
        this.connectionProperties = connectionProperties;
    }


    public static StorageProviderConfiguration get(DatabaseType databaseType,
                                                   String url,
                                                   String user,
                                                   String password,
                                                   Map<String, Object> connectionProperties) {
        return new StorageProviderConfiguration(databaseType, url, user, password, connectionProperties);
    }

    public static StorageProviderConfiguration get(DatabaseType databaseType,
                                                   String url,
                                                   String user,
                                                   String password) {
        return new StorageProviderConfiguration(databaseType, url, user, password, Collections.emptyMap());
    }

    public DatabaseType getDbType() {
        return dbType;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public Map<String, Object> getConnectionProperties() {
        return connectionProperties;
    }


}