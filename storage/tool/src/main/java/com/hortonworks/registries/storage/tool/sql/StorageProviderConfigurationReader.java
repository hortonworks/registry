/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.storage.tool.sql;

import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.common.util.Constants;

import java.util.Collections;
import java.util.Map;

public class StorageProviderConfigurationReader {
    private static final String STORAGE_PROVIDER_CONFIGURATION = "storageProviderConfiguration";
    private static final String PROPERTIES = "properties";
    private static final String DB_TYPE = "db.type";
    private static final String DB_PROPERTIES = "db.properties";

    public StorageProviderConfiguration readStorageConfig(Map<String, Object> conf) {
        Map<String, Object> storageConf = (Map<String, Object>) conf.get(
                STORAGE_PROVIDER_CONFIGURATION);
        if (storageConf == null) {
            throw new RuntimeException("No storageProviderConfiguration in config file.");
        }

        Map<String, Object> properties = (Map<String, Object>) storageConf.get(PROPERTIES);
        if (properties == null) {
            throw new RuntimeException("No properties presented to storageProviderConfiguration.");
        }

        String dbType = (String) properties.get(DB_TYPE);
        if (dbType == null) {
            throw new RuntimeException("No db.type presented to properties.");
        }

        Map<String, Object> dbProps = (Map<String, Object>) properties.get(DB_PROPERTIES);

        return readDatabaseProperties(dbProps, DatabaseType.fromValue(dbType));
    }

    private static StorageProviderConfiguration readDatabaseProperties(Map<String, Object> dbProperties, DatabaseType databaseType) {
        String jdbcUrl = (String) dbProperties.get(Constants.DataSource.URL);
        String user = (String) dbProperties.getOrDefault(Constants.DataSource.USER, "");
        String password = (String) dbProperties.getOrDefault(Constants.DataSource.PASSWORD, "");

        Map<String, Object> connectionProperties = (Map<String, Object>)
                dbProperties.getOrDefault(Constants.DataSource.CONNECTION_PROPERTIES, Collections.emptyMap());

        return StorageProviderConfiguration.get(databaseType, jdbcUrl, user, password, connectionProperties);
    }
}
