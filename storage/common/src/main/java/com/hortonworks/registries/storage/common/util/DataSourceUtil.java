/**
 * Copyright 2017 Hortonworks.
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

package com.hortonworks.registries.storage.common.util;

import com.hortonworks.registries.common.util.ReflectionHelper;
import com.hortonworks.registries.storage.common.DatabaseType;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

public class DataSourceUtil {

    public static void addConnectionProperties(DatabaseType type, DataSource dataSource, Map<String, Object> connectionProperties) {
        switch (type) {
            case MYSQL:
            case POSTGRESQL:
                break;
            case ORACLE:
                if (connectionProperties != null && !connectionProperties.isEmpty()) {
                    try {
                        Properties properties = new Properties();
                        connectionProperties.putAll(connectionProperties);
                        ReflectionHelper.invokeSetter("ConnectionProperties", dataSource, properties);
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // Do nothing if connection properties are not set.
                }
            default:
                throw new IllegalArgumentException("Unknown Database Type : " + type);
        }
    }
}
