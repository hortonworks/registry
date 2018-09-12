/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.serdes;

import java.util.Map;

public class Utils {

    public static void closeAll(AutoCloseable... closeables) throws Exception {
        Exception exception = null;
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null)
                    closeable.close();
            } catch (Exception e) {
                if (exception != null)
                    exception.addSuppressed(e);
                else
                    exception = e;
            }
        }
        if (exception != null)
            throw exception;
    }

    public static String getOrDefaultAsString(Map<String, ?> configs, String key, String defaultValue) {
        String value = (String) configs.get(key);
        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOrDefault(Map<String, ?> configs, String key, T defaultValue) {
        T result = (T) configs.get(key);
        return result != null ? result : defaultValue;
    }
}
