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

public final class Utils {

    private Utils() {
    }

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

    /**
     * Returns the value to which the specified key is mapped or
     * {@code defaultValue} if the provided map contains no mapping for the key /
     * the trimmed value is empty.
     *
     * @param map the mappings
     * @param key the key whose associated value is to be returned
     * @param defaultValue the default mapping of the key
     * @return the value to which the specified key is mapped or
     * {@code defaultValue} if the provided map contains no mapping for the key /
     * the trimmed value is empty.
     */
    public static String getOrDefaultAsString(Map<String, ?> map, String key, String defaultValue) {
        String value = (String) map.get(key);
        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOrDefault(Map<String, ?> map, String key, T defaultValue) {
        T value = (T) map.get(key);
        return value != null ? value : defaultValue;
    }
}
