/*
 * Copyright 2016-2020 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.authorizer.agent.util;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SchemaTextStore {
    public static final String SCHEMA_TEXT_3 = readFromFile("schema-text3.avcs");
    public static final String SCHEMA_TEXT_3_1 = readFromFile("schema-text3_1.avcs");
    public static final String SCHEMA_TEXT_4 = readFromFile("schema-text4.avcs");

    /**
     * Reads schema text from file stored in resources folder
     * @param fileName
     * @return
     */
    private static String readFromFile(String fileName) {
        ClassLoader classLoader = SchemaTextStore.class.getClassLoader();

        try (InputStream inputStream = classLoader.getResourceAsStream(fileName)) {

            String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8);

            return  result;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
