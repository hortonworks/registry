/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.json;

import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonSchemaProviderTest {

    private JsonSchemaProvider provider;

    @BeforeEach
    public void setUp() {
        provider = new JsonSchemaProvider();
    }

    @Test
    public void testGenerateFields() throws Exception {
        List<SchemaFieldInfo> fields = provider.generateFields(fetchResourceText("/json/person.json"));
        assertNotNull(fields);
        assertFalse(fields.isEmpty());

        SchemaFieldInfo firstName = getField("firstName", fields);
        SchemaFieldInfo lastName = getField("lastName", fields);
        SchemaFieldInfo age = getField("age", fields);

        assertEquals("string", firstName.getType());
        assertEquals("string", lastName.getType());
        assertEquals("integer", age.getType());
    }

    @Test
    public void testGetResultantSchema() throws Exception {
        String resultantSchema = provider.getResultantSchema(fetchResourceText("/json/person.json"));
        assertNotNull(resultantSchema);
    }

    private SchemaFieldInfo getField(String name, List<SchemaFieldInfo> fields) {
        Optional<SchemaFieldInfo> fieldOpt = fields.stream().filter(f -> f.getName().equals(name)).findFirst();
        assertTrue(fieldOpt.isPresent(), "Expected field not found: " + name);
        return fieldOpt.get();
    }

    private String fetchResourceText(String resourceName) throws IOException {
        return IOUtils.toString(JsonSchemaProviderTest.class.getResourceAsStream(resourceName), StandardCharsets.UTF_8);
    }
}
