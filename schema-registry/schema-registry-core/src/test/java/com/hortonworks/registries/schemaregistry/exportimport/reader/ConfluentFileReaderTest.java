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
package com.hortonworks.registries.schemaregistry.exportimport.reader;

import com.google.common.collect.ImmutableSet;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.exportimport.RawSchema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ConfluentFileReaderTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentFileReaderTest.class);

    @Test
    public void testBrackets() {
        ConfluentFileReader fileReader = new ConfluentFileReader(mock(InputStream.class));

        assertEquals(2, fileReader.getPositionOfClosingBracket(" {} {}"));
        assertEquals(5, fileReader.getPositionOfClosingBracket("{{{}}}"));
        assertEquals(5, fileReader.getPositionOfClosingBracket("{{  }}}}}"));
        assertEquals(-1, fileReader.getPositionOfClosingBracket("}}}{{{"));
        assertEquals(-1, fileReader.getPositionOfClosingBracket("{{{}}"));
        assertEquals(-1, fileReader.getPositionOfClosingBracket("abcde"));
    }

    @Test
    public void testParsing() {
        ConfluentFileReader fileReader = new ConfluentFileReader(getClass().getResourceAsStream("/exportimport/confluent1.txt"));
        Set<RawSchema> allSchemas = parseAllSchemas(3, fileReader);

        assertTrue("Did not find schema Car with version 1", findSchema(allSchemas, "Car", 1).isPresent());
        assertTrue("Did not find schema Car with version 2", findSchema(allSchemas, "Car", 2).isPresent());
        assertTrue("Did not find schema Fruit", findSchema(allSchemas, "Fruit").isPresent());
    }

    @Test
    public void testParsing2() {
        ConfluentFileReader fileReader = new ConfluentFileReader(getClass().getResourceAsStream("/exportimport/confluent2.txt"));
        Set<RawSchema> allSchemas = parseAllSchemas(2, fileReader);

        assertTrue("Did not find schema Animal", findSchema(allSchemas, "Animal").isPresent());
        assertTrue("Did not find schema Fruit", findSchema(allSchemas, "Fruit").isPresent());

        RawSchema animal = findSchema(allSchemas, "Animal").get();
        RawSchema fruit = findSchema(allSchemas, "Fruit").get();
        assertEquals(SchemaCompatibility.BOTH, animal.getMetadata().getSchemaMetadata().getCompatibility());
        assertEquals(SchemaValidationLevel.LATEST, animal.getMetadata().getSchemaMetadata().getValidationLevel());
        assertEquals(SchemaCompatibility.BACKWARD, fruit.getMetadata().getSchemaMetadata().getCompatibility());
        assertEquals(SchemaValidationLevel.LATEST, fruit.getMetadata().getSchemaMetadata().getValidationLevel());
    }

    @Test
    public void testParsing3() {
        ConfluentFileReader fileReader = new ConfluentFileReader(getClass().getResourceAsStream("/exportimport/confluent3.txt"));
        Set<RawSchema> allSchemas = parseAllSchemas(5, fileReader);

        assertTrue("Did not find schema Car with version 1", findSchema(allSchemas, "Car", 1).isPresent());
        assertTrue("Did not find schema Car with version 2", findSchema(allSchemas, "Car", 2).isPresent());
        assertTrue("Did not find schema Mas with version 1", findSchema(allSchemas, "Mas", 1).isPresent());
        assertTrue("Did not find schema Mas with version 2", findSchema(allSchemas, "Mas", 2).isPresent());
        assertTrue("Did not find schema Mas with version 3", findSchema(allSchemas, "Mas", 3).isPresent());

        assertEquals(SchemaCompatibility.BOTH, findSchema(allSchemas, "Car").get().getMetadata().getSchemaMetadata().getCompatibility());
        assertEquals(SchemaCompatibility.NONE, findSchema(allSchemas, "Mas").get().getMetadata().getSchemaMetadata().getCompatibility());
    }

    @Test
    public void testParsingInvalidFile() {
        ConfluentFileReader reader1 = new ConfluentFileReader(new ByteArrayInputStream("invalid schema".getBytes()));
        assertNull(reader1.readSchema());

        ConfluentFileReader reader2 = new ConfluentFileReader(new ByteArrayInputStream("{invalid schema} {invalid}".getBytes()));
        assertNull(reader2.readSchema());
    }

    /** Parse an expected amount of schemas and return them in a set. */
    private Set<RawSchema> parseAllSchemas(int expectedCount, ConfluentFileReader fileReader) {
        ImmutableSet.Builder<RawSchema> result = ImmutableSet.builder();
        for (int i = 0; i < expectedCount; i++) {
            RawSchema rawSchema = fileReader.readSchema();
            LOG.debug("Schema {}: {}", (i + 1), rawSchema);
            if (rawSchema != null) {
                result.add(rawSchema);
            } else {
                fail("Expected " + expectedCount + " schemas, got " + i);
            }
        }

        assertNull("Expected no more than " + expectedCount + " schemas.", fileReader.readSchema());

        return result.build();
    }

    private Optional<RawSchema> findSchema(Set<RawSchema> allSchemas, String schemaName) {
        return allSchemas.stream()
                .filter(s -> s.getMetadata().getSchemaMetadata().getName().equals(schemaName))
                .findFirst();
    }

    private Optional<RawSchema> findSchema(Set<RawSchema> allSchemas, String schemaName, Integer version) {
        return allSchemas.stream()
                .filter(s -> s.getMetadata().getSchemaMetadata().getName().equals(schemaName) &&
                        version.equals(s.getVersion().getVersion()))
                .findFirst();
    }

}
