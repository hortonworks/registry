/**
 * Copyright 2016-2022 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroSchemaValidatorTest {

    @Test
    public void testNullPrefixedUnionsShouldHaveNullDefaultImplicitly() {
        CompatibilityResult compatibilityResult = AvroSchemaValidator.of(SchemaCompatibility.BOTH).validate(
                new Schema.Parser().parse(schema1),
                new Schema.Parser().parse(schema2)
        );
        assertTrue(compatibilityResult.isCompatible());
    }

    @Test
    public void testImplicitNullDefaultOnlyWhenFieldInReaderSchemaNotFoundInWriter() {
        CompatibilityResult compatibilityResult = AvroSchemaValidator.of(SchemaCompatibility.BOTH).validate(
                new Schema.Parser().parse(schema1),
                new Schema.Parser().parse(schema1OptinalWithDefault)
        );
        assertFalse(compatibilityResult.isCompatible());
    }

    private String schema1 = "{\n" +
            "     \"type\": \"record\",\n" +
            "     \"namespace\": \"com.example\",\n" +
            "     \"name\": \"Record\",\n" +
            "     \"fields\": [\n" +
            "       { \"name\": \"field1\", \"type\": \"long\" }\n" +
            "     ]\n" +
            "} ";

    private String schema1OptinalWithDefault = "{\n" +
            "     \"type\": \"record\",\n" +
            "     \"namespace\": \"com.example\",\n" +
            "     \"name\": \"Record\",\n" +
            "     \"fields\": [\n" +
            "       { \"name\": \"field1\", \"type\": [\"null\", \"long\"], \"default\": null }\n" +
            "     ]\n" +
            "} ";

    private String schema2 = "{\n" +
            "     \"type\": \"record\",\n" +
            "     \"namespace\": \"com.example\",\n" +
            "     \"name\": \"Record\",\n" +
            "     \"fields\": [\n" +
            "       { \"name\": \"field1\", \"type\": \"long\" },\n" +
            "       { \"name\": \"field2\", \"type\": [\"null\", \"string\"] }\n" +
            "     ]\n" +
            "} ";
}