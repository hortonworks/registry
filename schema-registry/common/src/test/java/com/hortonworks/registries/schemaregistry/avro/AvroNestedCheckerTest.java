/*
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.schemaregistry.avro;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class AvroNestedCheckerTest {

    private static Schema simpleNestedSchema;
    private static Schema complexNestedSchema;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Schema.Parser schemaParser = new Schema.Parser();
        simpleNestedSchema = schemaParser.parse(AvroNestedCheckerTest.class.getResourceAsStream("/avro/nested/nested-simple.avsc"));
        complexNestedSchema = schemaParser.parse(AvroNestedCheckerTest.class.getResourceAsStream("/avro/nested/nested-complex.avsc"));
    }

    @Test
    public void testSimpleAvroFieldsGenerator() throws IOException {
        /*
            Should return 3 fields;
            - SimpleRecord.id : int
            - SimpleRecord.value : string
            - SimpleRecord.parent : Union(null, Record(Record_B))
         */
        Assert.assertEquals(3 , new AvroFieldsGenerator().generateFields(simpleNestedSchema).size());
    }

    @Test
    public void testComplexAvroFieldsGenerator() throws IOException {
        /*
            Should return 8 fields;
            - Record_A.id : int
            - Record_A.value : string
            - Record_A.child : Record(Record_B)
            - Record_A.child.id : int
            - Record_A.child.value : string
            - Record_A.child.parent : Union(null, Record(Record_A))
            - Record_A.arrayTest : array(Record_A)
            - Record_A.mapTest : map(Record_A)
         */
        Assert.assertEquals(8 , new AvroFieldsGenerator().generateFields(complexNestedSchema).size());
    }

    @Test
    public void testSimpleAvroSchemaProvider() throws IOException {
        // As long as no exceptions are thrown, this passes.
        new AvroSchemaProvider().normalize(simpleNestedSchema);
    }

    @Test
    public void testComplexAvroSchemaProvider() throws IOException {
        // As long as no exceptions are thrown, this passes.
        new AvroSchemaProvider().normalize(complexNestedSchema);
    }
}
