/**
 * Copyright 2016-2019 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.schemaregistry.SchemaFieldInfo;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

/**
 *
 */
public class AvroSchemaFieldsGeneratorTest {

    @Test
    public void testComplexSchemaRead() throws Exception {
        AvroFieldsGenerator avroFieldsGenerator = new AvroFieldsGenerator();

        try (InputStream schemaStream = this.getClass().getResourceAsStream("/schema-1.avsc");) {

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaStream);

            List<SchemaFieldInfo> schemaFieldInfos = avroFieldsGenerator.generateFields(schema);

            Assertions.assertEquals(12, schemaFieldInfos.size());
        }
    }
}
