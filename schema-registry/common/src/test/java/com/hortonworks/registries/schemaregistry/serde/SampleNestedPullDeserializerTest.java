/**
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
 **/
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.common.Schema;
import com.hortonworks.registries.schemaregistry.serde.pull.PullEventContext;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class SampleNestedPullDeserializerTest {
    private static final Logger log = LoggerFactory.getLogger(SampleNestedPullDeserializerTest.class);

    private static final Schema schema = Schema.of(
            Schema.NestedField.of("devices",
                    Schema.NestedField.of("thermostat",
                            Schema.Field.of("id", Schema.Type.STRING),
                            Schema.Field.of("description", Schema.Type.STRING),
                            Schema.Field.of("temp", Schema.Type.INTEGER),
                            Schema.Field.of("loc", Schema.Type.STRING)
                    ),
                    Schema.NestedField.of("camera",
                            Schema.Field.of("id", Schema.Type.STRING),
                            Schema.Field.of("description", Schema.Type.STRING),
                            Schema.Field.of("temp", Schema.Type.INTEGER),
                            Schema.Field.of("loc", Schema.Type.STRING),
                            Schema.Field.of("active", Schema.Type.BOOLEAN),
                            Schema.Field.of("clicks", Schema.Type.INTEGER)
                    )
            )
    );

    @Test
    public void testPullParser() throws Exception {
        _testNestedPullParser("/sample-parser-nested-payload-1.txt");
        _testNestedPullParser("/sample-parser-nested-payload-2.txt");
    }

    private void _testNestedPullParser(String payloadFile) throws Exception {
        SchemaDetails schemaDetails = new SchemaDetails(schema, "test-schema", (new Random().nextInt() % 10));
        try (SampleNestedPullDeserializer pullParser
                     = new SampleNestedPullDeserializer(schemaDetails, this.getClass().getResourceAsStream(payloadFile))) {
            List<PullEventContext.FieldValue> events = new ArrayList<>();
            PullEventContext.FieldValue effectiveFieldValue = null;

            while (pullParser.hasNext()) {
                final PullEventContext<Schema.Field> pullDeserializerContext = pullParser.next();
                if (pullDeserializerContext.startRecord()) {
                    effectiveFieldValue = null;
                    log.info("######### start record");
                } else if (pullDeserializerContext.startField()) {
                    Schema.Field field = pullDeserializerContext.currentField();
                    log.info("######### start field: [{}]", field);
                } else if (pullDeserializerContext.endField()) {
                    final PullEventContext.FieldValue fieldValue = pullDeserializerContext.fieldValue();
                    log.info("######### end field: [{}]", fieldValue);
                    effectiveFieldValue = fieldValue;
                } else if (pullDeserializerContext.endRecord()) {
                    if (effectiveFieldValue != null) {
                        events.add(effectiveFieldValue);
                    }
                    log.info("######### end record");
                }
            }

            Assert.assertTrue(events.size() == 2);
        }
    }

}
