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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class SamplePullDeserializerTest {
    private static final Schema schema = Schema.of(
            Schema.Field.of("dev", Schema.Type.STRING),
            Schema.Field.of("temp", Schema.Type.STRING),
            Schema.Field.of("loc", Schema.Type.STRING)
    );

    @Test
    public void testPullParser() throws Exception {
        SchemaDetails schemaInfo = new SchemaDetails(schema, "test-schema", (new Random().nextInt() % 10));

        try (SamplePullDeserializer pullParser
                     = new SamplePullDeserializer(schemaInfo, this.getClass().getResourceAsStream("/sample-parser-payload.txt"))) {
            List<Map<String, Object>> events = new ArrayList<>();
            Map<String, Object> newEvent = null;

            while (pullParser.hasNext()) {
                final PullEventContext<Schema.Field> pullDeserializerContext = pullParser.next();
                if (pullDeserializerContext.startRecord()) {
                    if (newEvent != null) {
                        events.add(newEvent);
                    }
                    newEvent = new HashMap<>();
                } else if (pullDeserializerContext.endField()) {
                    final PullEventContext.FieldValue<Schema.Field> fieldValue = pullDeserializerContext.fieldValue();
                    newEvent.put(fieldValue.field().getName(), fieldValue.value());
                }
            }

            Assert.assertEquals(events, createEvents(this.getClass().getResourceAsStream("/sample-parser-payload.txt")));
        }
    }

    public List<Map<String, Object>> createEvents(InputStream inputStream) throws Exception {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            List<Map<String, Object>> events = new ArrayList<>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final String[] fieldStrings = line.split(",");
                Map<String, Object> fields = new HashMap<>();
                for (String fieldStr : fieldStrings) {
                    final int index = fieldStr.indexOf(":");
                    fields.put(fieldStr.substring(0, index), fieldStr.substring(index + 1));
                }
                events.add(fields);
            }
            return events;
        }
    }
}
