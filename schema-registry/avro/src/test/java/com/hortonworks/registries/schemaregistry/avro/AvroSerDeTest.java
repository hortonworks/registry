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
package com.hortonworks.registries.schemaregistry.avro;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class AvroSerDeTest {

    private static Schema schema;

    @BeforeClass
    public static void setup() throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream("/device.avsc");
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(avroSchemaStream);
    }

    @Test
    public void testAvroJsonSerDeApis() {
        Map<String, Object> event = createEvent();

        AvroJsonSerializer<Map<String, Object>> avroJsonSerializer = new AvroJsonSerializer<>();
        byte[] serializedEvent = avroJsonSerializer.serialize(event, schema);

        AvroJsonDeserializer<Map<String, Object>> avroJsonDeserializer = new AvroJsonDeserializer<>();
        Map<String, Object> deserializedEvent = avroJsonDeserializer.deserialize(new ByteArrayInputStream(serializedEvent), schema);

        Assert.assertEquals(event, deserializedEvent);
    }

    private Map<String, Object> createEvent() {
        Map<String, Object> event = new LinkedHashMap<>();

        long timestamp = System.currentTimeMillis();
        event.put("xid", timestamp);
        event.put("name", "device: "+timestamp);
        event.put("version", new Random().nextInt() % 10);
        event.put("timestamp", timestamp);
        return event;
    }
}
