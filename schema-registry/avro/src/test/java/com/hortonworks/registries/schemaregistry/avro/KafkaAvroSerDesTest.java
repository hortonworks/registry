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

import com.hortonworks.registries.schemaregistry.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.avro.kafka.KafkaAvroSerializer;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class KafkaAvroSerDesTest extends AbstractAvroSchemaRegistryCientTest {

    @Test
    public void testPrimitiveSerDes() {
        String topic = TEST_NAME_RULE.getMethodName() + "-" + System.currentTimeMillis();

        _testPrimitiveSerDes(topic, true);

        _testPrimitiveSerDes(topic, false);
    }

    private void _testPrimitiveSerDes(String topic, boolean isKey) {
        Object[] payloads = generatePrimitivePayloads();
        _testKafkaSerDes(topic, isKey, payloads);
    }

    private void _testKafkaSerDes(String topic, boolean isKey, Object... payloads) {
        KafkaAvroSerializer keySerializer = new KafkaAvroSerializer();
        keySerializer.configure(SCHEMA_REGISTRY_CLIENT_CONF, isKey);
        KafkaAvroDeserializer keyDeserializer = new KafkaAvroDeserializer();
        keyDeserializer.configure(SCHEMA_REGISTRY_CLIENT_CONF, isKey);

        for (Object obj : payloads) {
            byte[] serializedData = keySerializer.serialize(topic, obj);
            Object deserializedObj = keyDeserializer.deserialize(topic, serializedData);
            if (obj instanceof byte[]) {
                Assert.assertArrayEquals((byte[]) obj, (byte[]) deserializedObj);
            } else {
                Assert.assertEquals(obj, deserializedObj);
            }
        }
    }

    @Test
    public void testGenericRecordSerDes() throws Exception {
        String topic = TEST_NAME_RULE.getMethodName() + "-" + System.currentTimeMillis();
        Object genericAvroRecord = createGenericAvroRecord(getSchema("/device.avsc"));
        _testKafkaSerDes(topic, true, genericAvroRecord);
    }
}
