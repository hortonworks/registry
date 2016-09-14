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

import com.hortonworks.iotas.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.avro.kafka.KafkaAvroSerializer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category(IntegrationTest.class)
public class KafkaAvroSerDesTest extends AbstractAvroSchemaRegistryCientTest {

    @Test
    public void testPrimitiveSerDes() {
        String topic = TEST_NAME_RULE.getMethodName() + "-" + System.currentTimeMillis();

        _testPrimitiveSerDes(topic);
    }

    private void _testPrimitiveSerDes(String topic) {
        Object[] payloads = generatePrimitivePayloads();
        _testKafkaSerDes(topic, payloads);
    }

    private void _testKafkaSerDes(String topic, Object... payloads) {
        _testKafkaSerDes(topic, true, payloads);
        _testKafkaSerDes(topic, false, payloads);
    }

    private void _testKafkaSerDes(String topic, boolean isKey, Object... payloads) {
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(SCHEMA_REGISTRY_CLIENT_CONF, isKey);
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        avroDeserializer.configure(SCHEMA_REGISTRY_CLIENT_CONF, isKey);

        for (Object obj : payloads) {
            byte[] serializedData = avroSerializer.serialize(topic, obj);
            Object deserializedObj = avroDeserializer.deserialize(topic, serializedData);
            if (obj instanceof byte[]) {
                Assert.assertArrayEquals((byte[]) obj, (byte[]) deserializedObj);
            } else {
                Assert.assertEquals(obj, deserializedObj);
            }
        }
    }

    @Test
    public void testAvroRecordsSerDes() throws Exception {
        String topic = TEST_NAME_RULE.getMethodName() + "-" + System.currentTimeMillis();

        _testKafkaSerDes(topic, createDeviceGenericAvroRecord());

        _testKafkaSerDes(topic, createDeviceRecord());
    }

}
