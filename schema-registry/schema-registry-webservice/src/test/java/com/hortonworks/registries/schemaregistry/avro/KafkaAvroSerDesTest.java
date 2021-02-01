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

import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.serdes.Device;
import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import com.hortonworks.registries.schemaregistry.util.CustomParameterizedRunner;
import com.hortonworks.registries.util.SchemaRegistryTestName;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;

/**
 *
 */

@RunWith(CustomParameterizedRunner.class)
@Category(IntegrationTest.class)
public class KafkaAvroSerDesTest {

    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;

    @Rule
    public SchemaRegistryTestName testNameRule = new SchemaRegistryTestName();

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
    }

    @CustomParameterizedRunner.AfterParam
    public static void afterParam() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }


    public KafkaAvroSerDesTest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {

    }

    @Test
    public void testPrimitiveSerDes() {
        String topicPrefix = testNameRule.getMethodName() + "-" + System.currentTimeMillis();

        testPrimitiveSerDes(topicPrefix);
    }

    private void testPrimitiveSerDes(String topicPrefix) {
        Object[] payloads = AvroSchemaRegistryClientUtil.generatePrimitivePayloads();

        for (Object payload : payloads) {
            String topic = topicPrefix + ":" + (payload != null ? payload.getClass().getName() : "null");
            testKafkaSerDes(topic, true, payload);
            testKafkaSerDes(topic, false, payload);
        }
    }

    private void testKafkaSerDes(String topic, boolean isKey, Object payload) {
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        Map<String, Object> schemaRegistryClientConf = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.exportClientConf(true);
        avroSerializer.configure(schemaRegistryClientConf, isKey);
        KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer();
        avroDeserializer.configure(schemaRegistryClientConf, isKey);

        byte[] serializedData = avroSerializer.serialize(topic, payload);
        Object deserializedObj = avroDeserializer.deserialize(topic, serializedData);
        if (payload instanceof byte[]) {
            Assert.assertArrayEquals((byte[]) payload, (byte[]) deserializedObj);
        } else {
            AvroSchemaRegistryClientUtil.assertAvroObjs(payload, deserializedObj);
        }
    }

    @Test
    public void testAvroRecordsSerDes() throws Exception {
        String topicPrefix = testNameRule.getMethodName() + "-" + System.currentTimeMillis();

        String genericRecordTopic = topicPrefix + "-generic";
        Object genericRecordForDevice = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();
        testKafkaSerDes(genericRecordTopic, true, genericRecordForDevice);
        testKafkaSerDes(genericRecordTopic, true, genericRecordForDevice);

        Device specificRecord = AvroSchemaRegistryClientUtil.createSpecificRecord();
        String specificRecordTopic = topicPrefix + "-specific";
        testKafkaSerDes(specificRecordTopic, true, specificRecord);
        testKafkaSerDes(specificRecordTopic, false, specificRecord);
    }

    @Test
    public void testIncompatibleSchemas() throws Exception {
        String topic = testNameRule.getMethodName() + "-" + System.currentTimeMillis();

        // send initial message
        Object initialMsg = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();
        testKafkaSerDes(topic, true, initialMsg);
        testKafkaSerDes(topic, false, initialMsg);

        // send a message with incompatible version of the schema
        Object incompatMsg = AvroSchemaRegistryClientUtil.createGenericRecordForIncompatDevice();
        try {
            testKafkaSerDes(topic, true, incompatMsg);
            Assert.fail("An error should have been received here because of incompatible schemas");
        } catch (Exception e) {
            // should have received an error.
        }

        // send a message with compatible version of the schema
        Object compatMsg = AvroSchemaRegistryClientUtil.createGenericRecordForCompatDevice();
        testKafkaSerDes(topic, true, compatMsg);
        testKafkaSerDes(topic, false, compatMsg);
    }

}
