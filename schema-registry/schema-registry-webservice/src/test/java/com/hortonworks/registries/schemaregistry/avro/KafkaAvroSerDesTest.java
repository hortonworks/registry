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
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */

@Tag("IntegrationTest")
public class KafkaAvroSerDesTest {

    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;

    private String testName;

    @BeforeEach
    void init(TestInfo testInfo) {
        testName = testInfo.getTestMethod().get().getName();
    }


    public static Stream<SchemaRegistryTestProfileType> profiles() {
        return Stream.of(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }
    
    public void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
    }

    @AfterEach
    public void afterParam() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }
    

    @ParameterizedTest
    @MethodSource("profiles")
    public void testPrimitiveSerDes(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        String topicPrefix = testName + "-" + System.currentTimeMillis();

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
            Assertions.assertArrayEquals((byte[]) payload, (byte[]) deserializedObj);
        } else {
            AvroSchemaRegistryClientUtil.assertAvroObjs(payload, deserializedObj);
        }
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void testAvroRecordsSerDes(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        String topicPrefix = testName + "-" + System.currentTimeMillis();

        String genericRecordTopic = topicPrefix + "-generic";
        Object genericRecordForDevice = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();
        testKafkaSerDes(genericRecordTopic, true, genericRecordForDevice);
        testKafkaSerDes(genericRecordTopic, true, genericRecordForDevice);

        Device specificRecord = AvroSchemaRegistryClientUtil.createSpecificRecord();
        String specificRecordTopic = topicPrefix + "-specific";
        testKafkaSerDes(specificRecordTopic, true, specificRecord);
        testKafkaSerDes(specificRecordTopic, false, specificRecord);
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void testIncompatibleSchemas(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        String topic = testName + "-" + System.currentTimeMillis();

        // send initial message
        Object initialMsg = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();
        testKafkaSerDes(topic, true, initialMsg);
        testKafkaSerDes(topic, false, initialMsg);

        // send a message with incompatible version of the schema
        Object incompatMsg = AvroSchemaRegistryClientUtil.createGenericRecordForIncompatDevice();
        try {
            testKafkaSerDes(topic, true, incompatMsg);
            Assertions.fail("An error should have been received here because of incompatible schemas");
        } catch (Exception e) {
            // should have received an error.
        }

        // send a message with compatible version of the schema
        Object compatMsg = AvroSchemaRegistryClientUtil.createGenericRecordForCompatDevice();
        testKafkaSerDes(topic, true, compatMsg);
        testKafkaSerDes(topic, false, compatMsg);
    }

}
