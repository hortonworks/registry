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
package com.hortonworks.registries.schemaregistry.avro;

import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.avro.util.CustomParameterizedRunner;
import com.hortonworks.registries.schemaregistry.avro.util.SchemaRegistryTestName;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * These tests use embedded kafka cluster and sends/receives messages with producer/consumer apis by configuring
 * {@link KafkaAvroSerializer} and {@link KafkaAvroDeserializer}
 */

@RunWith(CustomParameterizedRunner.class)
@Category(IntegrationTest.class)
public class KafkaAvroSerDesWithKafkaServerTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSerDesWithKafkaServerTest.class);
    private static EmbeddedKafkaCluster CLUSTER;
    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;

    @Rule
    public SchemaRegistryTestName TEST_NAME_RULE = new SchemaRegistryTestName();

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        CLUSTER =  new EmbeddedKafkaCluster(1);
        CLUSTER.start();
    }

    @CustomParameterizedRunner.AfterParam
    public static void afterParam() throws Exception {
        CLUSTER.stop();
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }

    public KafkaAvroSerDesWithKafkaServerTest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {
    }

    @Test
    public void testPrimitivesInKafkaCluster() throws Exception {
        String topicPrefix = TEST_NAME_RULE.getMethodName();

        Object[] msgs = AvroSchemaRegistryClientUtil.generatePrimitivePayloads();

        _testWithKafkaCluster(topicPrefix, msgs);
    }

    @Test
    public void testAvroRecordsInKafkaCluster() throws Exception {
        String topicPrefix = TEST_NAME_RULE.getMethodName();

        _testWithKafkaCluster(topicPrefix + "-generic", AvroSchemaRegistryClientUtil.createGenericRecordForDevice());
        _testWithKafkaCluster(topicPrefix + "-specific", AvroSchemaRegistryClientUtil.createSpecificRecord());
    }

    private void _testWithKafkaCluster(String topicPrefix, Object[] msgs) throws InterruptedException {
        for (Object msg : msgs) {
            String topicName = topicPrefix + (msg != null ? msg.getClass().getName().replace("$", "_") : "null");
            _testWithKafkaCluster(topicName, msg);
        }
    }

    private void _testWithKafkaCluster(String topicName, Object msg) throws InterruptedException {
        createTopic(topicName);
        try {
            String bootstrapServers = produceMessage(topicName, msg);

            String consumerGroup = topicName + "-group-" + new Random().nextLong();
            ConsumerRecords<String, Object> consumerRecords = consumeMessage(topicName, bootstrapServers, consumerGroup);

            Assert.assertEquals(1, consumerRecords.count());

            ConsumerRecord<String, Object> consumerRecord = consumerRecords.iterator().next();
            Object value = consumerRecord.value();
            Assert.assertEquals(getKey(msg), consumerRecord.key());
            AvroSchemaRegistryClientUtil.assertAvroObjs(msg, value);
        } finally {
            CLUSTER.deleteTopic(topicName);
        }
    }

    private void createTopic(String topicName) throws InterruptedException {
        long start = System.currentTimeMillis();
        while(true) {
            try {
                CLUSTER.createTopic(topicName);
                break;
            } catch (TopicExistsException e) {
                // earlier deleted topic may not yet have been deleted.
                // need to fix in EmbeddedSingleNodeKafkaCluster
                if(System.currentTimeMillis() - start > 30000) {
                    throw e;
                }
                Thread.sleep(2000);
            }
        }
    }

    private ConsumerRecords<String, Object> consumeMessage(String topicName, String bootstrapServers, String consumerGroup) throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.exportClientConf(true));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
        Collection<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }
        LOG.info("partitions [{}]", partitions);
        LOG.info("subscribed topis: [{}] ", consumer.listTopics());

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        ConsumerRecords<String, Object> consumerRecords = null;
        int ct = 0;
        while (ct++ < 100 && (consumerRecords == null || consumerRecords.isEmpty())) {
            LOG.info("Polling for consuming messages");
            consumerRecords = consumer.poll(500);
        }
        consumer.commitSync();
        consumer.close();

        return consumerRecords;
    }

    private String produceMessage(String topicName, Object msg) {
        String bootstrapServers = CLUSTER.bootstrapServers();
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.exportClientConf(true));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        final Producer<String, Object> producer = new KafkaProducer<>(config);
        final Callback callback = new ProducerCallback();
        LOG.info("Sending message: [{}] to topic: [{}]", msg, topicName);
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, getKey(msg), msg);
        producer.send(producerRecord, callback);
        producer.flush();
        LOG.info("Message successfully sent to topic: [{}]", topicName);
        producer.close(5, TimeUnit.SECONDS);

        return bootstrapServers;
    }

    private static class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
            LOG.info("Received notification: [{}] and ex: [{}]", recordMetadata, ex);
        }
    }

    private String getKey(Object msg) {
        return msg != null ? msg.toString() : "null";
    }


    @Test
    public void testSchemasCompatibility() throws Exception {
        String topic = TEST_NAME_RULE.getMethodName() + "-" + System.currentTimeMillis();

        // send initial message
        Object initialMsg = AvroSchemaRegistryClientUtil.createGenericRecordForDevice();
        _testWithKafkaCluster(topic, initialMsg);

        // send a message with incompatible version of the schema
        Object incompatMsg = AvroSchemaRegistryClientUtil.createGenericRecordForIncompatDevice();
        try {
            _testWithKafkaCluster(topic, incompatMsg);
            Assert.fail("An error should have been received here because of incompatible schemas");
        } catch (Exception e) {
            // should have received an error.
        }

        // send a message with compatible version of the schema
        Object compatMsg = AvroSchemaRegistryClientUtil.createGenericRecordForCompatDevice();
        _testWithKafkaCluster(topic, compatMsg);
    }

}
