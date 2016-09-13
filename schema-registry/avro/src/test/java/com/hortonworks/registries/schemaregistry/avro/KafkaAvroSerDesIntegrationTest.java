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
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(IntegrationTest.class)
public class KafkaAvroSerDesIntegrationTest extends AbstractAvroSchemaRegistryCientTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSerDesIntegrationTest.class);

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @Test
    public void testPrimitivesInKafkaCluster() throws Exception {
        String topicName = TEST_NAME_RULE.getMethodName();

        Object[] msgs = generatePrimitivePayloads();

        _testWithKafkaCluster(topicName, msgs);
    }

    @Test
    public void testGenericRecordInKafkaCluster() throws Exception {
        String topicName = TEST_NAME_RULE.getMethodName();

        Object record = createGenericAvroRecord(getSchema("/device.avsc"));

        _testWithKafkaCluster(topicName, record);

    }

    private void _testWithKafkaCluster(String topicName, Object... msgs) throws InterruptedException {
        CLUSTER.createTopic(topicName);

        String bootstrapServers = produceMessage(topicName, msgs);
        LOG.info("######## Sent the message, waiting for few secs");
        Thread.sleep(5 * 1000);

        ConsumerRecords<String, Object> consumerRecords = consumeMessage(topicName, bootstrapServers);

        Assert.assertEquals(msgs.length, consumerRecords.count());

        int i = 0;
        for (ConsumerRecord<String, Object> consumerRecord : consumerRecords) {
            Assert.assertEquals(getKey(msgs[i]), consumerRecord.key());
            Assert.assertEquals(msgs[i], consumerRecord.value());
            i++;
        }
    }

    private ConsumerRecords<String, Object> consumeMessage(String topicName, String bootstrapServers) throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.putAll(SCHEMA_REGISTRY_CLIENT_CONF);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topicName + "-group-" + System.currentTimeMillis());
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
        LOG.info("############## partitions [{}]", partitions);
        LOG.info("############## subscribed topis: [{}] ", consumer.listTopics());

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        ConsumerRecords<String, Object> consumerRecords = null;
        int ct = 0;
        while (ct++ < 100 && (consumerRecords == null || consumerRecords.isEmpty())) {
            LOG.info("################ polling again");
            consumerRecords = consumer.poll(500);
        }
        consumer.commitSync();
        consumer.close();

        return consumerRecords;
    }

    private String produceMessage(String topicName, Object... msgs) {
        String bootstrapServers = CLUSTER.bootstrapServers();
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(SCHEMA_REGISTRY_CLIENT_CONF);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        final Producer<String, Object> producer = new KafkaProducer<>(config);

        for (Object msg : msgs) {
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, getKey(msg), msg);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    LOG.info("################################################################################");
                    LOG.info("################# received notification: " + recordMetadata + "   ex:" + e);
                }
            });
        }
        producer.flush();
        producer.close(5, TimeUnit.SECONDS);

        return bootstrapServers;
    }

    private String getKey(Object msg) {
        return msg != null ? msg.toString() : "null";
    }
}
