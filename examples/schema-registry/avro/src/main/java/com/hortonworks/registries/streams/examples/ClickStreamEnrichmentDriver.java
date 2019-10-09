/*
 * Copyright 2018-2019 Cloudera, Inc.
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
package com.hortonworks.registries.streams.examples;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import com.hortonworks.registries.streams.examples.dtos.PageView;
import com.hortonworks.registries.streams.examples.dtos.UserActivity;
import com.hortonworks.registries.streams.examples.dtos.UserProfile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Before starting this program, create the below three topics.
 *
 * <ul>
 *     <li>sh kafka-topics.sh --zookeeper localhost:2181 --topic user-profile --partitions 1 --replication-factor 1 --create</li>
 *     <li>sh kafka-topics.sh --zookeeper localhost:2181 --topic page-views --partitions 1 --replication-factor 1 --create</li>
 *     <li>sh kafka-topics.sh --zookeeper localhost:2181 --topic user-activity --partitions 1 --replication-factor 1 --create</li>
 * </ul>
 *
 * To reset the application
 * <pre>
 *     sh kafka-streams-application-reset.sh --bootstrap-servers localhost:9092 --application-id click_stream_enrichment
 *     --input-topics page-views,user-profile
 * </pre>
 */
public class ClickStreamEnrichmentDriver {

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String SCHEMA_REGISTRY_URL = "http://localhost:9090/api/v1";
    static final String USER_PROFILE_TOPIC = "user-profile";
    static final String PAGE_VIEWS_TOPIC = "page-views";
    static final String USER_ACTIVITY_TOPIC = "user-activity";

    static final String STORE_SCHEMA_VERSION_ID_IN_HEADER_POLICY = "true";

    private ClickStreamEnrichmentDriver() {
    }

    @SuppressWarnings("unchecked")
    private void generateFakeProfilesAndPageViews() {
        UserProfile[] profiles = new UserProfile[] {
                new UserProfile(0L, "Neo", 25, "asia"),
                new UserProfile(1L, "Morpheus", 28, "africa"),
                new UserProfile(2L, "Smith", 40, "america"),
                new UserProfile(3L, "Trinity", 35, "america"),
                new UserProfile(4L, "Dozer", 30, "south america")
        };

        PageView[] views = new PageView[] {
                new PageView(0L, "the one"),
                new PageView(1L, "trainer"),
                new PageView(2L, "agent"),
                new PageView(4L, "submarine"),
                new PageView(10L, "optical")
        };

        List<ProducerRecord> records = new ArrayList<>();
        // user profiles
        for (UserProfile profile : profiles) {
            records.add(new ProducerRecord(USER_PROFILE_TOPIC, profile.getId(), profile));
        }
        // page views
        for (PageView view : views) {
            records.add(new ProducerRecord(PAGE_VIEWS_TOPIC, view.getUserId(), view));
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER, STORE_SCHEMA_VERSION_ID_IN_HEADER_POLICY);
        try (final KafkaProducer<Long, PageView> producer = new KafkaProducer<>(props)) {
            records.forEach(record -> producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    System.err.println("Error while sending record : " + recordMetadata + " with message : " + e.getMessage());
                }
            }));
        }
    }

    private void consumeUserActivity() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-activity-reader");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);

        try (KafkaConsumer<Integer, UserActivity> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(USER_ACTIVITY_TOPIC));
            while (true) {
                final ConsumerRecords<Integer, UserActivity> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                consumerRecords.forEach(System.out::println);
            }
        }
    }

    public static void main(String[] args) {
        ClickStreamEnrichmentDriver driver = new ClickStreamEnrichmentDriver();
        driver.generateFakeProfilesAndPageViews();
        driver.consumeUserActivity();
    }
}
