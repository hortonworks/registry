/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.registries.streams.examples;

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
    static final String USER_PROFILE_TOPIC = "user-profile";
    static final String PAGE_VIEWS_TOPIC = "page-views";
    static final String USER_ACTIVITY_TOPIC = "user-activity";

    // Streams doesn't pass the header information when serializing / de-serializing the key / value while writing the
    // record to the sink topics. (Cf. org.apache.kafka.streams.processor.internals.RecordCollectorImpl.java#L153).
    //
    // The Headers are passed as is b/w the processor nodes and to the sink topic. This leads to incorrect results as
    // the `key.schema.version.id` and `value.schema.version.id` in the header is not valid for the sink topic.
    //
    // So, storing schemaIdVersion in record header is not possible now.
    static final String STORE_SCHEMA_VERSION_ID_IN_HEADER_POLICY = "false";

    ClickStreamEnrichmentDriver() {
    }

    @SuppressWarnings("unchecked")
    void generateFakeProfilesAndPageViews() {
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

    void consumeUserActivity() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "user-activity-reader");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);

        try (KafkaConsumer<Integer, UserActivity> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(USER_ACTIVITY_TOPIC));
            while (true) {
                final ConsumerRecords<Integer, UserActivity> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                consumerRecords.forEach(record -> System.out.println("[offset : " + record.offset() +
                        ", key : " + record.key() + ", value : " + record.value() + "]"));
            }
        }
    }

    public static void main(String[] args) {
        ClickStreamEnrichmentDriver driver = new ClickStreamEnrichmentDriver();
        driver.generateFakeProfilesAndPageViews();
        driver.consumeUserActivity();
    }
}
