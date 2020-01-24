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
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerde;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import com.hortonworks.registries.streams.examples.dtos.PageView;
import com.hortonworks.registries.streams.examples.dtos.UserActivity;
import com.hortonworks.registries.streams.examples.dtos.UserProfile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class ClickStreamEnrichment {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click_stream_enrichment");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClickStreamEnrichmentDriver.BOOTSTRAP_SERVERS);
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(),
                ClickStreamEnrichmentDriver.SCHEMA_REGISTRY_URL);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractAvroSnapshotDeserializer.SPECIFIC_AVRO_READER, true);
        props.put(KafkaAvroSerializer.STORE_SCHEMA_VERSION_ID_IN_HEADER,
                ClickStreamEnrichmentDriver.STORE_SCHEMA_VERSION_ID_IN_HEADER_POLICY);

        StreamsBuilder builder = new StreamsBuilder();
        final KTable<Long, UserProfile> userProfile = builder.table(ClickStreamEnrichmentDriver.USER_PROFILE_TOPIC);
        final KStream<Long, PageView> pageViewStream = builder.stream(ClickStreamEnrichmentDriver.PAGE_VIEWS_TOPIC);

        final KStream<Long, UserActivity> userActivityKStream = pageViewStream.leftJoin(userProfile, (pageView, profile) -> {
            if (profile != null) {
                return new UserActivity(profile.getId(), profile.getName(), profile.getAge(), profile.getRegion(), pageView.getPage());
            } else {
                // anonymous user
                return new UserActivity(pageView.getUserId(), "anonymous", -1, "unknown", pageView.getPage());
            }
        });

        userActivityKStream.to(ClickStreamEnrichmentDriver.USER_ACTIVITY_TOPIC);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        try {
            // If you've reset the application using the script, `kafka-streams-application-reset.sh` enable the below
            // line only for the first run.
            // streams.cleanUp();
            streams.start();
            // By default, streams should run forever. Since we have finite inputs, the apps enriches the click stream
            // events only for 100s and then shutdowns itself.
            Thread.sleep(100_000);
        } finally {
            streams.close();
        }
    }
}
