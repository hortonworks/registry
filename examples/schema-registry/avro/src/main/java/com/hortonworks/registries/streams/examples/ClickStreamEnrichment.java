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
