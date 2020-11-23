/*
 * Copyright 2016-2019 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.registries.schemaregistry.examples.avro;

import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;
import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

/**
 * Below class can be used to send messages to a given topic in kafka-producer.props like below.
 *
 * To run the producer, give the following program arguments:
 * KafkaAvroSerDesApp -sm -d yelp_review_json -s yelp_review.avsc -p kafka-producer.props
 *
 * To run the consumer, give the following program arguments:
 * KafkaAvroSerDesApp -cm -c kafka-consumer.props
 *
 * If invalid messages need to be ignored while sending messages to a topic, you can set "ignoreInvalidMessages" to true
 * in kafka producer properties file.
 *
 */
public class KafkaAvroSerDesApp {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSerDesApp.class);

    public static final String MSGS_LIMIT_OLD = "msgsLimit";
    public static final String MSGS_LIMIT = "msgs.limit";
    public static final String TOPIC = "topic";
    public static final int DEFAULT_MSGS_LIMIT = 50;
    public static final String IGNORE_INVALID_MSGS = "ignore.invalid.messages";

    private String producerProps;
    private String schemaFile;
    private String consumerProps;

    public KafkaAvroSerDesApp(String producerProps, String schemaFile) {
        this.producerProps = producerProps;
        this.schemaFile = schemaFile;
    }

    public KafkaAvroSerDesApp(String consumerProps) {
        this.consumerProps = consumerProps;
    }

    public void sendMessages(String payloadJsonFile) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream(this.producerProps)) {
            props.load(fileInputStream);
        }
        int limit = Integer.parseInt(props.getProperty(MSGS_LIMIT_OLD,
                                                       props.getProperty(MSGS_LIMIT,
                                                                         DEFAULT_MSGS_LIMIT + "")));
        boolean ignoreInvalidMsgs = Boolean.parseBoolean(props.getProperty(IGNORE_INVALID_MSGS, "false"));

        int current = 0;
        Schema schema = new Schema.Parser().parse(new File(this.schemaFile));
        String topicName = props.getProperty(TOPIC);

        // set protocol version to the earlier one.
        props.put(SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

        final Producer<String, Object> producer = new KafkaProducer<>(props);
        final Callback callback = new MyProducerCallback();

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(payloadJsonFile))) {
            String line;
            while (current++ < limit && (line = bufferedReader.readLine()) != null) {
                // convert json to avro records
                Object avroMsg;
                try {
                    avroMsg = jsonToAvro(line, schema);
                } catch (Exception ex) {
                    LOG.warn("Error encountered while converting json to avro of message [{}]", line, ex);
                    if(ignoreInvalidMsgs) {
                        continue;
                    } else {
                        throw ex;
                    }
                }

                // send avro messages to given topic using KafkaAvroSerializer which registers payload schema if it does not exist
                // with schema name as "<topic-name>:v", type as "avro" and schemaGroup as "kafka".
                // schema registry should be running so that KafkaAvroSerializer can register the schema.
                LOG.info("Sending message: [{}] to topic: [{}]", avroMsg, topicName);
                ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, avroMsg);
                try {
                    producer.send(producerRecord, callback);
                } catch (SerDesException ex) {
                    LOG.warn("Error encountered while sending message [{}]", line, ex);
                    if(!ignoreInvalidMsgs) {
                        throw ex;
                    }
                }
            }
        } finally {
            producer.flush();
            LOG.info("All message are successfully sent to topic: [{}]", topicName);
            producer.close(5, TimeUnit.SECONDS);
        }
    }

    private Object jsonToAvro(String jsonString, Schema schema) throws Exception {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonString));

        if (schema.getType().equals(Schema.Type.STRING)) {
            object = object.toString();
        }
        return object;
    }

    private static class MyProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            LOG.info("#### received [{}], ex: [{}]", recordMetadata, e);
        }
    }

    public void consumeMessages() throws Exception {
        Properties props = new Properties();
        try (FileInputStream inputStream = new FileInputStream(this.consumerProps)) {
            props.load(inputStream);
        }
        String topicName = props.getProperty(TOPIC);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        while (true) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
            LOG.info("records size " + records.count());
            for (ConsumerRecord<String, Object> record : records) {
                LOG.info("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset()
                        + " with headers : " + Arrays.toString(record.headers().toArray()));
            }
        }
    }

    public static void main(String[] args) {

        Option sendMessages = Option.builder("sm").longOpt("send-messages").desc("Send Messages to Kafka").type(Boolean.class).build();
        Option consumeMessages = Option.builder("cm").longOpt("consume-messages").desc("Consume Messages from Kafka").type(Boolean.class).build();


        Option dataFileOption = Option.builder("d").longOpt("data-file").hasArg().desc("Provide a data file").type(String.class).build();
        Option producerFileOption = Option.builder("p").longOpt("producer-config").hasArg().desc("Provide a Kafka producer config file").type(String.class).build();
        Option schemaOption = Option.builder("s").longOpt("schema-file").hasArg().desc("Provide a schema file").type(String.class).build();

        Option consumerFileOption = Option.builder("c").longOpt("consumer-config").hasArg().desc("Provide a Kafka Consumer config file").type(String.class).build();

        OptionGroup groupOpt = new OptionGroup();
        groupOpt.addOption(sendMessages);
        groupOpt.addOption(consumeMessages);
        groupOpt.setRequired(true);

        Options options = new Options();
        options.addOptionGroup(groupOpt);
        options.addOption(dataFileOption);
        options.addOption(producerFileOption);
        options.addOption(schemaOption);
        options.addOption(consumerFileOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;

        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption("sm")) {
                if (commandLine.hasOption("p") && commandLine.hasOption("d") && commandLine.hasOption("s")) {
                    KafkaAvroSerDesApp kafkaAvroSerDesApp = new KafkaAvroSerDesApp(commandLine.getOptionValue("p"),
                                                                                     commandLine.getOptionValue("s"));
                    kafkaAvroSerDesApp.sendMessages(commandLine.getOptionValue("d"));
                } else {
                    LOG.error("please provide following options for sending messages to Kafka");
                    LOG.error("-d or --data-file");
                    LOG.error("-s or --schema-file");
                    LOG.error("-p or --producer-config");
                }
            } else if (commandLine.hasOption("cm")) {
                if (commandLine.hasOption("c")) {
                    KafkaAvroSerDesApp kafkaAvroSerDesApp = new KafkaAvroSerDesApp(commandLine.getOptionValue("c"));
                    kafkaAvroSerDesApp.consumeMessages();
                } else {
                    LOG.error("please provide following options for consuming messages from Kafka");
                    LOG.error("-c or --consumer-config");
                }
            }
        } catch (ParseException e) {
            LOG.error("Please provide all the options ", e);
        } catch (Exception e) {
            LOG.error("Failed to send/receive messages ", e);
        }

    }
}
