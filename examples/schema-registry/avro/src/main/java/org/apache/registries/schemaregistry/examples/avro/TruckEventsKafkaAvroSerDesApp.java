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
package org.apache.registries.schemaregistry.examples.avro;

import org.apache.registries.schemaregistry.client.SchemaRegistryClient;
import org.apache.registries.schemaregistry.examples.TruckEventsCsvConverter;
import org.apache.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TruckEventsKafkaAvroSerDesApp {
    private static final Logger LOG = LoggerFactory.getLogger(TruckEventsKafkaAvroSerDesApp.class);

    public static final String MSGS_LIMIT = "msgsLimit";
    public static final String TOPIC = "topic";
    String producerProps;
    String registryURL;
    String schemaFile;

    public TruckEventsKafkaAvroSerDesApp(String producerProps, String registryURL, String schemaFile) {
        this.producerProps = producerProps;
        this.registryURL = registryURL;
        this.schemaFile = schemaFile;
    }

    public void sendMessages(String payloadFile) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream(this.producerProps));
        int limit = Integer.parseInt(props.getProperty(MSGS_LIMIT, 50 + ""));

        // convert the payload from csv to json records
        TruckEventsCsvConverter truckEventsCsvConverter = new TruckEventsCsvConverter();
        InputStream payloadResourceStream = new FileInputStream(payloadFile);
        List<String> jsonRecords = truckEventsCsvConverter.convertToJsonRecords(payloadResourceStream, limit);

        // convert json to avro records
        Schema schema = new Schema.Parser().parse(new FileInputStream(this.schemaFile));
        List<Object> avrorecords = new ArrayList<>();
        for (String jsonRecord : jsonRecords) {
            avrorecords.add(jsonToAvro(jsonRecord, schema));
        }

        // send avro messages to given topic using KafkaAvroSerializer which registers payload schema if it does not exist
        // with schema name as "<topic-name>:v", type as "avro" and schemaGroup as "kafka".
        // schema registry should be running so that KafkaAvroSerializer can register the schema.
        Map<String, Object> producerConfig = createProducerConfig(props);

        String topicName = props.getProperty(TOPIC);
        produceMessage(topicName, avrorecords, producerConfig);
    }

    private Map<String, Object> createProducerConfig(Properties props) {
        String bootstrapServers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.putAll(Collections.singletonMap(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, this.registryURL));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        return config;
    }

    private Object jsonToAvro(String jsonString, Schema schema) throws Exception {
        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        Object object = reader.read(null, DecoderFactory.get().jsonDecoder(schema, jsonString));

        if (schema.getType().equals(Schema.Type.STRING)) {
            object = object.toString();
        }
        return object;
    }

    private void produceMessage(String topicName, List<Object> msgs, Map<String, Object> producerConfig) {
        LOG.info("inside produce Message " + msgs.size());
        final Producer<String, Object> producer = new KafkaProducer<>(producerConfig);
        final Callback callback = new MyProducerCallback();
        for (Object msg : msgs) {
            LOG.info("Sending message: [{}] to topic: [{}]", msg, topicName);
            ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topicName, msg);
            producer.send(producerRecord, callback);
        }
        producer.flush();
        LOG.info("Message successfully sent to topic: [{}]", topicName);
        producer.close(5, TimeUnit.SECONDS);
    }

    private static class MyProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            LOG.info("#### received [{}], ex: [{}]", recordMetadata, e);
        }
    }

    /**
     * Print the command line options help message and exit application.
     */
    @SuppressWarnings("static-access")
    private static void showHelpMessage(String[] args, Options options) {
        Options helpOptions = new Options();
        helpOptions.addOption(Option.builder("h").longOpt("help")
                .desc("print this message").build());
        try {
            CommandLine helpLine = new DefaultParser().parse(helpOptions, args, true);
            if (helpLine.hasOption("help") || args.length == 1) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("truck-events-kafka-ingest", options);
                System.exit(0);
            }
        } catch (ParseException ex) {
            LOG.error("Parsing failed.  Reason: " + ex.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        Option dataFileOption = Option.builder("d").longOpt("data-file").hasArg().desc("Provide a data file").required().type(String.class).build();
        Option producerFileOption = Option.builder("p").longOpt("producer-config").hasArg().desc("Provide a Kafka producer config file").required().type(String.class).build();
        Option registryUrlOption = Option.builder("r").longOpt("registry-url").hasArg().desc("Provide the registry URL").required().type(String.class).build();
        Option schemaOption = Option.builder("s").longOpt("schema-file").hasArg().desc("Provide a schema file").required().type(String.class).build();

        Options options = new Options();
        options.addOption(dataFileOption);
        options.addOption(producerFileOption);
        options.addOption(registryUrlOption);
        options.addOption(schemaOption);
        showHelpMessage(args, options);

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;

        try {
            commandLine = parser.parse(options, args);
            TruckEventsKafkaAvroSerDesApp truckEventsKafkaAvroSerDesApp = new TruckEventsKafkaAvroSerDesApp(commandLine.getOptionValue("p"),
                            commandLine.getOptionValue("r"), commandLine.getOptionValue("s"));
            truckEventsKafkaAvroSerDesApp.sendMessages(commandLine.getOptionValue("d"));

        } catch(ParseException e) {
            LOG.error("Please provide all the options ", e);
        } catch(Exception e) {
            LOG.error("Failed to send messages ", e);
        }

    }
}
