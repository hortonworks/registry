/**
 * Copyright 2016-2020 Cloudera, Inc.
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

import com.google.common.io.Resources;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION;

/**
 *  Test the Confluent API.
 */
public class ConfluentProtocolCompatibleTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentProtocolCompatibleTest.class);
    public static final String GENERIC_TEST_RECORD_SCHEMA =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"namespace\":\"com.hortonworks.registries.schemaregistry.serdes.avro\"," +
        "\"fields\":[{\"name\":\"field1\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null},{\"name\":" +
        "\"field2\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}";

    private LocalSchemaRegistryServer localSchemaRegistryServer;

    @Before
    public void setUp() throws Exception {
        String configPath = new File(Resources.getResource("schema-registry.yaml").toURI()).getAbsolutePath();
        localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
        localSchemaRegistryServer.start();
    }

    @After
    public void tearDown() throws Exception {
        if (localSchemaRegistryServer != null) {
            localSchemaRegistryServer.stop();
            localSchemaRegistryServer = null;
        }
    }

    @Test
    public void testConfluentProduceRegistryConsume() {
        final String confluentUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());
        final String registryUrl = String.format("http://localhost:%d/api/v1", localSchemaRegistryServer.getLocalPort());

        Map<String, Object> confluentConfig = new HashMap<>();
        confluentConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, confluentUrl);

        Map<String, Object> registryConfig = new HashMap<>();
        registryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl);

        Schema schema = new Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);

        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();


        io.confluent.kafka.serializers.KafkaAvroSerializer kafkaAvroSerializer = new io.confluent.kafka.serializers.KafkaAvroSerializer();
        kafkaAvroSerializer.configure(confluentConfig, false);
        byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

        io.confluent.kafka.serializers.KafkaAvroDeserializer confluentKafkaAvroDeserializer = 
                new io.confluent.kafka.serializers.KafkaAvroDeserializer();
        confluentKafkaAvroDeserializer.configure(confluentConfig, false);

        GenericRecord confluentResult = (GenericRecord) confluentKafkaAvroDeserializer.deserialize("topic", bytes);
        LOG.info(confluentResult.toString());

        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(registryConfig, false);

        GenericRecord registryResult = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);

        LOG.info(registryResult.toString());

        Assert.assertEquals(record, registryResult);
        Assert.assertEquals(record, confluentResult);

        Assert.assertEquals(registryResult, confluentResult);
    }

    @Test
    public void testRegistryProduceConfluentConsume() {
        final String confluentUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());
        final String registryUrl = String.format("http://localhost:%d/api/v1", localSchemaRegistryServer.getLocalPort());

        Map<String, Object> confluentConfig = new HashMap<>();
        confluentConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, confluentUrl);

        Map<String, Object> registryConfig = new HashMap<>();
        registryConfig.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), registryUrl);
        registryConfig.put(SERDES_PROTOCOL_VERSION, SerDesProtocolHandlerRegistry.CONFLUENT_VERSION_PROTOCOL);

        Schema schema = new Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);
        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();


        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
        kafkaAvroSerializer.configure(registryConfig, false);
        byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(registryConfig, false);

        GenericRecord registryResult = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);
        LOG.info(registryResult.toString());


        io.confluent.kafka.serializers.KafkaAvroDeserializer confluentKafkaAvroDeserializer = 
                new io.confluent.kafka.serializers.KafkaAvroDeserializer();
        confluentKafkaAvroDeserializer.configure(confluentConfig, false);

        GenericRecord confluentResult = (GenericRecord) confluentKafkaAvroDeserializer.deserialize("topic", bytes);
        LOG.info(confluentResult.toString());

        Assert.assertEquals(record, registryResult);
        Assert.assertEquals(record, confluentResult);

        Assert.assertEquals(registryResult, confluentResult);
    }

    @Test
    public void testProprietaryContentType() throws Exception {
        final String confluentUrl = String.format("http://localhost:%d/api/v1/confluent/subjects", localSchemaRegistryServer.getLocalPort());

        checkContentType(confluentUrl, "application/json", HttpURLConnection.HTTP_OK);
        checkContentType(confluentUrl, "application/vnd.schemaregistry.v1+json", HttpURLConnection.HTTP_OK);
        checkContentType(confluentUrl, "application/vnd.schemaregistry.v1+json; q=0.9, application/json; q=0.5", HttpURLConnection.HTTP_OK);
        checkContentType(confluentUrl, "text/plain", HttpURLConnection.HTTP_INTERNAL_ERROR);
        checkContentType(confluentUrl, "image/jpeg", HttpURLConnection.HTTP_INTERNAL_ERROR);
    }

    /** Send a GET request to the server with the given Accept: header. */
    private void checkContentType(String url, String acceptContent, int expectedStatus) throws IOException {
        CloseableHttpClient httpClient = null;
        try {
            httpClient = HttpClientBuilder.create().build();
            HttpGet getRequest = new HttpGet(url);
            getRequest.addHeader("accept", acceptContent);
            HttpResponse response = httpClient.execute(getRequest);
            Assert.assertEquals(expectedStatus, response.getStatusLine().getStatusCode());
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    
}
