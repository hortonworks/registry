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

import static com.hortonworks.registries.schemaregistry.avro.ConfluentProtocolCompatibleTest.GENERIC_TEST_RECORD_SCHEMA;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.Resources;

import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConfluentRegistryCompatibleResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentRegistryCompatibleResourceTest.class);

    @Test
    public void testSanity() throws Exception {
        String configPath = new File(Resources.getResource("schema-registry-test.yaml").toURI()).getAbsolutePath();
        LocalSchemaRegistryServer localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
        try {
            localSchemaRegistryServer.start();

            final String rootUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());

            
            
            Schema schema = new Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);
            GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();

            Map<String, Object> config = new HashMap<>();
            config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, rootUrl);

            KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
            kafkaAvroSerializer.configure(config, false);
            byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

            KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
            kafkaAvroDeserializer.configure(config, false);

            GenericRecord result = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);

            LOG.info(result.toString());

        } finally {
            localSchemaRegistryServer.stop();
        }
    }
    
}
