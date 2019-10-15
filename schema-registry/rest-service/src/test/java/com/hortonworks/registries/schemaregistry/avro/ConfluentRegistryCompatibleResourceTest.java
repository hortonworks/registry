/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.ErrorMessage;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.SchemaString;
import com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.Schema;
import com.hortonworks.registries.schemaregistry.webservice.LocalSchemaRegistryServer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.hortonworks.registries.schemaregistry.avro.ConfluentProtocolCompatibleTest.GENERIC_TEST_RECORD_SCHEMA;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.Id;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.incompatibleSchemaError;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.invalidSchemaError;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.subjectNotFoundError;
import static com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.versionNotFoundError;

/**
 *  Avro 1.9 removed APIs that exposes Jackson classes in its library. Unfortunately Confluent serdes still uses an older version
 *  of Avro so below test cases are broken against the latest Confluent serdes. Below test cases will be ignored for now
 *  and it will be enabled once Confluent serdes have updated their Avro dependency
 *
 *  Tests related to APIs exposed with {@link ConfluentSchemaRegistryCompatibleResource}
 */
@Ignore
public class ConfluentRegistryCompatibleResourceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfluentRegistryCompatibleResourceTest.class);

    @Rule
    public TestName testNameRule = new TestName();

    private WebTarget rootTarget;
    private LocalSchemaRegistryServer localSchemaRegistryServer;

    @Before
    public void setup() throws Exception {
        String configPath = new File(Resources.getResource("schema-registry.yaml").toURI()).getAbsolutePath();
        localSchemaRegistryServer = new LocalSchemaRegistryServer(configPath);
        localSchemaRegistryServer.start();
        String rootUrl = String.format("http://localhost:%d/api/v1/confluent", localSchemaRegistryServer.getLocalPort());
        rootTarget = createRootTarget(rootUrl);
    }

    @After
    public void cleanup() throws Exception {
        if (localSchemaRegistryServer != null) {
            localSchemaRegistryServer.stop();
        }
    }

    @Test
    public void stressTestConfluentApis() throws Exception {
        for (int i = 0; i < 1000; i++) {
            doTestAPIsMixWithIncompatibleInvalidSchemas(testName() + "-" + i);
        }
    }

    @Test
    public void testConfluentSerDes() throws Exception {

        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(GENERIC_TEST_RECORD_SCHEMA);
        GenericRecord record = new GenericRecordBuilder(schema).set("field1", "some value").set("field2", "some other value").build();

        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, rootTarget.getUri().toString());

        KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
        kafkaAvroSerializer.configure(config, false);
        byte[] bytes = kafkaAvroSerializer.serialize("topic", record);

        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(config, false);

        GenericRecord result = (GenericRecord) kafkaAvroDeserializer.deserialize("topic", bytes);
        LOG.info(result.toString());
    }

    @Test
    public void testConfluentBasicApisMixWithInvalidIncompatibleSchemas() throws Exception {
        String subjectName = testName();
        doTestAPIsMixWithIncompatibleInvalidSchemas(subjectName);
    }

    private void doTestAPIsMixWithIncompatibleInvalidSchemas(String subjectName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        // register initial version of schema
        long initialSchemaId = objectMapper.readValue(postSubjectSchema(subjectName, fetchSchema("/device.avsc"))
                                                             .readEntity(String.class),
                                                    Id.class).getId();

        // try to register incompatible schema and check for expected errors
        Response response = postSubjectSchema(subjectName, fetchSchema("/device-incompat.avsc"));
        Assert.assertEquals(incompatibleSchemaError().getStatus(), response.getStatus());
        ErrorMessage errorMessage = objectMapper.readValue(response.readEntity(String.class), ErrorMessage.class);
        Assert.assertEquals(((ErrorMessage) incompatibleSchemaError().getEntity()).getErrorCode(), errorMessage.getErrorCode());

        // register valid schema
        String secondVersionSchema = fetchSchema("/device-compat.avsc");
        long secondSchemaId = objectMapper.readValue(postSubjectSchema(subjectName, secondVersionSchema)
                                                            .readEntity(String.class),
                                                   Id.class).getId();
        Assert.assertTrue(initialSchemaId < secondSchemaId);
        // retrieve the schema for that version and check whether it has same schema.
        String receivedSchema = getVersion(subjectName,"latest").getSchema();
        Assert.assertEquals(new org.apache.avro.Schema.Parser().parse(secondVersionSchema),
                            new org.apache.avro.Schema.Parser().parse(receivedSchema));

        // check latest version of schema
        String latestSchema = getVersion(subjectName, "latest").getSchema();
        Assert.assertEquals(new org.apache.avro.Schema.Parser().parse(secondVersionSchema),
                            new org.apache.avro.Schema.Parser().parse(latestSchema));

        // check for invalid schemas
        Response invalidSchemaResponse = postSubjectSchema(subjectName, fetchSchema("/device-unsupported-type.avsc"));
        Assert.assertEquals(invalidSchemaError().getStatus(), invalidSchemaResponse.getStatus());
        ErrorMessage invalidSchemaErrorMessage = objectMapper.readValue(invalidSchemaResponse.readEntity(String.class), ErrorMessage.class);
        Assert.assertEquals(((ErrorMessage) invalidSchemaError().getEntity()).getErrorCode(), invalidSchemaErrorMessage.getErrorCode());
    }

    @Test
    public void testConfluentApis() throws Exception {
        List<String> schemas = Arrays.stream(new String[]{"/device.avsc", "/device-compat.avsc", "/device-incompat.avsc"})
                                     .map(x -> {
                                         try {
                                             return fetchSchema(x);
                                         } catch (IOException e) {
                                             throw new RuntimeException(e);
                                         }
                                     })
                                     .collect(Collectors.toList());

        ObjectMapper objectMapper = new ObjectMapper();
        List<String> subjects = new ArrayList<>();
        for (String schemaText : schemas) {
            String subjectName = UUID.randomUUID().toString();
            subjects.add(subjectName);

            // check post schema version
            Long schemaId = objectMapper.readValue(postSubjectSchema(subjectName, schemaText)
                                                             .readEntity(String.class),
                                                     Id.class).getId();

            // check get version api
            Schema schemaVersionEntry = getVersion(subjectName,"latest");

            Assert.assertEquals(subjectName, schemaVersionEntry.getSubject());
            Assert.assertEquals(schemaId.intValue(), schemaVersionEntry.getId().intValue());
            org.apache.avro.Schema recvdSchema = new org.apache.avro.Schema.Parser().parse(schemaVersionEntry.getSchema());
            org.apache.avro.Schema regdSchema = new org.apache.avro.Schema.Parser().parse(schemaText);
            Assert.assertEquals(regdSchema, recvdSchema);
        }

        // check all registered subjects
        List<String> recvdSubjects = getAllSubjects();
        Assert.assertEquals(new HashSet<>(subjects), new HashSet<>(recvdSubjects));
    }

    private String testName() {
        return testNameRule.getMethodName();
    }

    private WebTarget createRootTarget(String rootUrl) {
        Client client = ClientBuilder.newBuilder()
                                     .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE)
                                     .build();
        client.register(MultiPartFeature.class);
        return client.target(rootUrl);
    }

    @Test
    public void testSubjectWithVersionIdApi() throws Exception {
        String subject = testName();
        String response = postSubjectSchema(subject,
                                            fetchSchema("/device.avsc")).readEntity(String.class);
        ObjectMapper objectMapper = new ObjectMapper();
        long id = objectMapper.readValue(response, Id.class).getId();
        Assert.assertTrue(id > 0);

        String validResponse = rootTarget.path(String.format("/subjects/%s/versions/%s", subject, id))
                                         .request(MediaType.APPLICATION_JSON_TYPE)
                                         .get(String.class);
        Schema schemaVersionEntry = new ObjectMapper().readValue(validResponse, Schema.class);
        Assert.assertEquals(subject, schemaVersionEntry.getSubject());
        Assert.assertEquals(id, schemaVersionEntry.getVersion().intValue());

        // invalid subject, valid version
        String invalidSubject = subject + new Random().nextInt();
        Response invalidSubjectResponse = rootTarget.path(String.format("/subjects/%s/versions/%s", invalidSubject, id))
                                                    .request(MediaType.APPLICATION_JSON_TYPE)
                                                    .get();
        Assert.assertEquals(subjectNotFoundError().getEntity(),
                            objectMapper.readValue(invalidSubjectResponse.readEntity(String.class), ErrorMessage.class));

        // valid subject, invalid versions
        String[] invalidVersions = {
                id + 10 + "", // non existing version
                "invalid-version", // invalid version string
                -1 + "", // invalid version number can not be <= 0
                0 + "", // invalid version number can not be <= 0
                ((long) Integer.MAX_VALUE) + 10L + "" // invalid version number, can not be > Integer.MAX_VALUE
        };
        for (String invalidVersion : invalidVersions) {
            Response invalidVersionResponse = rootTarget.path(String.format("/subjects/%s/versions/%s", subject, invalidVersion))
                                                        .request(MediaType.APPLICATION_JSON_TYPE)
                                                        .get();
            Assert.assertEquals(versionNotFoundError().getEntity(),
                                objectMapper.readValue(invalidVersionResponse.readEntity(String.class), ErrorMessage.class));
        }

    }

    @Test
    public void testInValidSchemas() throws Exception {
        // add invalid schema
        Response invalidSchemaResponse = postSubjectSchema(testName(),
                                                           fetchSchema("/device-unsupported-type.avsc"));
        Assert.assertEquals(invalidSchemaError().getStatus(), invalidSchemaResponse.getStatus());

        ErrorMessage errorMessage = new ObjectMapper().readValue(invalidSchemaResponse.readEntity(String.class),
                                                                 ErrorMessage.class);
        Assert.assertEquals(invalidSchemaError().getEntity(), errorMessage);
    }


    @Test
    public void testIncompatibleSchemas() throws Exception {
        String subject = testName();
        String response = postSubjectSchema(subject,
                                            fetchSchema("/device.avsc")).readEntity(String.class);
        ObjectMapper objectMapper = new ObjectMapper();
        long id = objectMapper.readValue(response, Id.class).getId();
        Assert.assertTrue(id > 0);

        // add incompatible schema
        Response incompatSchemaResponse = postSubjectSchema(subject,
                                                            fetchSchema("/device-incompat.avsc"));
        Assert.assertEquals(incompatibleSchemaError().getStatus(), incompatSchemaResponse.getStatus());

        ErrorMessage errorMessage = objectMapper.readValue(incompatSchemaResponse.readEntity(String.class),
                                                           ErrorMessage.class);
        Assert.assertEquals(incompatibleSchemaError().getEntity(), errorMessage);
    }

    @Test
    public void testNonExistingSubject() throws Exception {
        // check non existing subject
        Response nonExistingSubjectResponse = rootTarget.path("/subjects/" + testName() + "/versions")
                                                        .request(MediaType.APPLICATION_JSON_TYPE)
                                                        .get();
        ErrorMessage errorMessage = new ObjectMapper().readValue(nonExistingSubjectResponse.readEntity(String.class),
                                                                 ErrorMessage.class);
        Assert.assertEquals(subjectNotFoundError().getStatus(),
                            nonExistingSubjectResponse.getStatus());

        Assert.assertEquals(subjectNotFoundError().getEntity(), errorMessage);
    }

    private List<String> getAllSubjects() throws IOException {
        String subjectsResponse = rootTarget.path("/subjects").request().get(String.class);
        return new ObjectMapper().readValue(subjectsResponse, new TypeReference<List<String>>() {
        });
    }

    private com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.Schema getVersion(String subjectName, String version) throws IOException {
        WebTarget versionsTarget = rootTarget.path("/subjects/" + subjectName + "/versions/");
        String versionsResponse = versionsTarget.path(version).request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        return new ObjectMapper().readValue(versionsResponse, Schema.class);
    }

    private com.hortonworks.registries.schemaregistry.webservice.ConfluentSchemaRegistryCompatibleResource.Schema getSchemaById(Long schemaId) throws IOException {
        WebTarget versionsTarget = rootTarget.path("/schemas/ids/");
        String versionsResponse = versionsTarget.path(schemaId.toString()).request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        return new ObjectMapper().readValue(versionsResponse, Schema.class);
    }

    private Response postSubjectSchema(String subjectName, String schemaText) throws IOException {
        WebTarget subjectsTarget = rootTarget.path("/subjects/" + subjectName + "/versions");
        SchemaString schemaString = new SchemaString();
        schemaString.setSchema(schemaText);
        return subjectsTarget.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaString));
    }

    private String fetchSchema(String filePath) throws IOException {
        return IOUtils.toString(this.getClass().getResourceAsStream(filePath),
                                "UTF-8");
    }

}
