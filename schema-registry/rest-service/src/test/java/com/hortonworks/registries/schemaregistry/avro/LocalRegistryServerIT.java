/**
 * Copyright 2016-2019 Cloudera, Inc.
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
 **/
package com.hortonworks.registries.schemaregistry.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestConfiguration;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Client;
import com.hortonworks.registries.shaded.javax.ws.rs.client.ClientBuilder;
import com.hortonworks.registries.shaded.javax.ws.rs.client.WebTarget;
import com.hortonworks.registries.shaded.javax.ws.rs.core.MediaType;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientConfig;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientProperties;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.JerseyClientBuilder;
import com.hortonworks.registries.util.CustomParameterizedRunner;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_CONNECTION_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_READ_TIMEOUT;

/**
 *
 */

@RunWith(CustomParameterizedRunner.class)
public class LocalRegistryServerIT {

    private static SchemaRegistryTestConfiguration SCHEMA_REGISTRY_TEST_CONFIGURATION;
    private static SchemaMetadata schemaMetadata_0;
    private static SchemaMetadata schemaMetadata_1;
    private static SchemaMetadata schemaMetadata_2;
    private static Long schemaId_0;
    private static Long schemaId_1;
    private static Long schemaId_2;
    private static SchemaMetadataInfo schemaMetadataInfo0;
    private static SchemaMetadataInfo schemaMetadataInfo1;
    private static SchemaMetadataInfo schemaMetadataInfo2;
    private static Client restClient;
    private static SchemaRegistryTestServerClientWrapper schemaRegistryTestServerClientWrapper;
    

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void setUp(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_CONFIGURATION = SchemaRegistryTestConfiguration.forProfileType(schemaRegistryTestProfileType);
    }

    public LocalRegistryServerIT(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {

    }
    @Before
    public void setup() throws Exception{
        schemaRegistryTestServerClientWrapper = new SchemaRegistryTestServerClientWrapper(SchemaRegistryTestConfiguration.forProfileType(SchemaRegistryTestProfileType.DEFAULT));
        schemaRegistryTestServerClientWrapper.startTestServer();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryTestServerClientWrapper.getClient();
        
        schemaMetadata_0 = new SchemaMetadata.Builder("dummy").description("important desc").validationLevel(SchemaValidationLevel.ALL).compatibility(SchemaCompatibility.BACKWARD).schemaGroup("Kafka").type("avro").build();
        schemaMetadata_1 = new SchemaMetadata.Builder("plant").description("very important desc").validationLevel(SchemaValidationLevel.ALL).compatibility(SchemaCompatibility.NONE).schemaGroup("Kafka").type("avro").build();
        schemaMetadata_2 = new SchemaMetadata.Builder("plutonium").description("spacemacs").validationLevel(SchemaValidationLevel.LATEST).compatibility(SchemaCompatibility.BACKWARD).schemaGroup("Kafka").type("avro").build();

        schemaId_0 = schemaRegistryClient.registerSchemaMetadata(schemaMetadata_0);
        schemaId_1 = schemaRegistryClient.registerSchemaMetadata(schemaMetadata_1);
        schemaId_2 = schemaRegistryClient.registerSchemaMetadata(schemaMetadata_2);

        schemaMetadataInfo0 = new SchemaMetadataInfo(schemaMetadata_0, schemaId_0, null);
        schemaMetadataInfo1 = new SchemaMetadataInfo(schemaMetadata_1, schemaId_1, null);
        schemaMetadataInfo2 = new SchemaMetadataInfo(schemaMetadata_2, schemaId_2, null);

        Map<String, String> conf = new HashMap<>();
        conf.put("name", "config");
        ClientConfig config = createClientConfig(conf);
        ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder()
                .withConfig(config)
                .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE);
        restClient = clientBuilder.build();
    }
    
    @After 
    public void deleteSetup() throws Exception{
        schemaRegistryTestServerClientWrapper.stopTestServer();
    }
    
    @Test
    public void testSanity() throws Exception {

        SchemaRegistryTestServerClientWrapper schemaRegistryTestServerClientWrapper = new SchemaRegistryTestServerClientWrapper(SCHEMA_REGISTRY_TEST_CONFIGURATION);
        schemaRegistryTestServerClientWrapper.startTestServer();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryTestServerClientWrapper.getClient();

        // registering schema metadata
        SchemaMetadata schemaMetadata = new SchemaMetadata.Builder("foo").type("avro").build();
        Long schemaId = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
        Assert.assertNotNull(schemaId);

        // registering a new schema
        String schemaName = schemaMetadata.getName();
        String schema1 = IOUtils.toString(LocalRegistryServerIT.class.getResourceAsStream("/schema-1.avsc"), "UTF-8");
        SchemaIdVersion v1 = schemaRegistryClient.addSchemaVersion(schemaName, new SchemaVersion(schema1, "Initial version of the schema"));

        schemaRegistryTestServerClientWrapper.stopTestServer();
    }

    @Test
    public void listSchemas_NameOrderValidation() throws Exception {
        //given
        SchemaMetadataInfo expected = schemaMetadataInfo1;
        
        //when
        TestResponse schemaMetadataInfo = createSchemaMetadataInfoFromQuery("/schemaregistry/schemas?name=plant&validationLevel=ALL&_orderByFields=timestamp,a", schemaRegistryTestServerClientWrapper, restClient);
        SchemaMetadataInfo actual = schemaMetadataInfo.getEntities().get(0);
        
        //then
        Assert.assertThat(actual.getSchemaMetadata(), Is.is(expected.getSchemaMetadata()));
        Assert.assertThat(actual.getId(), Is.is(expected.getId()));
    }
    @Test
    public void listSchemas_OrderCompatibility() throws Exception {
        //given
        List<SchemaMetadata> expecteds = new ArrayList<SchemaMetadata>();
        expecteds.add(schemaMetadata_0);
        expecteds.add(schemaMetadata_2);
        
        //when
        TestResponse schemaMetadataInfo = createSchemaMetadataInfoFromQuery("/schemaregistry/schemas?compatibility=BACKWARD&_orderByFields=timestamp,a", schemaRegistryTestServerClientWrapper, restClient);
        SchemaMetadataInfo actual_1 = (SchemaMetadataInfo)((ArrayList)schemaMetadataInfo.getEntities()).get(0);
        SchemaMetadataInfo actual_2 = (SchemaMetadataInfo)((ArrayList)schemaMetadataInfo.getEntities()).get(1);
        List<SchemaMetadata> actuals = new ArrayList<SchemaMetadata>();
        actuals.add(actual_1.getSchemaMetadata());
        actuals.add(actual_2.getSchemaMetadata());
        
        //then
        Assert.assertThat(actuals, Is.is(expecteds));
    }

    @Test
    public void listSchemas_NameDescSchemaGroup() throws Exception {
        //given
        SchemaMetadataInfo expected = schemaMetadataInfo2;
        
        //when
        TestResponse schemaMetadataInfo = createSchemaMetadataInfoFromQuery("/schemaregistry/schemas?name=plutonium&desc=spacemacs&schemaGroup=Kafka", schemaRegistryTestServerClientWrapper, restClient);
        SchemaMetadataInfo actual = (SchemaMetadataInfo) ((ArrayList) schemaMetadataInfo.getEntities()).get(0);
        
        //then
        Assert.assertThat(actual.getSchemaMetadata(), Is.is(expected.getSchemaMetadata()));
        Assert.assertThat(actual.getId(), Is.is(expected.getId()));

    }

    
    @Test
    public void listAggregatedSchemas_NameOrderValidation() throws Exception {
        //given
        SchemaMetadataInfo expected = schemaMetadataInfo1;

        //when
        TestResponse schemaMetadataInfo = createSchemaMetadataInfoFromQuery("/schemaregistry/schemas/aggregated?name=plant&validationLevel=ALL&_orderByFields=timestamp,a", schemaRegistryTestServerClientWrapper, restClient);
        SchemaMetadataInfo actual = (SchemaMetadataInfo) ((ArrayList) schemaMetadataInfo.getEntities()).get(0);

        //then
        Assert.assertThat(actual.getSchemaMetadata(), Is.is(expected.getSchemaMetadata()));
        Assert.assertThat(actual.getId(), Is.is(expected.getId()));
    }

    @Test
    public void listAggregatedSchemas_OrderCompatibility() throws Exception {
        //given
        List<SchemaMetadata> expecteds = new ArrayList<SchemaMetadata>();
        expecteds.add(schemaMetadata_0);
        expecteds.add(schemaMetadata_2);

        //when
        TestResponse schemaMetadataInfo = createSchemaMetadataInfoFromQuery("/schemaregistry/schemas/aggregated?compatibility=BACKWARD&_orderByFields=timestamp,a", schemaRegistryTestServerClientWrapper, restClient);
        SchemaMetadataInfo actual_1 = (SchemaMetadataInfo) ((ArrayList) schemaMetadataInfo.getEntities()).get(0);
        SchemaMetadataInfo actual_2 = (SchemaMetadataInfo) ((ArrayList) schemaMetadataInfo.getEntities()).get(1);
        List<SchemaMetadata> actuals = new ArrayList<SchemaMetadata>();
        actuals.add(actual_1.getSchemaMetadata());
        actuals.add(actual_2.getSchemaMetadata());

        //then
        Assert.assertThat(actuals, Is.is(expecteds));
    }
    
    @Test
    public void listAggregatedSchemas_NameDescSchemaGroup() throws Exception {
        //given
        SchemaMetadataInfo expected = schemaMetadataInfo2;
        
        //when
        TestResponse schemaMetadataInfo = createSchemaMetadataInfoFromQuery("/schemaregistry/schemas/aggregated?name=plutonium&desc=spacemacs&schemaGroup=Kafka", schemaRegistryTestServerClientWrapper, restClient);
        SchemaMetadataInfo actual = (SchemaMetadataInfo) ((ArrayList) schemaMetadataInfo.getEntities()).get(0);
        
        //then
        Assert.assertThat(actual.getSchemaMetadata(), Is.is(expected.getSchemaMetadata()));
        Assert.assertThat(actual.getId(), Is.is(expected.getId()));
    }
    
    TestResponse createSchemaMetadataInfoFromQuery(String query, SchemaRegistryTestServerClientWrapper schemaRegistryTestServerClientWrapper, Client restClient) throws Exception{
        String uri = schemaRegistryTestServerClientWrapper.exportClientConf().get("schema.registry.url").toString() + query;
        WebTarget target = restClient.target(uri);
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();
        String entity = response.readEntity(String.class);
        ObjectMapper om = new ObjectMapper();
        return om.readValue(entity, TestResponse.class);
    }
    
    protected ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        config.property(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            config.property(entry.getKey(), entry.getValue());
        }
        return config;
    }
    
}
class TestResponse{
    private List<SchemaMetadataInfo> entities;
    public TestResponse(){}

    public List<SchemaMetadataInfo> getEntities() {
        return entities;
    }

    public void setEntities(List<SchemaMetadataInfo> entities) {
        this.entities = entities;
    }
}
