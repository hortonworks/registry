/*
 * Copyright 2016-2021 Cloudera, Inc.
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
 **/
package com.cloudera.dim.schemaregistry.steps;

import com.cloudera.dim.schemaregistry.GlobalState;
import com.cloudera.dim.schemaregistry.TestAtlasServer;
import com.cloudera.dim.schemaregistry.TestSchemaRegistryServer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.CollectionResponse;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.cloudera.dim.schemaregistry.GlobalState.AGGREGATED_SCHEMAS;
import static com.cloudera.dim.schemaregistry.GlobalState.COMPATIBILITY;
import static com.cloudera.dim.schemaregistry.GlobalState.HTTP_RESPONSE_CODE;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_EXPORT;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_ID;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_META_INFO;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_NAME;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_VERSION_ID;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_VERSION_NO;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_VERSION_TEXT;
import static com.cloudera.dim.schemaregistry.TestAtlasServer.KAFKA_TOPIC_TYPEDEF_NAME;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.hortonworks.registries.schemaregistry.SchemaCompatibility.BACKWARD;
import static com.hortonworks.registries.schemaregistry.SchemaValidationLevel.ALL;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringEscapeUtils.unescapeJava;
import static org.apache.http.HttpHeaders.ACCEPT_CHARSET;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.MULTIPART_FORM_DATA;
import static org.apache.http.protocol.HttpCoreContext.HTTP_RESPONSE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class WhenSteps extends AbstractSteps {

    private static final Logger LOG = LoggerFactory.getLogger(WhenSteps.class);

    public WhenSteps(TestSchemaRegistryServer testServer, GlobalState sow, TestAtlasServer testAtlasServer) {
        super(testServer, sow, testAtlasServer);
    }

    @Given("that Schema Registry is running")
    public void thatSchemaRegistryIsRunning() throws Exception {
        sow.clear();
        if (!testServer.isRunning()) {
            try {
                testServer.start();   // start schema registry server
            } catch (Throwable ex) {
                LOG.error("Failed to start test server.", ex);
                fail("Failed to start test server. If you're running from IntelliJ then do a gradle build first.");
            }
        }
        testServer.cleanupDb();
        schemaRegistryClient = createSchemaRegistryClient(testServer.getPort());
    }

    @Given("that Atlas is running")
    public void thatAtlasIsRunning() throws Exception {
        if (!testAtlasServer.isRunning()) {
            testAtlasServer.start();
        }
    }

    /**
     * Enable Atlas integration. If SR is already running, then stop it, so it can be restarted.
     */
    @Given("Atlas integration is enabled")
    public void atlasIntegrationIsEnabled() throws Exception {
        if (!testServer.isRunning()) {
            testServer.setAtlasEnabled(true);
        } else if (!testServer.isAtlasEnabled()) {
            testServer.stop();
            testServer.setAtlasEnabled(true);
        }
        testServer.setAtlasPort(testAtlasServer.getAtlasPort());
    }

    /**
     * Disable Atlas integration. If SR is already running, then stop it, so it can be restarted.
     */
    @Given("Atlas integration is disabled")
    public void atlasIntegrationIsDisabled() throws Exception {
        if (!testServer.isRunning()) {
            testServer.setAtlasEnabled(false);
        } else if (testServer.isAtlasEnabled()) {
            testServer.stop();
            testServer.setAtlasEnabled(false);
        }
        testServer.setAtlasPort(-1);
    }

    @Given("no {string} topic exists")
    public void noTopicExistsWithName(String topicName) {
        testAtlasServer.dslSearch("{}");
    }

    @Given("there is preexisting {string} topic")
    public void thereIsPreexistingTopic(String topicName) {
        AtlasSearchResult result = new AtlasSearchResult();
        result.setEntities(
                singletonList(
                        new AtlasEntityHeader(KAFKA_TOPIC_TYPEDEF_NAME, "[guid]", ImmutableMap.of("name", topicName))
                )
        );
        testAtlasServer.dslSearch(result);
    }

    @Given("an item exported from a Confluent registry:")
    public void aSchemaDefinitionExportedFromAConfluentRegistry(String confluentExport) throws IOException {
        TreeNode schemaTree = skipFirstObjectThenParse(confluentExport);
        sow.setValue(SCHEMA_VERSION_TEXT, unquote(unescapeJava(schemaTree.get("schema").toString())));
        sow.setValue(SCHEMA_EXPORT, confluentExport);
    }

    @Given("an export containing the schema {string} and version {int} on the {string} branch and schema text:")
    public void aPreviouslyExportedSchemaFromSchemaRegistryWithNameWithTheVersionOnTheBranchAndSchemaText(String name, Integer version, String branch, String schemaText) {
        SchemaVersionInfo versionInfo = new SchemaVersionInfo((long) version,
                name,
                version,
                schemaText,
                currentTimeMillis(),
                "[version-description]"
        );
        AggregatedSchemaBranch schemaBranch =
                new AggregatedSchemaBranch(
                        new SchemaBranch(1L, branch, name, "[branch-description]", currentTimeMillis()),
                        null,
                        new LinkedList<>(singletonList(versionInfo))
                );

        AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = new AggregatedSchemaMetadataInfo(
                new SchemaMetadata.Builder(name)
                        .type("avro")
                        .schemaGroup("Kafka")
                        .description("[description]")
                        .compatibility(BACKWARD)
                        .validationLevel(ALL)
                        .evolve(true)
                        .build(),
                1L,
                currentTimeMillis(),
                new LinkedList<>(singletonList(schemaBranch)),
                emptyList()
        );

        sow.setValue(SCHEMA_VERSION_TEXT, schemaText);
        sow.setValue(SCHEMA_EXPORT, aggregatedSchemaMetadataInfo);
    }


    @Given("we got an export containing the schema {string}")
    public void aSchemaRegistryExportContainingASchemaWithTheName(String name) {
        weGotASchemaRegistryExportContainingASchemaWithTheNameAndType("avro", name);
    }

    @Given("we got an export containing an {string} typed {string} schema")
    public void weGotASchemaRegistryExportContainingASchemaWithTheNameAndType(String type, String name) {
        weGotASchemaRegistryExportContainingASchemaWithTheNameAndType(type, name, 1L);
    }

    @Given("we got an export containing an {string} typed {string} schema with id {long}")
    public void weGotASchemaRegistryExportContainingASchemaWithTheNameAndType(String type, String name, long id) {
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = new AggregatedSchemaMetadataInfo(
                new SchemaMetadata.Builder(name)
                        .type(type)
                        .schemaGroup("Kafka")
                        .description("[description]")
                        .compatibility(BACKWARD)
                        .validationLevel(ALL)
                        .evolve(true)
                        .build(),
                id,
                currentTimeMillis(),
                new ArrayList<>(),
                emptyList()
        );
        sow.setValue(SCHEMA_EXPORT, aggregatedSchemaMetadataInfo);
    }

    @Given("in the export there is a version {int} with id {long} on the {string} branch with the same schema text as the already existing one")
    public void versionOnTheBranchIsWhatWeAlreadyHaveInSchemaRegistry(Integer version, Long id, String branchName) {
        versionOnTheBranchIsWhatWeAlreadyHaveInSchemaRegistry(version, id, branchName, null);
    }

    @Given("in the export there is a version {int} with id {long} on the {string} branch forked from version {int} with the same schema text as the already existing one")
    public void versionOnTheBranchIsWhatWeAlreadyHaveInSchemaRegistry(Integer version, Long id, String branchName, Integer forkedFromVersion) {
        String schemaText = (String) sow.getValue(SCHEMA_VERSION_TEXT);
        versionOnTheBranchWithSchemaText(version, id, branchName, forkedFromVersion, schemaText);
    }


    @Given("in the export there is a version {int} with id {long} on the {string} branch with schema text:")
    public void versionOnTheBranchWithSchemaText(Integer version, Long id, String branchName, String schemaText) {
        versionOnTheBranchWithSchemaText(version, id, branchName, null, schemaText);
    }

    @Given("in the export there is a version {int} with id {long} on the {string} branch forked from version {int} with schema text:")
    public void versionOnTheBranchWithSchemaText(Integer version, Long id, String branchName, Integer forkedFromVersion, String schemaText) {
        AggregatedSchemaMetadataInfo meta = (AggregatedSchemaMetadataInfo) sow.getValue(SCHEMA_EXPORT);
        String name = meta.getSchemaMetadata().getName();
        SchemaVersionInfo versionInfo = new SchemaVersionInfo(
                id,
                name,
                version,
                schemaText,
                currentTimeMillis(),
                "[version-description]"
        );

        Optional<AggregatedSchemaBranch> schemaBranch = meta.getSchemaBranches().stream()
                .filter(b -> branchName.equalsIgnoreCase(b.getSchemaBranch().getName()))
                .findFirst();
        if (schemaBranch.isPresent()) {
            schemaBranch.get().getSchemaVersionInfos().add(versionInfo);
        } else {
            AggregatedSchemaBranch branch = new AggregatedSchemaBranch(
                    new SchemaBranch(nextBranchId(meta),
                            branchName,
                            name,
                            "[branch-description]",
                            currentTimeMillis()
                    ),
                    forkedFromVersion != null ? (long) forkedFromVersion : null,
                    new LinkedList<>(singletonList(versionInfo))
            );
            meta.getSchemaBranches().add(branch);
        }

        sow.setValue(SCHEMA_EXPORT, meta);
    }

    private long nextBranchId(AggregatedSchemaMetadataInfo meta) {
        if (meta.getSchemaBranches().isEmpty()) {
            return 1L;
        } else {
            return new LinkedList<>(meta.getSchemaBranches()).getLast().getSchemaBranch().getId() + 1;
        }
    }


    @Given("assuming it was created with the id {int} and version {int}")
    public void assumingItWasCreatedWithTheIdAndVersion(Integer id, Integer version) {
        assertEquals(id.toString(), sow.getValue(SCHEMA_VERSION_ID).toString());
        assertEquals(version.toString(), sow.getValue(SCHEMA_VERSION_NO).toString());
    }

    @Given("the schema meta {string} exists with the following parameters:")
    @When("we create a new schema meta {string} with the following parameters:")
    public void weCreateANewSchemaMetaWithTheFollowingParameters(String schemaName, DataTable table) {
        SchemaMetadata meta = dataTableToSchemaMetadata(schemaName, table);
        Long id = schemaRegistryClient.addSchemaMetadata(meta);
        sow.setValue(SCHEMA_NAME, schemaName);
        sow.setValue(SCHEMA_ID, id);

        LOG.info("New schema [{}] was created with id {}", schemaName, id);
    }

    @When("we update the schema {string} with the following parameters:")
    public void weUpdateTheSchemaWithTheFollowingParameters(String schemaName, DataTable table) {
        SchemaMetadata meta = dataTableToSchemaMetadata(schemaName, table);
        SchemaMetadataInfo info = schemaRegistryClient.updateSchemaMetadata(schemaName, meta);

        assertNotNull(info, "No response was received from SchemaRegistry.");

        sow.setValue(SCHEMA_META_INFO, info);
    }

    private SchemaMetadata dataTableToSchemaMetadata(String schemaName, DataTable table) {
        SchemaMetadata.Builder builder = new SchemaMetadata.Builder(schemaName);

        List<Map<String, String>> rows = table.asMaps(String.class, String.class);
        for (Map<String, String> columns : rows) {
            String name = checkNotNull(columns.get("Name"), "Missing \"Name\" column.");
            String value = StringUtils.trimToNull(columns.get("Value"));

            if (value == null) {
                continue;
            }

            switch (name.toLowerCase()) {
                case "type":
                    builder.type(value);
                    break;
                case "schemagroup":
                    builder.schemaGroup(value);
                    break;
                case "compatibility":
                    builder.compatibility(SchemaCompatibility.valueOf(value));
                    break;
                case "validationlevel":
                    builder.validationLevel(SchemaValidationLevel.valueOf(value));
                    break;
                case "description":
                    builder.description(value);
                    break;
                case "evolve":
                    builder.evolve(BooleanUtils.toBooleanObject(value));
                    break;
                default:
                    LOG.warn("Ignoring unknown property {}", name);
                    break;
            }
        }

        return builder.build();
    }

    @When("we create a new version for schema {string} with the following schema:")
    public void weCreateANewVersionForSchemaWithTheFollowingParameters(String schemaName, String avroTxt) throws Exception {
        weCreateANewVersionForSchemaWithTheFollowingParametersAndDesc(schemaName, "desc", avroTxt);
    }

    @When("we create a new version for schema {string} with the following description {string} and schema:")
    public void weCreateANewVersionForSchemaWithTheFollowingParametersAndDesc(String schemaName, String desc, String avroTxt) throws Exception {
        SchemaVersion sv = new SchemaVersion(avroTxt, desc);
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaName, sv, false);

        if (schemaIdVersion != null) {
            sow.setValue(SCHEMA_VERSION_ID, schemaIdVersion.getSchemaVersionId());
            sow.setValue(SCHEMA_VERSION_NO, schemaIdVersion.getVersion());
            sow.setValue(SCHEMA_VERSION_TEXT, avroTxt);
        }
    }

    @When("we search for aggregated schemas")
    public void weSearchForAggregatedSchemas() {
        weSearchForAggregatedSchemasWithTheFollowingParameters(null);
    }

    @When("we search for aggregated schemas with the following parameters:")
    public void weSearchForAggregatedSchemasWithTheFollowingParameters(@Nullable DataTable table) {
        String name = null;
        String description = null;
        String orderBy = null;

        if (table != null) {
            List<Map<String, String>> rows = table.asMaps(String.class, String.class);
            assertTrue(rows.size() > 0);

            Map<String, String> cols = rows.get(0);
            name = StringUtils.trimToNull(cols.get("Name"));
            description = StringUtils.trimToNull(cols.get("Description"));
            orderBy = StringUtils.trimToNull(cols.get("OrderBy"));
        }

        List<AggregatedSchemaMetadataInfo> aggregatedSchemas = schemaRegistryClient.findAggregatedSchemas(name, description, orderBy);
        sow.setValue(AGGREGATED_SCHEMAS, aggregatedSchemas);
    }

    @When("we send an HTTP POST form request to {string} with the following parameters:")
    public void weSendAnHTTPPOSTFormRequestToWithTheFollowingParameters(String url, DataTable table) {
        try (CloseableHttpClient httpClient = getHttpClient()) {
            List<NameValuePair> parameters = new ArrayList<>();

            List<Map<String, String>> rows = table.asMaps(String.class, String.class);
            for (Map<String, String> columns : rows) {
                String name = checkNotNull(columns.get("Name"), "Missing \"Name\" column.");
                String value = StringUtils.trimToEmpty(columns.get("Value"));
                parameters.add(new BasicNameValuePair(name, value));
            }

            HttpPost request = new HttpPost(getBaseUrl(testServer.getPort()) + url);

            request.setEntity(new UrlEncodedFormEntity(parameters));
            request.setHeader(ACCEPT_CHARSET, "UTF-8");
            request.setHeader(CONTENT_TYPE, "application/x-www-form-urlencoded");

            CloseableHttpResponse response = httpClient.execute(request);
            storeHttpResponse(response);
        } catch (Exception ex) {
            fail(ex);
        }
    }

    @When("we send an HTTP POST request to {string} with the following parameters:")
    public void weSendAnHTTPPOSTRequestToWithTheFollowingParameters(String url, DataTable table) throws Exception {
        Map<String, Object> json = new LinkedHashMap<>();
        List<Map<String, String>> rows = table.asMaps(String.class, String.class);
        for (Map<String, String> columns : rows) {
            String name = checkNotNull(columns.get("Name"), "Missing \"Name\" column.");
            String value = StringUtils.trimToEmpty(columns.get("Value"));

            json.put(name, value);
        }
        String payload = objectMapper.writeValueAsString(json);

        weSendAnHTTPPOSTRequestToWithTheFollowingPayload(url, payload);
    }

    @When("we send an HTTP POST request to {string} with the following payload:")
    public void weSendAnHTTPPOSTRequestToWithTheFollowingPayload(String url, String payload) {
        try (CloseableHttpClient httpClient = getHttpClient()) {
            HttpPost request = new HttpPost(getBaseUrl(testServer.getPort()) + url);
            request.setEntity(new StringEntity(payload));
            request.setHeader(CONTENT_TYPE, "application/json");

            CloseableHttpResponse response = httpClient.execute(request);
            storeHttpResponse(response);
        } catch (Exception ex) {
            fail(ex);
        }
    }

    private void weSendHttpGetTo(@Nonnull URI uri) {
        try (CloseableHttpClient httpClient = getHttpClient()) {
            HttpGet request = new HttpGet(uri);

            CloseableHttpResponse response = httpClient.execute(request);
            storeHttpResponse(response);
        } catch (Exception ex) {
            fail(ex);
        }
    }

    @When("we send an HTTP GET request to {string}")
    public void weSendAnHTTPGETRequestTo(String url) {
        weSendHttpGetTo(URI.create(getBaseUrl(testServer.getPort()) + url));
    }

    @When("we send an HTTP GET request to {string} with the following query parameters:")
    public void weSendAnHTTPGETRequestToWithTheFollowingQueryParameters(String url, DataTable table) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(getBaseUrl(testServer.getPort()) + url);

        List<Map<String, String>> rows = table.asMaps(String.class, String.class);
        for (Map<String, String> columns : rows) {
            String name = checkNotNull(columns.get("Name"), "Missing \"Name\" column.");
            String value = StringUtils.trimToEmpty(columns.get("Value"));
            uriBuilder.addParameter(name, value);
        }

        URI uri = uriBuilder.build();
        weSendHttpGetTo(uri);
    }

    private void storeHttpResponse(CloseableHttpResponse response) {
        checkNotNull(response, "No HTTP response");

        sow.setValue(HTTP_RESPONSE_CODE, response.getStatusLine().getStatusCode());
        sow.setValue(HTTP_RESPONSE, response);
    }

    @When("we delete the schema {string}")
    public void weDeleteTheSchema(String schemaName) throws SchemaNotFoundException {
        schemaRegistryClient.deleteSchema(schemaName);
    }

    @When("we initialize a new schema registry client")
    public void weInitializeANewSchemaRegistryClient() {
        schemaRegistryClient = createSchemaRegistryClient(testServer.getPort());
    }

    @When("we check the compatibility of schema {string} with the following Avro test:")
    public void weCheckTheCompatibilityOfSchemaWithTheFollowingAvroTest(String schemaName, String avroText) throws Exception {
        CompatibilityResult result = schemaRegistryClient.checkCompatibility(schemaName, avroText);
        sow.setValue(COMPATIBILITY, result.isCompatible());
    }

    @When("we delete the last version of schema {string}")
    public void weDeleteTheSecondVersionOfSchema(String schemaName) throws Exception {
        Long versionId = (Long) sow.getValue(SCHEMA_VERSION_ID);
        assertNotNull(versionId);

        schemaRegistryClient.deleteSchemaVersion(versionId);
    }

    @When("we create the model in Atlas")
    public void weCreateTheModelInAtlas() {

    }

    @When("we export all schemas")
    public void weExportAllSchemas() {
        weSearchForAggregatedSchemas();
    }

    private String unquote(String quoted) {
        return quoted.substring(1, quoted.length() - 1);
    }

    private TreeNode skipFirstObjectThenParse(String concatedJson) throws IOException {
        ObjectReader reader = objectMapper.reader();
        JsonParser parser = reader.createParser(new StringReader(concatedJson));
        parser.readValueAsTree();
        return parser.readValueAsTree();
    }

    @When("we import that into the Schema Registry as a {string} format schema")
    public void weImportThatExportIntoTheSchemaRegistryAsASchema(String schemaType) {
        String typeFlag = "0";
        if (schemaType.toLowerCase().startsWith("confluent")) {
            typeFlag = "1";
        }

        String url = "/api/v1/schemaregistry/import/";
        Charset charset = UTF_8;

        try (CloseableHttpClient httpClient = getHttpClient()) {

            URIBuilder builder = new URIBuilder(getBaseUrl(testServer.getPort()));
            builder
                    .setPath(url)
                    .setParameter("format", typeFlag)
                    .setParameter("failOnError", "true");
            HttpPost request = new HttpPost(builder.build());

            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder
                    .create()
                    .addBinaryBody(
                            "file",
                            new ByteArrayInputStream(getImportFile().getBytes(charset)),
                            MULTIPART_FORM_DATA,
                            "file"
                    );
            request.setEntity(entityBuilder.build());
            request.setHeader(ACCEPT_CHARSET, charset.name());

            CloseableHttpResponse response = httpClient.execute(request);
            storeHttpResponse(response);
        } catch (Exception ex) {
            fail(ex);
        }
    }

    private String getImportFile() throws JsonProcessingException {
        String importFile;
        Object schemaExport = sow.getValue(SCHEMA_EXPORT);
        if (schemaExport instanceof String) {
            importFile = (String) schemaExport;
        } else if (schemaExport instanceof AggregatedSchemaMetadataInfo) {
            importFile = objectMapper.writeValueAsString(
                    CollectionResponse.newResponse()
                            .entities(singletonList((AggregatedSchemaMetadataInfo) schemaExport))
                            .build()
            );
        } else {
            importFile = objectMapper.writeValueAsString(schemaExport);
        }
        return importFile;
    }

    @Given("an import file {string}")
    public void givenAnImportFile(String fileName) throws URISyntaxException, IOException {
        String export = loadTestData(fileName);
        sow.setValue(SCHEMA_EXPORT, export);
    }

}
