/**
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
import com.cloudera.dim.schemaregistry.TestSchemaRegistryServer;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.cloudera.dim.schemaregistry.GlobalState.AGGREGATED_SCHEMAS;
import static com.cloudera.dim.schemaregistry.GlobalState.HTTP_RESPONSE_CODE;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_ID;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_NAME;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_VERSION_ID;
import static com.cloudera.dim.schemaregistry.GlobalState.SCHEMA_VERSION_NO;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.http.protocol.HttpCoreContext.HTTP_RESPONSE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class WhenSteps extends AbstractSteps {

    private static final Logger LOG = LoggerFactory.getLogger(WhenSteps.class);

    public WhenSteps(TestSchemaRegistryServer testServer, GlobalState sow) {
        super(testServer, sow);
    }

    @Given("that Schema Registry is running")
    public void thatSchemaRegistryIsRunning() throws Exception {
        sow.clear();
        if (!testServer.isRunning()) {
            testServer.start();   // start schema registry server
        }
        testServer.cleanupDb();
        schemaRegistryClient = createSchemaRegistryClient(testServer.getPort());
    }

    @When("we create a new schema meta {string} with the following parameters:")
    public void weCreateANewSchemaMetaWithTheFollowingParameters(String schemaName, DataTable table) {
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

        SchemaMetadata meta = builder.build();
        Long id = schemaRegistryClient.addSchemaMetadata(meta);
        sow.setValue(SCHEMA_NAME, schemaName);
        sow.setValue(SCHEMA_ID, id);

        LOG.info("New schema [{}] was created with id {}", schemaName, id);
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
            request.setHeader("Accept-Charset", "UTF-8");
            request.setHeader("Content-Type", "application/x-www-form-urlencoded");

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
            request.setHeader("Content-Type", "application/json");

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
}
