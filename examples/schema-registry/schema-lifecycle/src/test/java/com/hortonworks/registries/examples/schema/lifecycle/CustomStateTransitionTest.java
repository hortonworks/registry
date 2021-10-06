/**
 * Copyright 2017-2019 Cloudera, Inc.
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

package com.hortonworks.registries.examples.schema.lifecycle;

import com.hortonworks.registries.examples.schema.lifecycle.state.CustomReviewCycleStates;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class CustomStateTransitionTest {

    private static final Logger LOG = LoggerFactory.getLogger(CustomStateTransitionTest.class);

    private SchemaLifecycleApp schemaRegistryTestServer;
    private SchemaRegistryClient schemaRegistryClient;

    @BeforeEach
    public void startUp() throws Exception {
        String serverYAMLPath = CustomStateTransitionTest.class.getClassLoader().getResource("registry-lifecycle-test-example.yaml").getPath();
        String clientYAMLPath = CustomStateTransitionTest.class.getClassLoader().getResource("registry-lifecycle-client.yaml").getPath();
        schemaRegistryTestServer = new SchemaLifecycleApp(serverYAMLPath);
        schemaRegistryTestServer.startUp();
        schemaRegistryClient = schemaRegistryTestServer.getClient(clientYAMLPath);
    }

    @AfterEach
    public void tearDown() throws Exception {
        schemaRegistryTestServer.stop();
    }

    @Test
    public void testTransitionToRejectedState()
            throws IOException, SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException,
            SchemaBranchAlreadyExistsException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = createSchemaMetadata("test", SchemaCompatibility.NONE);
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", schemaMetadata.getName());

        String schema1 = getSchema("/device.avsc");
        LOG.info("Creating new schema with name \"{}\"", schemaMetadata.getName());
        Long id = schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        LOG.info("ID of the new schema is {}. Adding new version to it.", id);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));
        LOG.info("ID of the first version is {}", schemaIdVersion1);

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = getSchema("/device1.avsc");
        LOG.info("Creating second version for the schema \"{}\"", schemaMetadata.getName());
        SchemaIdVersion schemaIdVersion2 = schemaRegistryClient
                .addSchemaVersion(branch1.getName(), schemaMetadata, new SchemaVersion(schema2, "modify version"));
        LOG.info("ID of the second version is {}", schemaIdVersion2);

        LOG.info("Starting review of the second version with ID {} (new state: {})",
                schemaIdVersion2.getSchemaVersionId(), CustomReviewCycleStates.PEER_REVIEW_STATE.getId());
        schemaRegistryClient.startSchemaVersionReview(schemaIdVersion2.getSchemaVersionId());
        SchemaVersionInfo schemaVersionInfoAtReviewState = schemaRegistryClient
                .getSchemaVersionInfo(new SchemaIdVersion(schemaIdVersion2.getSchemaVersionId()));
        LOG.info("1) Second version's state is now: {}", schemaVersionInfoAtReviewState);
        Assertions.assertEquals(CustomReviewCycleStates.PEER_REVIEW_STATE.getId(), schemaVersionInfoAtReviewState.getStateId());

        LOG.info("Transitioning second version into REJECTED ({}) state.", CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId());
        schemaRegistryClient.transitionState(schemaIdVersion2.getSchemaVersionId(),
                CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(), "Rejected by X".getBytes());
        SchemaVersionInfo schemaVersionInfoAtRejectedState = schemaRegistryClient
                .getSchemaVersionInfo(new SchemaIdVersion(schemaIdVersion2.getSchemaVersionId()));
        LOG.info("2) Second version's state is now: {}", schemaVersionInfoAtRejectedState);
        Assertions.assertEquals(CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(), schemaVersionInfoAtRejectedState.getStateId());

    }

    private SchemaMetadata createSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(schemaDesc + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(schemaDesc + "-group")
                .description("Schema for " + schemaDesc)
                .compatibility(compatibility)
                .build();
    }

    public String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = CustomStateTransitionTest.class.getResourceAsStream(schemaFileName);
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

}
