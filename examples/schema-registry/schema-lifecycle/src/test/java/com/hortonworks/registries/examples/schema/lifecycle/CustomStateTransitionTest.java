/**
 * Copyright 2017 Hortonworks.
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
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestConfiguration;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class CustomStateTransitionTest {

    private SchemaRegistryTestServerClientWrapper schemaRegistryTestServerClientWrapper;
    private SchemaRegistryClient schemaRegistryClient;

    @Before
    public void startUp() throws Exception {
        String serverYAMLPath = CustomStateTransitionTest.class.getClassLoader().getResource("registry-lifecycle-test-example.yaml").getPath();
        String clientYAMLPath = CustomStateTransitionTest.class.getClassLoader().getResource("registry-lifecycle-client.yaml").getPath();
        schemaRegistryTestServerClientWrapper = new SchemaRegistryTestServerClientWrapper(new SchemaRegistryTestConfiguration(serverYAMLPath,clientYAMLPath));
        schemaRegistryTestServerClientWrapper.startTestServer();
        schemaRegistryClient = schemaRegistryTestServerClientWrapper.getClient();
    }

    @After
    public void tearDown() throws Exception {
        schemaRegistryTestServerClientWrapper.stopTestServer();
    }

    @Test
    public void testTransitionToRejectedState() throws IOException, SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = createSchemaMetadata("test", SchemaCompatibility.NONE);
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", schemaMetadata.getName());

        String schema1 = getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device1.avsc");
        SchemaIdVersion schemaIdVersion2 = schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata, new SchemaVersion(schema2, "modify version"));

        schemaRegistryClient.startSchemaVersionReview(schemaIdVersion2.getSchemaVersionId());
        SchemaVersionInfo schemaVersionInfoAtReviewState = schemaRegistryClient.getSchemaVersionInfo(new SchemaIdVersion(schemaIdVersion2.getSchemaVersionId()));
        Assert.assertTrue(schemaVersionInfoAtReviewState.getStateId().equals(CustomReviewCycleStates.PEER_REVIEW_STATE.getId()));

        schemaRegistryClient.transitionState(schemaIdVersion2.getSchemaVersionId(), CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId(), "Rejected by X".getBytes());
        SchemaVersionInfo schemaVersionInfoAtRejectedState = schemaRegistryClient.getSchemaVersionInfo(new SchemaIdVersion(schemaIdVersion2.getSchemaVersionId()));
        Assert.assertTrue(schemaVersionInfoAtRejectedState.getStateId().equals(CustomReviewCycleStates.REJECTED_REVIEW_STATE.getId()));

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
