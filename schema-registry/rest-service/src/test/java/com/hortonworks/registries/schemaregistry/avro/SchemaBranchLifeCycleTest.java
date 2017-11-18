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

package com.hortonworks.registries.schemaregistry.avro;

import com.google.common.collect.Lists;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.avro.conf.SchemaRegistryTestProfileType;
import com.hortonworks.registries.schemaregistry.avro.helper.SchemaRegistryTestServerClientWrapper;
import com.hortonworks.registries.schemaregistry.avro.util.AvroSchemaRegistryClientUtil;
import com.hortonworks.registries.schemaregistry.avro.util.CustomParameterizedRunner;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.util.SchemaRegistryUtil;
import org.apache.commons.collections.SetUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

//TODO : Refactor redundant code across the tests

@RunWith(CustomParameterizedRunner.class)
public class SchemaBranchLifeCycleTest {

    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;
    private static SchemaRegistryClient schemaRegistryClient;

    @Rule
    public TestName testNameRule = new TestName();

    @CustomParameterizedRunner.Parameters
    public static Iterable<SchemaRegistryTestProfileType> profiles() {
        return Arrays.asList(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }

    @CustomParameterizedRunner.BeforeParam
    public static void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
    }

    @Before
    public void startUp () throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        schemaRegistryClient = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient();
    }

    @After
    public void tearDown() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }

    public SchemaBranchLifeCycleTest(SchemaRegistryTestProfileType schemaRegistryTestProfileType) {

    }

    @Test
    public void getAllBranchesWithOnlyMaster() throws Exception {

        SchemaBranch masterBranch = new SchemaBranch(SchemaRegistryUtil.MASTER_BRANCH_NAME, SchemaRegistryUtil.MASTER_BRANCH_DESCRIPTION, null);
        Collection <SchemaBranch> schemaBranches = Collections.singletonList(masterBranch);
        Assert.assertEquals(schemaBranches, schemaRegistryClient.getAllBranches());
    }

    @Test
    public void getAllBranches() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {

        SchemaBranch masterBranch = new SchemaBranch(SchemaRegistryUtil.MASTER_BRANCH_NAME, SchemaRegistryUtil.MASTER_BRANCH_DESCRIPTION, null);
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);
        SchemaBranch branch2 = new SchemaBranch("BRANCH2", "desc2", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        SchemaIdVersion schemaIdVersion2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema2, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion2.getSchemaVersionId(), branch2);

        Set<String> actualSchemaBranches = schemaRegistryClient.getAllBranches().stream().map(branch -> branch.getName()).collect(Collectors.toSet());
        Set<String> expectedSchemaBranches = new HashSet<>(Lists.newArrayList(masterBranch.getName(), branch1.getName(), branch2.getName()));

        Assert.assertTrue(SetUtils.isEqualSet(actualSchemaBranches, expectedSchemaBranches));

    }

    @Test (expected = SchemaNotFoundException.class)
    public void createBranchWithInvalidSchemaId() throws SchemaNotFoundException, SchemaBranchAlreadyExistsException {

        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);
        schemaRegistryClient.createSchemaBranch(1l, branch1);
    }

    @Test (expected = SchemaBranchAlreadyExistsException.class)
    public void createAlreadyExistingBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {

        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        SchemaIdVersion schemaIdVersion2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema2, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion2.getSchemaVersionId(), branch1);
    }

    @Test
    public void addSchemaVersionToBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {

        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema3, "third version"));

        Collection<SchemaVersionInfo> branchSchemaVersionInfos = schemaRegistryClient.getAllVersions(branch1.getName(), schemaMetadata.getName());
        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());

        Assert.assertTrue(masterSchemaVersionInfos.size() == 1);
        Assert.assertTrue(branchSchemaVersionInfos.size() == 3);

        Long versionsInInitiatedState = branchSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.INITIATED.getId())).count();

        Long versionsInEnabledState = branchSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).count();

        Assert.assertTrue(versionsInInitiatedState == 2);
        Assert.assertTrue(versionsInEnabledState == 1);
    }

    @Test (expected = IncompatibleSchemaException.class)
    public void addIncompatibleSchemaToBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BACKWARD);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));
    }

    @Test (expected = InvalidSchemaException.class)
    public void addInvalidSchemaToBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BACKWARD);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = "--- invalid schema ---";
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

    }

    @Test
    public void mergeSchemaWithOptimisticStrategy() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(branch1.getName(),
                schemaMetadata.getName(), new SchemaVersion(schema3, "second version"));

        schemaRegistryClient.mergeSchemaVersion(schemaIdVersion.getSchemaVersionId(), SchemaVersionMergeStrategy.OPTIMISTIC);

        Collection<SchemaVersionInfo> branchSchemaVersionInfos = schemaRegistryClient.getAllVersions(branch1.getName(), schemaMetadata.getName());
        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());

        Assert.assertTrue(masterSchemaVersionInfos.size() == 2);
        Assert.assertTrue(branchSchemaVersionInfos.size() == 3);

        Long branchVersionsInInitiatedState = branchSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.INITIATED.getId())).count();

        Long masterVersionsInEnabledState = masterSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).count();

        Assert.assertTrue(branchVersionsInInitiatedState == 2);
        Assert.assertTrue(masterVersionsInEnabledState == 2);


    }

    @Test
    public void mergeSchemaWithPessimisticStrategy() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(branch1.getName(),
                schemaMetadata.getName(), new SchemaVersion(schema3, "second version"));

        schemaRegistryClient.mergeSchemaVersion(schemaIdVersion.getSchemaVersionId(), SchemaVersionMergeStrategy.PESSIMISTIC);

        Collection<SchemaVersionInfo> branchSchemaVersionInfos = schemaRegistryClient.getAllVersions(branch1.getName(), schemaMetadata.getName());
        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());

        Assert.assertTrue(masterSchemaVersionInfos.size() == 2);
        Assert.assertTrue(branchSchemaVersionInfos.size() == 3);

        Long branchVersionsInInitiatedState = branchSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.INITIATED.getId())).count();

        Long masterVersionsInEnabledState = masterSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).count();

        Assert.assertTrue(branchVersionsInInitiatedState == 2);
        Assert.assertTrue(masterVersionsInEnabledState == 2);

    }

    @Test (expected = RuntimeException.class)
    public void mergeSchemaWithPessimisticStrategyWhenRootVersionHasChanged() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        schemaRegistryClient.addSchemaVersion(schemaMetadata.getName(),
                new SchemaVersion(schema3, "second version"));

        schemaRegistryClient.mergeSchemaVersion(schemaIdVersion.getSchemaVersionId(), SchemaVersionMergeStrategy.PESSIMISTIC);

    }

    @Test
    public void mergeSchemaWithOptimisticStrategyWhenRootVersionHasChanged() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        schemaRegistryClient.addSchemaVersion(schemaMetadata.getName(),
                new SchemaVersion(schema3, "second version"));

        schemaRegistryClient.mergeSchemaVersion(schemaIdVersion.getSchemaVersionId(), SchemaVersionMergeStrategy.OPTIMISTIC);

    }

    @Test (expected = SchemaNotFoundException.class)
    public void mergeSchemaWithInvalidSchemaVersion() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata.getName(),
                new SchemaVersion(schema3, "second version"));

        schemaRegistryClient.mergeSchemaVersion(schemaIdVersion.getSchemaVersionId()+1, SchemaVersionMergeStrategy.OPTIMISTIC);

    }

    @Test
    public void deleteSchemaBranch() throws SchemaNotFoundException, SchemaBranchAlreadyExistsException, IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        SchemaBranch persistedSchemaBranch = schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(),
                new SchemaVersion(schema3, "second version"));

        Collection<SchemaVersionInfo> branchSchemaVersionInfos = schemaRegistryClient.getAllVersions(branch1.getName(), schemaMetadata.getName());

        Assert.assertTrue(schemaRegistryClient.getAllBranches().size() == 2);

        schemaRegistryClient.deleteSchemaBranch(persistedSchemaBranch.getId());

        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());

        Assert.assertTrue(masterSchemaVersionInfos.size() == 1);
        Assert.assertTrue(schemaRegistryClient.getAllBranches().size() == 1);
    }


    @Test (expected = SchemaBranchNotFoundException.class)
    public void deleteInvalidSchemaBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, InvalidSchemaBranchDeletionException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        SchemaBranch createdSchemaBranch = schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        schemaRegistryClient.deleteSchemaBranch(createdSchemaBranch.getId() + 1);
    }

    @Test (expected = InvalidSchemaBranchDeletionException.class)
    public void deleteMasterBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, InvalidSchemaBranchDeletionException {
        schemaRegistryClient.deleteSchemaBranch(1l);
    }

    @Test (expected = InvalidSchemaBranchDeletionException.class)
    public void deleteBranchWithEnabledSchema() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, InvalidSchemaBranchDeletionException, SchemaLifecycleException {
        SchemaBranch branch1 = new SchemaBranch("BRANCH1", "desc1", null);

        SchemaMetadata schemaMetadata = createSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
        String schema1 = AvroSchemaRegistryClientUtil.getSchema("/device.avsc");
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        SchemaIdVersion schemaIdVersion1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "initial version"));

        SchemaBranch persistedSchemaBranch = schemaRegistryClient.createSchemaBranch(schemaIdVersion1.getSchemaVersionId(), branch1);

        String schema2 = AvroSchemaRegistryClientUtil.getSchema("/device-incompat.avsc");
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

        String schema3 = AvroSchemaRegistryClientUtil.getSchema("/device-compat.avsc");
        schemaRegistryClient.addSchemaVersion(branch1.getName(), schemaMetadata.getName(),
                new SchemaVersion(schema3, "second version"));

        schemaRegistryClient.enableSchemaVersion(schemaIdVersion.getSchemaVersionId());

        schemaRegistryClient.deleteSchemaBranch(persistedSchemaBranch.getId());
    }

    private SchemaMetadata createSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(schemaDesc + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(schemaDesc + "-group")
                .description("Schema for " + schemaDesc)
                .compatibility(compatibility)
                .build();
    }
}
