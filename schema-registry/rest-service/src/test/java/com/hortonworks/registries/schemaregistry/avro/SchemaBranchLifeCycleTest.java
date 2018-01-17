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
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

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


    @Test (expected = SchemaNotFoundException.class)
    public void getAllBranchesForInvalidSchemaName() throws SchemaNotFoundException {
        schemaRegistryClient.getSchemaBranches(testNameRule.getMethodName());
    }

    @Test
    public void getAllBranches() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion masterSchemaIdVersion2 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device-incompat.avsc");
        SchemaBranch schemaBranch2 = addSchemaBranch("BRANCH2", schemaMetadata, masterSchemaIdVersion2.getSchemaVersionId());

        Set<String> actualSchemaBranches = schemaRegistryClient.getSchemaBranches(schemaMetadata.getName()).stream().map(branch -> branch.getName()).collect(Collectors.toSet());
        Set<String> expectedSchemaBranches = new HashSet<>(Lists.newArrayList("MASTER", schemaBranch1.getName(), schemaBranch2.getName()));

        Assert.assertTrue(SetUtils.isEqualSet(actualSchemaBranches, expectedSchemaBranches));
    }

    @Test (expected = SchemaNotFoundException.class)
    public void createBranchWithInvalidSchemaId() throws SchemaNotFoundException, SchemaBranchAlreadyExistsException {
         SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);
         addSchemaBranch("BRANCH1", schemaMetadata, 1l);
    }

    @Test (expected = SchemaBranchAlreadyExistsException.class)
    public void createAlreadyExistingBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion masterSchemaIdVersion2 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device-incompat.avsc");
        addSchemaBranch(schemaBranch1.getName(), schemaMetadata, masterSchemaIdVersion2.getSchemaVersionId());
    }

    @Test
    public void addSchemaVersionToBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-compat.avsc");

        Collection<SchemaVersionInfo> schemaBranch1VersionInfos = schemaRegistryClient.getAllVersions(schemaBranch1.getName(), schemaMetadata.getName());
        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());
        Assert.assertTrue(masterSchemaVersionInfos.size() == 1);
        Assert.assertTrue(schemaBranch1VersionInfos.size() == 3);

        Long versionsInInitiatedState = schemaBranch1VersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.INITIATED.getId())).count();
        Long versionsInEnabledState = schemaBranch1VersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).count();
        Assert.assertTrue(versionsInInitiatedState == 2);
        Assert.assertTrue(versionsInEnabledState == 1);
    }

    @Test (expected = IncompatibleSchemaException.class)
    public void addIncompatibleSchemaToBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BACKWARD);

        SchemaIdVersion masterSchemaIdVersion = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
    }

    @Test (expected = InvalidSchemaException.class)
    public void addInvalidSchemaToBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.BACKWARD);

        SchemaIdVersion masterSchemaIdVersion = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion.getSchemaVersionId());
        String schema2 = "--- invalid schema ---";
        schemaRegistryClient.addSchemaVersion(schemaBranch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version"));

    }

    @Test
    public void mergeSchemaWithDefaultMergeStrategy() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.toString(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        SchemaIdVersion schemaBranch1Version2 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-compat.avsc");

        schemaRegistryClient.mergeSchemaVersion(schemaBranch1Version2.getSchemaVersionId());

        Collection<SchemaVersionInfo> branchSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaBranch1.getName(), schemaMetadata.getName());
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
    public void mergeSchemaWhenRootVersionIsNotLatest() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device-compat.avsc");

        schemaRegistryClient.mergeSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

    }

    @Test (expected = SchemaNotFoundException.class)
    public void mergeSchemaWithInvalidSchemaVersion() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        schemaRegistryClient.mergeSchemaVersion(schemaBranch1Version1.getSchemaVersionId()+1);
    }

    @Test
    public void deleteSchemaBranch() throws SchemaNotFoundException, SchemaBranchAlreadyExistsException, IOException, InvalidSchemaException, IncompatibleSchemaException, SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-compat.avsc");

        Assert.assertTrue(schemaRegistryClient.getSchemaBranches(schemaMetadata.getName()).size() == 2);

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId());

        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());
        Assert.assertTrue(masterSchemaVersionInfos.size() == 1);
        Assert.assertTrue(schemaRegistryClient.getSchemaBranches(schemaMetadata.getName()).size() == 1);
    }


    @Test (expected = SchemaBranchNotFoundException.class)
    public void deleteInvalidSchemaBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, InvalidSchemaBranchDeletionException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId() + 1);
    }

    @Test (expected = InvalidSchemaBranchDeletionException.class)
    public void deleteMasterBranch() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, InvalidSchemaBranchDeletionException {
        addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        schemaRegistryClient.deleteSchemaBranch(1l);
    }

    @Test (expected = InvalidSchemaBranchDeletionException.class)
    public void deleteBranchWithEnabledSchema() throws IOException, SchemaBranchNotFoundException, InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, SchemaBranchAlreadyExistsException, InvalidSchemaBranchDeletionException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        schemaRegistryClient.enableSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId());
    }

    @Test
    public void deleteBranchWithArchivedSchema() throws InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, IOException, SchemaBranchAlreadyExistsException, SchemaLifecycleException, InvalidSchemaBranchDeletionException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        schemaRegistryClient.enableSchemaVersion(schemaBranch1Version1.getSchemaVersionId());
        schemaRegistryClient.archiveSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId());
    }


    @Test (expected = InvalidSchemaBranchDeletionException.class)
    public void deleteBranchWithRootVersionOfAnotherBranch() throws InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, IOException, SchemaBranchAlreadyExistsException, SchemaLifecycleException, InvalidSchemaBranchDeletionException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaBranch("CHILD-BRANCH", schemaMetadata,schemaBranch1Version1.getSchemaVersionId());

        schemaRegistryClient.enableSchemaVersion(schemaBranch1Version1.getSchemaVersionId());
        schemaRegistryClient.archiveSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId());
    }

    @Test (expected = SchemaLifecycleException.class)
    public void deleteRootSchemaOfBranch() throws InvalidSchemaException, SchemaNotFoundException, IncompatibleSchemaException, IOException, SchemaBranchAlreadyExistsException, SchemaLifecycleException {
        SchemaMetadata schemaMetadata = addSchemaMetadata(testNameRule.getMethodName(), SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());

        schemaRegistryClient.archiveSchemaVersion(masterSchemaIdVersion1.getSchemaVersionId());

        schemaRegistryClient.deleteSchemaVersion(masterSchemaIdVersion1.getSchemaVersionId());
    }

     // Utils

    private SchemaBranch addSchemaBranch(String schemaBranchName, SchemaMetadata schemaMetadata, Long versionId) throws SchemaNotFoundException, SchemaBranchAlreadyExistsException {
        SchemaBranch branch = new SchemaBranch(schemaBranchName, schemaMetadata.getName());
        return schemaRegistryClient.createSchemaBranch(versionId, branch);
    }

    private SchemaIdVersion addSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, String pathToSchema) throws IOException, SchemaNotFoundException, InvalidSchemaException, IncompatibleSchemaException {
        String schema = AvroSchemaRegistryClientUtil.getSchema(pathToSchema);
        SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaBranchName, schemaMetadata, new SchemaVersion(schema, "schema version description"));
        return schemaIdVersion;
    }

    private SchemaMetadata addSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        SchemaMetadata schemaMetadata = buildSchemaMetadata(schemaDesc, compatibility);
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        return schemaMetadata;
    }

    private SchemaMetadata buildSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(schemaDesc + "-schema")
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(schemaDesc + "-group")
                .description("Schema for " + schemaDesc)
                .compatibility(compatibility)
                .build();
    }
}
