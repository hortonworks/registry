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
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import org.apache.commons.collections4.SetUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaBranchLifeCycleTest {

    private static SchemaRegistryTestServerClientWrapper SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER;
    private static SchemaRegistryClient schemaRegistryClient;

    private String testName;

    @BeforeEach
    void init(TestInfo testInfo) {
        testName = testInfo.getTestMethod().get().getName();
    }


    public static Stream<SchemaRegistryTestProfileType> profiles() {
        return Stream.of(SchemaRegistryTestProfileType.DEFAULT, SchemaRegistryTestProfileType.SSL);
    }
    
    public void beforeParam(SchemaRegistryTestProfileType schemaRegistryTestProfileType) throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER = new SchemaRegistryTestServerClientWrapper(schemaRegistryTestProfileType);
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.startTestServer();
        schemaRegistryClient = SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.getClient();
    }

    @AfterEach
    public void tearDown() throws Exception {
        SCHEMA_REGISTRY_TEST_SERVER_CLIENT_WRAPPER.stopTestServer();
    }


    @ParameterizedTest
    @MethodSource("profiles")
    public void getAllBranchesForInvalidSchemaName(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        Assertions.assertThrows(SchemaNotFoundException.class, () -> schemaRegistryClient.getSchemaBranches(testName));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void getAllBranches(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion masterSchemaIdVersion2 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device-incompat.avsc");
        SchemaBranch schemaBranch2 = addSchemaBranch("BRANCH2", schemaMetadata, masterSchemaIdVersion2.getSchemaVersionId());

        Set<String> actualSchemaBranches = schemaRegistryClient.getSchemaBranches(schemaMetadata.getName()).stream().map(branch -> branch.getName()).collect(Collectors.toSet());
        Set<String> expectedSchemaBranches = new HashSet<>(Lists.newArrayList("MASTER", schemaBranch1.getName(), schemaBranch2.getName()));

        Assertions.assertTrue(SetUtils.isEqualSet(actualSchemaBranches, expectedSchemaBranches));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void createBranchWithInvalidSchemaId(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);
        Assertions.assertThrows(SchemaNotFoundException.class, () -> addSchemaBranch("BRANCH1", schemaMetadata, 1L));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void createAlreadyExistingBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion masterSchemaIdVersion2 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device-incompat.avsc");
        Assertions.assertThrows(SchemaBranchAlreadyExistsException.class, () -> addSchemaBranch(schemaBranch1.getName(), schemaMetadata, masterSchemaIdVersion2.getSchemaVersionId()));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void addSchemaVersionToBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-compat.avsc");

        Collection<SchemaVersionInfo> schemaBranch1VersionInfos = schemaRegistryClient.getAllVersions(schemaBranch1.getName(), schemaMetadata.getName());
        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());
        Assertions.assertTrue(masterSchemaVersionInfos.size() == 1);
        Assertions.assertTrue(schemaBranch1VersionInfos.size() == 3);

        Long versionsInInitiatedState = schemaBranch1VersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.INITIATED.getId())).count();
        Long versionsInEnabledState = schemaBranch1VersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).count();
        Assertions.assertTrue(versionsInInitiatedState == 2);
        Assertions.assertTrue(versionsInEnabledState == 1);
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void addIncompatibleSchemaToBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.BACKWARD);

        SchemaIdVersion masterSchemaIdVersion = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion.getSchemaVersionId());
        Assertions.assertThrows(IncompatibleSchemaException.class, () -> addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc"));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void addInvalidSchemaToBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.BACKWARD);

        SchemaIdVersion masterSchemaIdVersion = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion.getSchemaVersionId());
        String schema2 = "--- invalid schema ---";
        Assertions.assertThrows(InvalidSchemaException.class, () -> schemaRegistryClient.addSchemaVersion(schemaBranch1.getName(), schemaMetadata.getName(), new SchemaVersion(schema2, "second version")));

    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void mergeSchemaWithDefaultMergeStrategy(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        SchemaIdVersion schemaBranch1Version2 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-compat.avsc");

        schemaRegistryClient.mergeSchemaVersion(schemaBranch1Version2.getSchemaVersionId());

        Collection<SchemaVersionInfo> branchSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaBranch1.getName(), schemaMetadata.getName());
        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());
        Assertions.assertTrue(masterSchemaVersionInfos.size() == 2);
        Assertions.assertTrue(branchSchemaVersionInfos.size() == 3);

        Long branchVersionsInInitiatedState = branchSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.INITIATED.getId())).count();
        Long masterVersionsInEnabledState = masterSchemaVersionInfos.stream().filter(
                schemaVersionInfo -> schemaVersionInfo.getStateId().equals(SchemaVersionLifecycleStates.ENABLED.getId())).count();
        Assertions.assertTrue(branchVersionsInInitiatedState == 2);
        Assertions.assertTrue(masterVersionsInEnabledState == 2);
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void mergeSchemaWhenRootVersionIsNotLatest(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device-compat.avsc");

        schemaRegistryClient.mergeSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void mergeSchemaWithInvalidSchemaVersion(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        Assertions.assertThrows(SchemaNotFoundException.class, () -> schemaRegistryClient.mergeSchemaVersion(schemaBranch1Version1.getSchemaVersionId() + 1));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteSchemaBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-compat.avsc");

        Assertions.assertTrue(schemaRegistryClient.getSchemaBranches(schemaMetadata.getName()).size() == 2);

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId());

        Collection<SchemaVersionInfo> masterSchemaVersionInfos = schemaRegistryClient.getAllVersions(schemaMetadata.getName());
        Assertions.assertTrue(masterSchemaVersionInfos.size() == 1);
        Assertions.assertTrue(schemaRegistryClient.getSchemaBranches(schemaMetadata.getName()).size() == 1);
    }


    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteInvalidSchemaBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        Assertions.assertThrows(SchemaBranchNotFoundException.class, () -> schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId() + 1));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteMasterBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        addSchemaMetadata(testName, SchemaCompatibility.NONE);

        Assertions.assertThrows(InvalidSchemaBranchDeletionException.class, () -> schemaRegistryClient.deleteSchemaBranch(1L));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteBranchWithEnabledSchema(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        schemaRegistryClient.enableSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

        Assertions.assertThrows(InvalidSchemaBranchDeletionException.class, () -> schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId()));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteBranchWithArchivedSchema(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");

        schemaRegistryClient.enableSchemaVersion(schemaBranch1Version1.getSchemaVersionId());
        schemaRegistryClient.archiveSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

        schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId());
    }


    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteBranchWithRootVersionOfAnotherBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        SchemaBranch schemaBranch1 = addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());
        SchemaIdVersion schemaBranch1Version1 = addSchemaVersion(schemaBranch1.getName(), schemaMetadata, "/device-incompat.avsc");
        addSchemaBranch("CHILD-BRANCH", schemaMetadata, schemaBranch1Version1.getSchemaVersionId());

        schemaRegistryClient.enableSchemaVersion(schemaBranch1Version1.getSchemaVersionId());
        schemaRegistryClient.archiveSchemaVersion(schemaBranch1Version1.getSchemaVersionId());

        Assertions.assertThrows(InvalidSchemaBranchDeletionException.class, () -> schemaRegistryClient.deleteSchemaBranch(schemaBranch1.getId()));
    }

    @ParameterizedTest
    @MethodSource("profiles")
    public void deleteRootSchemaOfBranch(SchemaRegistryTestProfileType profile) throws Exception {
        beforeParam(profile);
        SchemaMetadata schemaMetadata = addSchemaMetadata(testName, SchemaCompatibility.NONE);

        SchemaIdVersion masterSchemaIdVersion1 = addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, "/device.avsc");
        addSchemaBranch("BRANCH1", schemaMetadata, masterSchemaIdVersion1.getSchemaVersionId());

        schemaRegistryClient.archiveSchemaVersion(masterSchemaIdVersion1.getSchemaVersionId());

        Assertions.assertThrows(SchemaLifecycleException.class, () -> schemaRegistryClient.deleteSchemaVersion(masterSchemaIdVersion1.getSchemaVersionId()));
    }

     // AuthorizationUtils

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
