/*
 * Copyright 2016-2019 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.authorizer.agent;


import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.HAServerNotificationManager;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.authorizer.agent.util.SchemaTextStore;
import com.hortonworks.registries.schemaregistry.authorizer.agent.util.SecurityContextForTesting;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.test.TestAuthorizer;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(BlockJUnit4ClassRunner.class)
@Category(IntegrationTest.class)
public class DefaultAuthorizationAgentTest {


    private static AuthorizationAgent authorizationAgent;
    private static ISchemaRegistry schemaRegistry;
    private static SchemaIdVersion siv3;
    private static SchemaIdVersion siv31;
    private static SchemaIdVersion siv4;
    private static SchemaBranch branch3;


    //TODO: Add tests for UserGroupInformation user groups calculation

    @BeforeClass
    public static void setUp() throws SchemaNotFoundException,
            InvalidSchemaException,
            IncompatibleSchemaException,
            SchemaBranchAlreadyExistsException {
        ///////////////////////////////// SchemaRegistry instantiation ////////////////

        Collection<Map<String, Object>> schemaProvidersConfig =
                Collections.singleton(Collections.singletonMap("providerClass",
                        AvroSchemaProvider.class.getName()));
        StorageManager storageManager = new InMemoryStorageManager();

        schemaRegistry = new DefaultSchemaRegistry(storageManager,
                null,
                schemaProvidersConfig,
                new HAServerNotificationManager(),
                new SchemaLockManager(new NOOPTransactionManager()));
        schemaRegistry.init(Collections.<String, Object>emptyMap());



        ///////////////////////////////// SchemaRegistry initialization ////////////////

        SchemaMetadata sm3 = new SchemaMetadata
                .Builder("Schema3")
                .schemaGroup("Group3")
                .type("avro")
                .build();
        schemaRegistry.registerSchemaMetadata(sm3);

        SchemaVersion sv3 = new SchemaVersion(SchemaTextStore.SCHEMA_TEXT_3, "dummy");
        siv3 = schemaRegistry.addSchemaVersion("Schema3", sv3);

        SchemaBranch schemaBranch = new SchemaBranch("Branch3", "Schema3");
        branch3 = schemaRegistry.createSchemaBranch(siv3.getSchemaVersionId(), schemaBranch);

        SchemaVersion sv31 = new SchemaVersion(SchemaTextStore.SCHEMA_TEXT_3_1, "dummy");
        siv31 = schemaRegistry.addSchemaVersion("Branch3", "Schema3", sv31);

        SchemaMetadata sm4 = new SchemaMetadata
                .Builder("Schema4")
                .schemaGroup("Group4")
                .type("avro")
                .build();
        schemaRegistry.registerSchemaMetadata(sm4);

        SchemaVersion sv4 = new SchemaVersion(SchemaTextStore.SCHEMA_TEXT_4, "dummy");
        siv4 = schemaRegistry.addSchemaVersion("Schema4", sv4);



        ////////////////////////////// Authorizer instantiation ///////////////////////

        Map<String, Object> props = new HashMap<>();

        props.put("authorizationAgentClassName", DefaultAuthorizationAgent.class.getCanonicalName());
        props.put("authorizerClassName", TestAuthorizer.class.getCanonicalName());

        authorizationAgent = AuthorizationAgentFactory.getAuthorizationAgent(props);
        TestAuthorizer testAuthorizer = (TestAuthorizer) (((DefaultAuthorizationAgent)authorizationAgent)
                .getAuthorizer());



        ///////////////////////////////////// Policies ////////////////////////////////

        //p1
        Authorizer.Resource schemaMetadata1 = new Authorizer
                .SchemaMetadataResource("Group1", "Schema1");
        TestAuthorizer.Policy p1 = new TestAuthorizer.Policy(schemaMetadata1,
                "user1",
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p1);


        //p2
        Authorizer.Resource schemaMetadata2 = new Authorizer
                .SchemaMetadataResource("Group2", "Schema2");
        TestAuthorizer.Policy p2 = new TestAuthorizer.Policy(schemaMetadata2,
                "user1",
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p2);


        //p3
        Authorizer.Resource schemaMetadata3 = new Authorizer
                .SchemaMetadataResource("Group3", "Schema3");
        TestAuthorizer.Policy p3 = new TestAuthorizer.Policy(schemaMetadata3,
                "user3",
                Authorizer.AccessType.READ, Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p3);


        //p4
        Authorizer.Resource schemaBranch3 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch3");
        TestAuthorizer.Policy p4 = new TestAuthorizer.Policy(schemaBranch3,
                "user3",
                Authorizer.AccessType.CREATE);
        testAuthorizer.addPolicy(p4);


        //p5
        Authorizer.Resource schemaVersion3 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "MASTER");
        TestAuthorizer.Policy p5 = new TestAuthorizer.Policy(schemaVersion3,
                "user3",
                Authorizer.AccessType.READ, Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p5);

        //p6
        Authorizer.Resource schemaBranch4 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch4");
        TestAuthorizer.Policy p6 = new TestAuthorizer.Policy(schemaBranch4,
                new String[] {"user3", "user4"},
                new HashSet<>(),
                Authorizer.AccessType.CREATE);
        testAuthorizer.addPolicy(p6);


        //p7
        Authorizer.Resource schemaVersion31 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "Branch3");
        TestAuthorizer.Policy p7 = new TestAuthorizer.Policy(schemaVersion31,
                "user3",
                new HashSet<>(),
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p7);

        //p8
        TestAuthorizer.Policy p8 = new TestAuthorizer.Policy(schemaBranch3,
                new String[]{"user5", "user6"},
                new HashSet<>(),
                Authorizer.AccessType.DELETE, Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p8);

        //p9
        Authorizer.Resource schemaBranch5 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch5");
        TestAuthorizer.Policy p9 = new TestAuthorizer.Policy(schemaBranch5,
                new String[]{"user6"},
                new HashSet<>(),
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p9);

    }

    @Test
    public void authorizeFindSchemas() {

        // 1
        // Empty because initial list is empty
        String user = "user4";
        SecurityContext sc = new SecurityContextForTesting(user);
        ArrayList<SchemaMetadataInfo> schemas = new ArrayList();
        Collection<SchemaMetadataInfo> resEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resEmpty.isEmpty());

        // 2
        // Initial list contains one element, but there is no policy for current user that allows the access
        // The result list is empty again

        String user1 = "userNotAuthorized";
        SecurityContext sc1 = new SecurityContextForTesting(user1);
        schemas = new ArrayList();
        SchemaMetadata sm1 = new SchemaMetadata
                .Builder("Schema1")
                .schemaGroup("Group1")
                .type("avro")
                .build();
        SchemaMetadataInfo smi1 = new SchemaMetadataInfo(sm1);
        schemas.add(smi1);
        resEmpty = authorizationAgent.authorizeFindSchemas(sc1, schemas);
        assertTrue(resEmpty.isEmpty());

        // 3
        // Initial list contains one element, and there is a policy (p1) that allows access
        // The result list will contain one element

        String user2 = "user1";
        SecurityContext sc2 = new SecurityContextForTesting(user2);
        schemas = new ArrayList();
        SchemaMetadata sm2 = new SchemaMetadata
                .Builder("Schema1")
                .schemaGroup("Group1")
                .type("avro")
                .build();
        SchemaMetadataInfo smi2 = new SchemaMetadataInfo(sm2);
        schemas.add(smi2);
        Collection<SchemaMetadataInfo> resNonEmpty = authorizationAgent.authorizeFindSchemas(sc2, schemas);
        assertTrue(resNonEmpty.size() == 1);
        assertTrue(resNonEmpty.contains(smi2));

        // 4
        // Initial list contains two elements, and there are policies (p1, p2) that allows access
        // to all elements of the list
        // The result list will contain two elements

        String user3 = "user1";
        sc = new SecurityContextForTesting(user3);
        SchemaMetadata sm3 = new SchemaMetadata
                .Builder("Schema2")
                .schemaGroup("Group2")
                .type("avro")
                .build();
        SchemaMetadataInfo smi3 = new SchemaMetadataInfo(sm3);
        schemas.add(smi3);
        resNonEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resNonEmpty.size() == 2);
        assertTrue(resNonEmpty.contains(smi2));
        assertTrue(resNonEmpty.contains(smi3));

        // 5
        // Initial list contains three elements, and there are policies (p1, p2) that allows access
        // to two elements of the list
        // The result list will contain two elements

        String user4 = "user1";
        sc = new SecurityContextForTesting(user4);
        SchemaMetadata sm4 = new SchemaMetadata
                .Builder("Schema4")
                .schemaGroup("Group2")
                .type("avro")
                .build();
        SchemaMetadataInfo smi4 = new SchemaMetadataInfo(sm4);
        schemas.add(smi4);
        resNonEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resNonEmpty.size() == 2);
        assertTrue(resNonEmpty.contains(smi2));
        assertTrue(resNonEmpty.contains(smi3));
        assertFalse(resNonEmpty.contains(smi4));

    }


    @Test
    public void configure() {
        try {
            authorizationAgent.configure(new HashMap<>());
            fail("Expected an DefaultAuthorizationAgent.AlreadyConfiguredException to be thrown");
        } catch (DefaultAuthorizationAgent.AlreadyConfiguredException e) {
            String expectedMsg = "DefaultAuthorizationAgent is already configured";
            assertThat(e.getMessage(), is(expectedMsg));
        }
    }

    @Test
    public void authorizeGetAggregatedSchemaList() {
    }

    @Test
    public void authorizeGetAggregatedSchemaInfo() {
    }

    @Test
    public void authorizeFindSchemasByFields() {
    }

    //TODO: Add NOT_FOUND test cases
    @Test
    public void authorizeSchemaMetadata() throws AuthorizationException {

        ///////////////////////////// Positive cases /////////////////////////////////

        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);
        SchemaMetadata sm3 = new SchemaMetadata
                .Builder("Schema3")
                .schemaGroup("Group3")
                .type("avro")
                .build();
        SchemaMetadataInfo smi3 = new SchemaMetadataInfo(sm3);

        // Access allowed by policy p3
        authorizationAgent.authorizeSchemaMetadata(sc3, sm3, Authorizer.AccessType.READ);
        authorizationAgent.authorizeSchemaMetadata(sc3, smi3, Authorizer.AccessType.DELETE);
        authorizationAgent.authorizeSchemaMetadata(sc3, schemaRegistry, sm3.getName(), Authorizer.AccessType.READ);

        ///////////////////////////// Negative cases /////////////////////////////////

        // The user doen't have UPDATE and CREATE permissions
        try {
            authorizationAgent.authorizeSchemaMetadata(sc3, sm3, Authorizer.AccessType.UPDATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e){
            String expectedMsg = "User 'user3' does not have [update] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(sc3, smi3, Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(sc3, schemaRegistry, sm3.getName(), Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

        // No policy that matches the resource
        SchemaMetadata sm4 = new SchemaMetadata
                .Builder("Schema4")
                .schemaGroup("Group4")
                .type("avro")
                .build();
        SchemaMetadataInfo smi4 = new SchemaMetadataInfo(sm4);
        try {
            authorizationAgent.authorizeSchemaMetadata(sc3, sm4, Authorizer.AccessType.READ);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [read] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group4', schemaMetadataName='Schema4' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(sc3, smi4, Authorizer.AccessType.READ);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [read] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group4', schemaMetadataName='Schema4' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(sc3, schemaRegistry, sm4.getName(), Authorizer.AccessType.DELETE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [delete] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group4', schemaMetadataName='Schema4' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

    }

    @Test
    public void authorizeCreateSchemaBranch() throws AuthorizationException {

        ///////////////////////////// Positive cases /////////////////////////////////

        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);
        String schemaName3 = "Schema3";

        // Access allowed by p4 (for creating Branch3) and p5 (for reading siv3)
        authorizationAgent.authorizeCreateSchemaBranch(sc3,
                schemaRegistry,
                schemaName3,
                 new Long(siv3.getSchemaVersionId()), "Branch3");

        // Access allowed by p6 (for creating Branch3) and p7 (for reading siv31)
        authorizationAgent.authorizeCreateSchemaBranch(sc3,
                schemaRegistry,
                schemaName3,
                new Long(siv31.getSchemaVersionId()), "Branch4");

        ///////////////////////////// Negative cases /////////////////////////////////

        String user999 = "user999";
        SecurityContext sc999 = new SecurityContextForTesting(user999);
        try {
            authorizationAgent.authorizeCreateSchemaBranch(sc999,
                    schemaRegistry,
                    schemaName3,
                    new Long(siv31.getSchemaVersionId()), "Branch4");
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [create] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch4' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

        String user4 = "user4";
        SecurityContext sc4 = new SecurityContextForTesting(user4);
        try {
            // Creation is allowed by p6
            authorizationAgent.authorizeCreateSchemaBranch(sc4,
                    schemaRegistry,
                    schemaName3,
                    new Long(siv31.getSchemaVersionId()), "Branch4");
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user4' does not have [read] permission on " +
                    "SchemaVersion{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch3'," +
                    " schemaVersionName='*' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

    }

    @Test
    public void authorizeDeleteSchemaBranch() throws AuthorizationException {
        String user5 = "user5";
        SecurityContext sc5 = new SecurityContextForTesting(user5);

        // Allowed by p8
        authorizationAgent.authorizeDeleteSchemaBranch(sc5,
                schemaRegistry,
                new Long(branch3.getId()));

        String user999 = "user999";
        SecurityContext sc999 = new SecurityContextForTesting(user999);

        try {
            authorizationAgent.authorizeDeleteSchemaBranch(sc999,
                    schemaRegistry,
                    new Long(branch3.getId()));
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [delete] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch3' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }
    }

    @Test
    public void authorizeGetAllBranches() throws SchemaNotFoundException {
        String user6 = "user6";
        SecurityContext sc6 = new SecurityContextForTesting(user6);
        ArrayList<SchemaBranch> branches = new ArrayList<>();
        Collection<SchemaBranch> res =
                authorizationAgent.authorizeGetAllBranches(sc6, schemaRegistry,
                        "Schema3", branches);
        assertTrue(res.isEmpty());

        SchemaBranch branch3 = new SchemaBranch("Branch3", "Schema3"),
                branch4 = new SchemaBranch("Branch4", "Schema3"),
                branch5 = new SchemaBranch("Branch5", "Schema3");

        branches.add(branch3);
        branches.add(branch5);

        Collection<SchemaBranch> expected = new ArrayList<>(branches);
        // Filter by p8, p9
        res = authorizationAgent.authorizeGetAllBranches(sc6, schemaRegistry,
                "Schema3", branches);
        assertThat(res, is(expected));

        expected = new ArrayList<>(branches);
        branches.add(branch4);
        // Filter by p8, p9
        res = authorizationAgent.authorizeGetAllBranches(sc6, schemaRegistry,
                "Schema3", branches);
        assertThat(res, is(expected));

    }

    @Test
    public void authorizeSchemaVersion()
            throws AuthorizationException, SchemaNotFoundException {
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);

        // Authorized by p5
        authorizationAgent.authorizeSchemaVersion(sc3,
                schemaRegistry,
                "Schema3",
                "MASTER",
                Authorizer.AccessType.READ);

        SchemaVersionKey svk3 = new SchemaVersionKey("Schema3", siv3.getVersion());
        authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry, svk3,  Authorizer.AccessType.DELETE);

        SchemaVersionInfo svi3 = schemaRegistry.getSchemaVersionInfo(siv3);
        authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry, svi3, Authorizer.AccessType.READ);

        authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry, siv3, Authorizer.AccessType.DELETE);

        authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry,
                siv31.getSchemaVersionId(), Authorizer.AccessType.READ);

        try {
            authorizationAgent.authorizeSchemaVersion(sc3,
                    schemaRegistry,
                    "Schema3",
                    "MASTER",
                    Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaVersion{ schemaGroupName='Group3', " +
                    "schemaMetadataName='Schema3', schemaBranchName='MASTER', schemaVersionName='*' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

        try {
            authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry, svk3, Authorizer.AccessType.UPDATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [update] permission on " +
                    "SchemaVersion{ schemaGroupName='Group3', " +
                    "schemaMetadataName='Schema3', schemaBranchName='MASTER', schemaVersionName='*' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

        try {
            authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry, svi3, Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaVersion{ schemaGroupName='Group3', " +
                    "schemaMetadataName='Schema3', schemaBranchName='MASTER', schemaVersionName='*' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

        try {
            authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry, siv31, Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaVersion{ schemaGroupName='Group3', " +
                    "schemaMetadataName='Schema3', schemaBranchName='Branch3', schemaVersionName='*' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }

        try {
            authorizationAgent.authorizeSchemaVersion(sc3, schemaRegistry,
                    siv31.getSchemaVersionId(), Authorizer.AccessType.UPDATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [update] permission on " +
                    "SchemaVersion{ schemaGroupName='Group3', " +
                    "schemaMetadataName='Schema3', schemaBranchName='Branch3', schemaVersionName='*' }";
            assertThat(e.getMessage(), is(expectedMsg));
        }
    }

    @Test
    public void authorizeGetSerializers() {
    }

    @Test
    public void authorizeSerDes() {
    }

    @Test
    public void authorizeMapSchemaWithSerDes() {
    }

    @Test
    public void authorizeMergeSchemaVersion() {
    }

    @Test
    public void authorizeGetAllVersions() {
    }
}
