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

import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.registries.schemaregistry.ISchemaRegistry;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.common.hadoop.KerberosKeytabCheck;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.authorizer.agent.util.SchemaTextStore;
import com.hortonworks.registries.schemaregistry.authorizer.agent.util.SecurityContextForTesting;
import com.hortonworks.registries.schemaregistry.authorizer.core.util.AuthenticationUtils;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.agent.util.TestAuthorizer;
import com.hortonworks.registries.schemaregistry.authorizer.exception.AuthorizationException;
import com.hortonworks.registries.schemaregistry.authorizer.exception.RangerException;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.locks.SchemaLockManager;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.ws.rs.NotSupportedException;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class DefaultAuthorizationAgentTest {


    private static AuthorizationAgent authorizationAgent;
    private static AuthenticationUtils authenticationUtils;
    private static ISchemaRegistry schemaRegistry;
    private static SchemaIdVersion siv3;
    private static SchemaIdVersion siv31;
    private static SchemaIdVersion siv4;
    private static SchemaBranch branch3;
    private static Map<String, Long> schemaNameToIdMap;

    @BeforeAll
    public static void setUp() throws SchemaNotFoundException,
            InvalidSchemaException,
            IncompatibleSchemaException,
            SchemaBranchAlreadyExistsException {
        ///////////////////////////////// SchemaRegistry instantiation ////////////////

        final ImmutableMap.Builder<String, Long> schemaIdMap = ImmutableMap.builder();

        authenticationUtils = new AuthenticationUtils(new KerberosKeytabCheck());

        Collection<Map<String, Object>> schemaProvidersConfig =
                Collections.singleton(Collections.singletonMap("providerClass",
                        AvroSchemaProvider.class.getName()));
        StorageManager storageManager = new InMemoryStorageManager();

        ModuleDetailsConfiguration configuration = new ModuleDetailsConfiguration();
        schemaRegistry = new DefaultSchemaRegistry(configuration, storageManager,
                null,
                schemaProvidersConfig,
                new SchemaLockManager(new NOOPTransactionManager()));

        ///////////////////////////////// SchemaRegistry initialization ////////////////

        SchemaMetadata sm3 = new SchemaMetadata
                .Builder("Schema3")
                .schemaGroup("Group3")
                .type("avro")
                .build();
        schemaIdMap.put("Schema3", schemaRegistry.addSchemaMetadata(sm3));

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
        schemaIdMap.put("Schema4", schemaRegistry.addSchemaMetadata(sm4));

        SchemaVersion sv4 = new SchemaVersion(SchemaTextStore.SCHEMA_TEXT_4, "dummy");
        siv4 = schemaRegistry.addSchemaVersion("Schema4", sv4);



        ////////////////////////////// Authorizer instantiation ///////////////////////

        Map<String, Object> props = new HashMap<>();

        props.put(AuthorizationAgent.AUTHORIZATION_AGENT_CONFIG, DefaultAuthorizationAgent.class.getCanonicalName());
        props.put(Authorizer.AUTHORIZER_CONFIG, TestAuthorizer.class.getCanonicalName());

        authorizationAgent = AuthorizationAgentFactory.getAuthorizationAgent(props);
        TestAuthorizer testAuthorizer = (TestAuthorizer) (((DefaultAuthorizationAgent) authorizationAgent)
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
                new String[] {"user3", "user100", "user101", "user102", "user333", "user33"},
                Authorizer.AccessType.READ, Authorizer.AccessType.DELETE, Authorizer.AccessType.UPDATE);
        testAuthorizer.addPolicy(p3);


        //p4
        Authorizer.Resource schemaBranch3 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch3");
        TestAuthorizer.Policy p4 = new TestAuthorizer.Policy(schemaBranch3,
                new String[] {"user3", "user101", "user102"},
                Authorizer.AccessType.CREATE, Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p4);


        //p5
        Authorizer.Resource schemaVersion3 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "MASTER");
        TestAuthorizer.Policy p5 = new TestAuthorizer.Policy(schemaVersion3,
                new String[] {"user3", "user101", "user102"},
                Authorizer.AccessType.READ, Authorizer.AccessType.DELETE, Authorizer.AccessType.CREATE);
        testAuthorizer.addPolicy(p5);

        //p6
        Authorizer.Resource schemaBranch4 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch4");
        TestAuthorizer.Policy p6 = new TestAuthorizer.Policy(schemaBranch4,
                new String[] {"user3", "user4"},
                Authorizer.AccessType.CREATE);
        testAuthorizer.addPolicy(p6);


        //p7
        Authorizer.Resource schemaVersion31 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "Branch3");
        TestAuthorizer.Policy p7 = new TestAuthorizer.Policy(schemaVersion31,
                new String[]{"user3", "user33", "user102"},
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p7);

        //p8
        TestAuthorizer.Policy p8 = new TestAuthorizer.Policy(schemaBranch3,
                new String[]{"user5", "user6", "user88"},
                Authorizer.AccessType.DELETE, Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p8);

        //p9
        Authorizer.Resource schemaBranch5 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch5");
        TestAuthorizer.Policy p9 = new TestAuthorizer.Policy(schemaBranch5,
                "user6",
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p9);

        //p10
        Authorizer.Resource serdeResource = new Authorizer.SerdeResource();
        TestAuthorizer.Policy p10 = new TestAuthorizer.Policy(serdeResource,
                new String[]{"user3", "user4"},
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p10);

        //p11
        Authorizer.Resource schemaVersion4 = new Authorizer
                .SchemaVersionResource("Group4", "Schema4", "MASTER");
        TestAuthorizer.Policy p11 = new TestAuthorizer.Policy(schemaVersion4,
                "user4",
                Authorizer.AccessType.READ);
        testAuthorizer.addPolicy(p11);

        //p12
        Authorizer.Resource schemaVersion5 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "*");
        TestAuthorizer.Policy p12 = new TestAuthorizer.Policy(schemaVersion5,
                new String[] {"user5", "user333"},
                Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p12);

        //p13
        Authorizer.Resource schemaBranch333 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "MASTER");
        TestAuthorizer.Policy p13 = new TestAuthorizer.Policy(schemaBranch333,
                "user333",
                Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p13);

        //p14
        Authorizer.Resource schemaBranch33 = new Authorizer
                .SchemaBranchResource("Group3", "Schema3", "Branch3");
        TestAuthorizer.Policy p14 = new TestAuthorizer.Policy(schemaBranch33,
                new String[] {"user333"},
                Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p14);

        //p15
        Authorizer.Resource schemaVersion333 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "Branch3");
        TestAuthorizer.Policy p15 = new TestAuthorizer.Policy(schemaVersion333,
                new String[]{"user333", "user5"},
                Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p15);

        //p16
        Authorizer.Resource schemaVersion3333 = new Authorizer
                .SchemaVersionResource("Group3", "Schema3", "MASTER");
        TestAuthorizer.Policy p16 = new TestAuthorizer.Policy(schemaVersion3333,
                "user333",
                Authorizer.AccessType.DELETE);
        testAuthorizer.addPolicy(p16);

        schemaNameToIdMap = schemaIdMap.build();
    }

    @Test
    public void authorizeDeleteSchemaMetadata() throws SchemaNotFoundException, AuthorizationException {
        String user = "user333";
        SecurityContext sc = new SecurityContextForTesting(user);

        // Authorized by p13, p14, p15, p16
        authorizationAgent.authorizeDeleteSchemaMetadata(authenticationUtils.getUserAndGroups(sc),
                schemaRegistry, "Schema3");

        try {
            String user999 = "user999";
            SecurityContext sc999 = new SecurityContextForTesting(user999);
            authorizationAgent.authorizeDeleteSchemaMetadata(authenticationUtils.getUserAndGroups(sc999),
                    schemaRegistry, "Schema3");
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [delete] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            String user999 = "user33";
            SecurityContext sc999 = new SecurityContextForTesting(user999);
            // Partially authorized by p3
            authorizationAgent.authorizeDeleteSchemaMetadata(authenticationUtils.getUserAndGroups(sc999),
                    schemaRegistry, "Schema3");
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg1 = "User 'user33' does not have [delete] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='MASTER' }";
            String expectedMsg2 = "User 'user33' does not have [delete] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch3' }";
            // The result depends on the order in which the branches were added
            assertTrue(expectedMsg1.equals(e.getMessage()) || expectedMsg2.equals(e.getMessage()));
        }

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeDeleteSchemaMetadata(authenticationUtils.getUserAndGroups(sc),
                    schemaRegistry, "SchemaNotFound");
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No SchemaMetadata exists with key: SchemaNotFound";
            assertEquals(expectedMsg, e.getMessage());
        }

    }

    @Test
    public void authorizeFindSchemas() throws SchemaNotFoundException {

        // 1
        // Empty because initial list is empty
        String user = "user4";
        SecurityContext sc = new SecurityContextForTesting(user);
        ArrayList<SchemaMetadataInfo> schemas = new ArrayList();
        Collection<SchemaMetadataInfo> resEmpty = authorizationAgent.authorizeFindSchemas(authenticationUtils.getUserAndGroups(sc), schemas);
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
        resEmpty = authorizationAgent.authorizeFindSchemas(authenticationUtils.getUserAndGroups(sc1), schemas);
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
        Collection<SchemaMetadataInfo> resNonEmpty = authorizationAgent.authorizeFindSchemas(authenticationUtils.getUserAndGroups(sc2), schemas);
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
        resNonEmpty = authorizationAgent.authorizeFindSchemas(authenticationUtils.getUserAndGroups(sc), schemas);
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
        resNonEmpty = authorizationAgent.authorizeFindSchemas(authenticationUtils.getUserAndGroups(sc), schemas);
        assertTrue(resNonEmpty.size() == 2);
        assertTrue(resNonEmpty.contains(smi2));
        assertTrue(resNonEmpty.contains(smi3));
        assertFalse(resNonEmpty.contains(smi4));

        // NOT_FOUND TEST CASES do not exist

    }


    @Test
    public void configure() {
        try {
            authorizationAgent.configure(new HashMap<>());
            fail("Expected an DefaultAuthorizationAgent.AlreadyConfiguredException to be thrown");
        } catch (DefaultAuthorizationAgent.AlreadyConfiguredException e) {
            String expectedMsg = "DefaultAuthorizationAgent is already configured";
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND TEST CASES do not exist
    }

    @Test
    public void authorizeGetAggregatedSchemaList() throws SchemaNotFoundException {
        List<AggregatedSchemaMetadataInfo> asmiList = new ArrayList<>();

        SecurityContext sc3 = new SecurityContextForTesting("user3");
        Collection<AggregatedSchemaMetadataInfo> res = authorizationAgent
                .authorizeGetAggregatedSchemaList(authenticationUtils.getUserAndGroups(sc3), asmiList);
        assertTrue(res.isEmpty());

        SchemaMetadataInfo smi3 = schemaRegistry.getSchemaMetadataInfo("Schema3");
        ArrayList<AggregatedSchemaBranch> aggregatedBranches = new ArrayList<>();
        ArrayList<SerDesInfo> serDesInfo = new ArrayList<>();
        serDesInfo.add(new SerDesInfo(1L, smi3.getTimestamp(), new SerDesPair()));
        serDesInfo.add(new SerDesInfo(2L, smi3.getTimestamp(), new SerDesPair()));
        List<SchemaVersionInfo> versions = new ArrayList<>();
        SchemaVersionInfo svi31 = schemaRegistry.getSchemaVersionInfo(siv31);
        versions.add(svi31);
        SchemaBranch sb = new SchemaBranch("Branch3", "Schema3");
        AggregatedSchemaBranch branch = new AggregatedSchemaBranch(sb, siv3.getSchemaVersionId(), versions);
        aggregatedBranches.add(branch);

        AggregatedSchemaMetadataInfo asmi =
                new AggregatedSchemaMetadataInfo(smi3.getSchemaMetadata(),
                        smi3.getId(),
                        smi3.getTimestamp(),
                        aggregatedBranches,
                        serDesInfo);

        asmiList.add(asmi);
        res = authorizationAgent
                .authorizeGetAggregatedSchemaList(authenticationUtils.getUserAndGroups(sc3), asmiList);
        assertTrue(res.size() == 1);

        SecurityContext sc999 = new SecurityContextForTesting("user999");
        res = authorizationAgent
                .authorizeGetAggregatedSchemaList(authenticationUtils.getUserAndGroups(sc999), asmiList);
        assertTrue(res.isEmpty());

        // NOT_FOUND TEST CASES do not exist
    }

    @Test
    public void authorizeGetAggregatedSchemaInfo() throws SchemaNotFoundException, AuthorizationException {
        SchemaMetadataInfo smi3 = schemaRegistry.getSchemaMetadataInfo("Schema3");
        ArrayList<AggregatedSchemaBranch> aggregatedBranches = new ArrayList<>();
        ArrayList<SerDesInfo> serDesInfo = new ArrayList<>();
        serDesInfo.add(new SerDesInfo(1L, smi3.getTimestamp(), new SerDesPair()));
        serDesInfo.add(new SerDesInfo(2L, smi3.getTimestamp(), new SerDesPair()));
        List<SchemaVersionInfo> versions = new ArrayList<>();
        SchemaVersionInfo svi31 = schemaRegistry.getSchemaVersionInfo(siv31);
        versions.add(svi31);
        SchemaBranch sb = new SchemaBranch("Branch3", "Schema3");
        AggregatedSchemaBranch branch = new AggregatedSchemaBranch(sb, siv3.getSchemaVersionId(), versions);
        aggregatedBranches.add(branch);

        AggregatedSchemaMetadataInfo asmi =
                new AggregatedSchemaMetadataInfo(smi3.getSchemaMetadata(),
                        smi3.getId(),
                        smi3.getTimestamp(),
                        aggregatedBranches,
                        serDesInfo);

        SecurityContext sc3 = new SecurityContextForTesting("user3");
        authorizationAgent.authorizeGetAggregatedSchemaInfo(authenticationUtils.getUserAndGroups(sc3), asmi);

        try {
            SecurityContext sc999 = new SecurityContextForTesting("user999");
            authorizationAgent.authorizeGetAggregatedSchemaInfo(authenticationUtils.getUserAndGroups(sc999), asmi);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [read] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            SecurityContext sc100 = new SecurityContextForTesting("user100");
            authorizationAgent.authorizeGetAggregatedSchemaInfo(authenticationUtils.getUserAndGroups(sc100), asmi);
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user100' does not have [read] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            SecurityContext sc101 = new SecurityContextForTesting("user101");
            authorizationAgent.authorizeGetAggregatedSchemaInfo(authenticationUtils.getUserAndGroups(sc101), asmi);
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user101",
                    "read", "Group3", "Schema3", "Branch3");
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            SecurityContext sc101 = new SecurityContextForTesting("user102");
            authorizationAgent.authorizeGetAggregatedSchemaInfo(authenticationUtils.getUserAndGroups(sc101), asmi);
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user102' does not have [read] permission on SerDe{ serDeName='*' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND TEST CASES do not exist

    }

    @Test
    public void authorizeFindSchemasByFields() throws SchemaNotFoundException, RangerException {
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);

        // Empty array should stay empty
        List<SchemaVersionKey> versions = new ArrayList<>();
        Collection<SchemaVersionKey> res = authorizationAgent
                .authorizeFindSchemasByFields(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, versions);
        assertTrue(res.isEmpty());

        SchemaVersionKey svi3 = new SchemaVersionKey("Schema3", siv3.getVersion());
        versions.add(svi3);
        SchemaVersionKey svi31 = new SchemaVersionKey("Schema3", siv31.getVersion());
        versions.add(svi31);

        List<SchemaVersionKey> expected = new ArrayList<>(versions);
        // Authorized by p7
        res = authorizationAgent.authorizeFindSchemasByFields(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, versions);
        assertEquals(expected, res);

        SchemaVersionKey svi4 = new SchemaVersionKey("Schema4", siv4.getVersion());
        versions.add(svi4);
        expected = new ArrayList<>();
        expected.add(svi4);
        String user4 = "user4";
        SecurityContext sc4 = new SecurityContextForTesting(user4);
        // Authorized by p11
        res = authorizationAgent.authorizeFindSchemasByFields(authenticationUtils.getUserAndGroups(sc4), schemaRegistry, versions);
        assertEquals(expected, res);

        // NOT_FOUND TEST CASES do not exist
    }


// problem is: javax/ws/rs/WebApplicationException
// its coming from ranger-shaded and also from our own lib
// we need to exclude it from ranger-shaded OR rewrite our code to not throw NotSupportedException
    @Test
    public void authorizeSchemaMetadata()
            throws AuthorizationException, SchemaNotFoundException {

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
        authorizationAgent.authorizeSchemaMetadata(
                authenticationUtils.getUserAndGroups(sc3),
                sm3,
                Authorizer.AccessType.READ);
        authorizationAgent.authorizeSchemaMetadata(
                authenticationUtils.getUserAndGroups(sc3),
                smi3, 
                Authorizer.AccessType.UPDATE);
        authorizationAgent.authorizeSchemaMetadata(
                authenticationUtils.getUserAndGroups(sc3),
                schemaRegistry, 
                sm3.getName(), 
                Authorizer.AccessType.READ);

        ///////////////////////////// Negative cases /////////////////////////////////

        try {
            authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(sc3),
                    smi3, Authorizer.AccessType.DELETE);
        } catch (NotSupportedException e) {
            String expectedMsg = "AccessType.DELETE is not supported for authorizeSchemaMetadata method";
            assertEquals(expectedMsg, e.getMessage());
        } catch (Throwable e2) {
            fail("Unexpected exception: " + ExceptionUtils.getStackTrace(e2));
        }

        try {
            String user4 = "user4";
            SecurityContext sc4 = new SecurityContextForTesting(user4);
            authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(sc4), sm3, Authorizer.AccessType.UPDATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user4' does not have [update] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(sc3), smi3, Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(
                    authenticationUtils.getUserAndGroups(sc3),
                    schemaRegistry, 
                    sm3.getName(),
                    Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [create] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        // No policy that matches the resource
        SchemaMetadata sm4 = new SchemaMetadata
                .Builder("Schema4")
                .schemaGroup("Group4")
                .type("avro")
                .build();
        SchemaMetadataInfo smi4 = new SchemaMetadataInfo(sm4);
        try {
            authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(sc3), sm4, Authorizer.AccessType.READ);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [read] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group4', schemaMetadataName='Schema4' }";
            assertEquals(expectedMsg, e.getMessage());
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(sc3), smi4, Authorizer.AccessType.READ);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user3' does not have [read] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group4', schemaMetadataName='Schema4' }";
            assertEquals(expectedMsg, e.getMessage());
        }
        try {
            authorizationAgent.authorizeSchemaMetadata(
                    authenticationUtils.getUserAndGroups(sc3),
                    schemaRegistry,
                    sm4.getName(),
                    Authorizer.AccessType.DELETE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (NotSupportedException e) {
            String expectedMsg = "AccessType.DELETE is not supported for authorizeSchemaMetadata method";
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeSchemaMetadata(authenticationUtils.getUserAndGroups(sc3),
                    schemaRegistry, "SchemaNotFound", Authorizer.AccessType.READ);
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No SchemaMetadata exists with key: SchemaNotFound";
            assertEquals(expectedMsg, e.getMessage());
        }

    }

    @Test
    public void authorizeCreateSchemaBranch() throws AuthorizationException, SchemaNotFoundException {

        ///////////////////////////// Positive cases /////////////////////////////////

        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);
        String schemaName3 = "Schema3";

        // Access allowed by p4 (for creating Branch3) and p5 (for reading siv3)
        authorizationAgent.authorizeCreateSchemaBranch(authenticationUtils.getUserAndGroups(sc3),
                schemaRegistry,
                schemaNameToIdMap.get(schemaName3),
                 new Long(siv3.getSchemaVersionId()), "Branch3");

        // Access allowed by p6 (for creating Branch3) and p7 (for reading siv31)
        authorizationAgent.authorizeCreateSchemaBranch(authenticationUtils.getUserAndGroups(sc3),
                schemaRegistry,
                schemaNameToIdMap.get(schemaName3),
                siv31.getSchemaVersionId(), "Branch4");

        ///////////////////////////// Negative cases /////////////////////////////////

        String user999 = "user999";
        SecurityContext sc999 = new SecurityContextForTesting(user999);
        try {
            authorizationAgent.authorizeCreateSchemaBranch(authenticationUtils.getUserAndGroups(sc999),
                    schemaRegistry,
                    schemaNameToIdMap.get(schemaName3),
                    new Long(siv31.getSchemaVersionId()), "Branch4");
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [create] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch4' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        String user4 = "user4";
        SecurityContext sc4 = new SecurityContextForTesting(user4);
        try {
            // Creation is allowed by p6
            authorizationAgent.authorizeCreateSchemaBranch(authenticationUtils.getUserAndGroups(sc4),
                    schemaRegistry,
                    schemaNameToIdMap.get(schemaName3),
                    new Long(siv31.getSchemaVersionId()), "Branch4");
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user4",
                    "read", "Group3", "Schema3", "Branch3");
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeCreateSchemaBranch(authenticationUtils.getUserAndGroups(sc3),
                    schemaRegistry,
                    schemaNameToIdMap.get(schemaName3),
                    -9999L,
                    "Branch3");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "Schema version with id : -9999 not found";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            authorizationAgent.authorizeCreateSchemaBranch(authenticationUtils.getUserAndGroups(sc4),
                    schemaRegistry,
                    99999999999999L,
                    siv31.getSchemaVersionId(),
                    "BranchX");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "Could not find schema with ID 99999999999999";
            assertEquals(expectedMsg, e.getMessage());
        }
    }

    @Test
    public void authorizeDeleteSchemaBranch() throws AuthorizationException {
        ///////////////// Positive cases ////////////////////////////
        String user5 = "user5";
        SecurityContext sc5 = new SecurityContextForTesting(user5);

        // Allowed by p8, p12
        authorizationAgent.authorizeDeleteSchemaBranch(authenticationUtils.getUserAndGroups(sc5),
                schemaRegistry,
                new Long(branch3.getId()));

        String user999 = "user999";
        SecurityContext sc999 = new SecurityContextForTesting(user999);

        /////////////////// Negative cases //////////////////////////
        try {
            authorizationAgent.authorizeDeleteSchemaBranch(authenticationUtils.getUserAndGroups(sc999),
                    schemaRegistry,
                    new Long(branch3.getId()));
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [delete] permission on " +
                    "SchemaBranch{ schemaGroupName='Group3', schemaMetadataName='Schema3', schemaBranchName='Branch3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        String user88 = "user88";
        SecurityContext sc88 = new SecurityContextForTesting(user88);

        /////////////////// Negative cases //////////////////////////
        try {
            authorizationAgent.authorizeDeleteSchemaBranch(authenticationUtils.getUserAndGroups(sc88),
                    schemaRegistry,
                    new Long(branch3.getId()));
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user88",
                    "delete", "Group3", "Schema3", "Branch3");
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeDeleteSchemaBranch(authenticationUtils.getUserAndGroups(sc88),
                    schemaRegistry,
                    new Long(-9999));
        } catch (SchemaBranchNotFoundException e) {
            String expectedMsg = "Schema branch with id : '-9999' not found";
            assertEquals(expectedMsg, e.getMessage());
        }
    }

    @Test
    public void authorizeGetAllBranches() throws SchemaNotFoundException {
        String user6 = "user6";
        SecurityContext sc6 = new SecurityContextForTesting(user6);
        ArrayList<SchemaBranch> branches = new ArrayList<>();
        Collection<SchemaBranch> res =
                authorizationAgent.authorizeGetAllBranches(authenticationUtils.getUserAndGroups(sc6), schemaRegistry,
                        "Schema3", branches);
        assertTrue(res.isEmpty());

        SchemaBranch branch3 = new SchemaBranch("Branch3", "Schema3");
        SchemaBranch branch4 = new SchemaBranch("Branch4", "Schema3");
        SchemaBranch branch5 = new SchemaBranch("Branch5", "Schema3");

        branches.add(branch3);
        branches.add(branch5);

        Collection<SchemaBranch> expected = new ArrayList<>(branches);
        // Filter by p8, p9
        res = authorizationAgent.authorizeGetAllBranches(authenticationUtils.getUserAndGroups(sc6), schemaRegistry,
                "Schema3", branches);
        assertEquals(expected, res);

        expected = new ArrayList<>(branches);
        branches.add(branch4);
        // Filter by p8, p9
        res = authorizationAgent.authorizeGetAllBranches(authenticationUtils.getUserAndGroups(sc6), schemaRegistry,
                "Schema3", branches);
        assertEquals(expected, res);

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeGetAllBranches(authenticationUtils.getUserAndGroups(sc6), schemaRegistry,
                    "SchemaNotFound", branches);
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No SchemaMetadata exists with key: SchemaNotFound";
            assertEquals(expectedMsg, e.getMessage());
        }

    }

    @Test
    public void authorizeSchemaVersion()
            throws AuthorizationException, SchemaNotFoundException {
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);

        ///////////////// Positive cases ////////////////////////////

        // Authorized by p5
        authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3),
                schemaRegistry,
                "Schema3",
                "MASTER",
                Authorizer.AccessType.READ);

        SchemaVersionKey svk3 = new SchemaVersionKey("Schema3", siv3.getVersion());
        authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, svk3,  Authorizer.AccessType.DELETE);

        SchemaVersionInfo svi3 = schemaRegistry.getSchemaVersionInfo(siv3);
        authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, svi3, Authorizer.AccessType.READ);

        authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, siv3, Authorizer.AccessType.DELETE);

        authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry,
                siv31.getSchemaVersionId(), Authorizer.AccessType.READ);

        /////////////////// Negative cases //////////////////////////

        String user4 = "user4";
        SecurityContext sc4 = new SecurityContextForTesting(user4);
        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc4),
                    schemaRegistry,
                    "Schema3",
                    "MASTER",
                    Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user4",
                    "create", "Group3", "Schema3", "MASTER");
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, svk3, Authorizer.AccessType.UPDATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user3",
                    "update", "Group3", "Schema3", "MASTER");
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc4), schemaRegistry, svi3, Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user4",
                    "create", "Group3", "Schema3", "MASTER");
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, siv31, Authorizer.AccessType.CREATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user3",
                    "create", "Group3", "Schema3", "Branch3");
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry,
                    siv31.getSchemaVersionId(), Authorizer.AccessType.UPDATE);
            fail("Expected an AuthorizationException to be thrown");
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user3",
                    "update", "Group3", "Schema3", "Branch3");
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND test cases

        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3),
                    schemaRegistry,
                    "SchemaNotFound",
                    "MASTER",
                    Authorizer.AccessType.READ);
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No SchemaMetadata exists with key: SchemaNotFound";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            svk3 = new SchemaVersionKey("SchemaNotFound", siv3.getVersion());
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, svk3, Authorizer.AccessType.DELETE);
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No SchemaMetadata exists with key: SchemaNotFound";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            svk3 = new SchemaVersionKey("Schema3", -99999);
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, svk3, Authorizer.AccessType.DELETE);
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No Schema version exists with name Schema3 and version -99999";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            SchemaIdVersion notFoundSchemaIdVersion = new SchemaIdVersion(-9999L);
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry,
                    notFoundSchemaIdVersion, Authorizer.AccessType.DELETE);
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No Schema version exists with id -9999";
            assertEquals(expectedMsg, e.getMessage());
        }

        try {
            authorizationAgent.authorizeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry,
                new Long(-9999), Authorizer.AccessType.READ);
            fail("Expected an SchemaNotFoundException to be thrown");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No Schema version exists with id -9999";
            assertEquals(expectedMsg, e.getMessage());
        }

    }

    private String errorMsgForUnathorizedSchemaVersion(String user,
                                                       String perm,
                                                       String groupName,
                                                       String schemaName,
                                                       String branchName) {
        return String.format("User '%s' does not have [%s] permission on SchemaVersion{ schemaGroupName='%s', "
                        + "schemaMetadataName='%s', schemaBranchName='%s', schemaVersionName='*' }",
                user, perm, groupName, schemaName, branchName);
    }

    @Test
    public void authorizeGetSerializers() throws AuthorizationException {
        ///////////////// Positive cases ////////////////////////////
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);
        SchemaMetadata sm3 = new SchemaMetadata
                .Builder("Schema3")
                .schemaGroup("Group3")
                .type("avro")
                .build();
        SchemaMetadataInfo smi3 = new SchemaMetadataInfo(sm3);

        // Access allowed by p3 and p10
        authorizationAgent.authorizeGetSerializers(authenticationUtils.getUserAndGroups(sc3), smi3);

        /////////////////// Negative cases //////////////////////////

        String user5 = "user5";
        SecurityContext sc5 = new SecurityContextForTesting(user5);

        try {
            authorizationAgent.authorizeGetSerializers(authenticationUtils.getUserAndGroups(sc5), smi3);
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user5' does not have [read] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        String user1 = "user1";
        SecurityContext sc1 = new SecurityContextForTesting(user1);
        SchemaMetadata sm1 = new SchemaMetadata
                .Builder("Schema1")
                .schemaGroup("Group1")
                .type("avro")
                .build();
        SchemaMetadataInfo smi1 = new SchemaMetadataInfo(sm1);
        try {
            // Access allowed to metadata by p1
            authorizationAgent.authorizeGetSerializers(authenticationUtils.getUserAndGroups(sc1), smi1);
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user1' does not have [read] permission on " +
                    "SerDe{ serDeName='*' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUD test cases do not exist
    }

    @Test
    public void authorizeSerDes() throws AuthorizationException {
        ///////////////// Positive cases ////////////////////////////
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);
        authorizationAgent.authorizeSerDes(authenticationUtils.getUserAndGroups(sc3), Authorizer.AccessType.READ);

        /////////////////// Negative cases //////////////////////////
        String user999 = "user999";
        SecurityContext sc999 = new SecurityContextForTesting(user999);
        try {
            authorizationAgent.authorizeSerDes(authenticationUtils.getUserAndGroups(sc999), Authorizer.AccessType.READ);
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user999' does not have [read] permission on SerDe{ serDeName='*' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUD test cases do not exist
    }

    @Test
    public void authorizeMapSchemaWithSerDes()
            throws AuthorizationException, SchemaNotFoundException {
        ///////////////// Positive cases ////////////////////////////
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);

        // Access allowed by p3 and p10
        authorizationAgent.authorizeMapSchemaWithSerDes(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, "Schema3");

        /////////////////// Negative cases //////////////////////////
        String user5 = "user5";
        SecurityContext sc5 = new SecurityContextForTesting(user5);

        try {
            authorizationAgent.authorizeMapSchemaWithSerDes(authenticationUtils.getUserAndGroups(sc5), schemaRegistry, "Schema3");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user5' does not have [read] permission on SerDe{ serDeName='*' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        String user4 = "user4";
        SecurityContext sc4 = new SecurityContextForTesting(user4);
        try {
            // Accessed allowed to serdes by p10
            authorizationAgent.authorizeMapSchemaWithSerDes(authenticationUtils.getUserAndGroups(sc4), schemaRegistry, "Schema3");
        } catch (AuthorizationException e) {
            String expectedMsg = "User 'user4' does not have [update] permission on " +
                    "SchemaMetadata{ schemaGroupName='Group3', schemaMetadataName='Schema3' }";
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeMapSchemaWithSerDes(authenticationUtils.getUserAndGroups(sc4), schemaRegistry, "SchemaNotFound");
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No SchemaMetadata exists with key: SchemaNotFound";
            assertEquals(expectedMsg, e.getMessage());
        }
    }

    @Test
    public void authorizeMergeSchemaVersion()
            throws SchemaNotFoundException, AuthorizationException {
        ///////////////// Positive cases ////////////////////////////
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);

        // Accessed allowed by p7
        authorizationAgent.authorizeMergeSchemaVersion(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, siv31.getSchemaVersionId());

        /////////////////// Negative cases //////////////////////////
        String user999 = "user999";
        SecurityContext sc999 = new SecurityContextForTesting(user999);

        try {
            authorizationAgent.authorizeMergeSchemaVersion(authenticationUtils.getUserAndGroups(sc999), schemaRegistry, siv31.getSchemaVersionId());
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user999",
                    "read", "Group3", "Schema3", "Branch3");
            assertEquals(expectedMsg, e.getMessage());
        }

        String user33 = "user33";
        SecurityContext sc33 = new SecurityContextForTesting(user33);
        try {
            // Read is allowed by p7
            authorizationAgent.authorizeMergeSchemaVersion(authenticationUtils.getUserAndGroups(sc33), schemaRegistry, siv31.getSchemaVersionId());
        } catch (AuthorizationException e) {
            String expectedMsg = errorMsgForUnathorizedSchemaVersion("user33",
                    "create", "Group3", "Schema3", "MASTER");
            assertEquals(expectedMsg, e.getMessage());
        }

        // NOT_FOUND test cases
        try {
            authorizationAgent.authorizeMergeSchemaVersion(authenticationUtils.getUserAndGroups(sc33), schemaRegistry, -9999L);
        } catch (SchemaNotFoundException e) {
            String expectedMsg = "No Schema version exists with id -9999";
            assertEquals(expectedMsg, e.getMessage());
        }
    }

    @Test
    public void authorizeGetAllVersions() throws SchemaNotFoundException {
        String user3 = "user3";
        SecurityContext sc3 = new SecurityContextForTesting(user3);

        // Empty array should stay empty
        List<SchemaVersionInfo> versions = new ArrayList<>();
        Collection<SchemaVersionInfo> res = authorizationAgent.authorizeGetAllVersions(
                authenticationUtils.getUserAndGroups(sc3), schemaRegistry, versions);
        assertTrue(res.isEmpty());

        SchemaVersionInfo svi3 = schemaRegistry.getSchemaVersionInfo(siv3);
        versions.add(svi3);
        SchemaVersionInfo svi31 = schemaRegistry.getSchemaVersionInfo(siv31);
        versions.add(svi31);

        List<SchemaVersionInfo> expected = new ArrayList<>(versions);
        // Authorized by p7
        res = authorizationAgent.authorizeGetAllVersions(authenticationUtils.getUserAndGroups(sc3), schemaRegistry, versions);
        assertEquals(expected, res);

        SchemaVersionInfo svi4 = schemaRegistry.getSchemaVersionInfo(siv4);
        versions.add(svi4);
        expected = new ArrayList<>();
        expected.add(svi4);
        String user4 = "user4";
        SecurityContext sc4 = new SecurityContextForTesting(user4);
        // Authorized by p11
        res = authorizationAgent.authorizeGetAllVersions(authenticationUtils.getUserAndGroups(sc4), schemaRegistry, versions);
        assertEquals(expected, res);

        // NOT_FOUND test cases do not exist
    }
}
