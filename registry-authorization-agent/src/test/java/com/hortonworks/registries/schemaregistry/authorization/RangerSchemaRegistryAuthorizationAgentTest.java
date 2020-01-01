package com.hortonworks.registries.schemaregistry.authorization;


import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.authorization.util.SecurityContextForTesting;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.mockito.MockitoAnnotations;

import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(BlockJUnit4ClassRunner.class)
@Category(IntegrationTest.class)
public class RangerSchemaRegistryAuthorizationAgentTest {


    private AuthorizationAgent authorizationAgent;


    @Before
    public void setUp() {
        Map<String, Object> props = new HashMap<>();
        props.put("className",
                "com.hortonworks.registries.schemaregistry.authorization.RangerSchemaRegistryAuthorizationAgent");
        authorizationAgent = AuthorizationAgentFactory.getAuthorizationAgent(props);
    }

    @Test
    public void authorizeListAggregatedSchemas() {
    }

    @Test
    public void authorizeGetAggregatedSchemaInfo() {
    }

    @Test
    public void authorizeFindSchemas() {

        // 1
        // Empty because initial list is empty
        String user = "user1";
        SecurityContext sc = new SecurityContextForTesting(user);
        ArrayList<SchemaMetadataInfo> schemas = new ArrayList();
        Collection<SchemaMetadataInfo> resEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resEmpty.isEmpty());

        // 2
        // Initial list contains one element, but there is no policy for current user that allows the access
        // The result list is empty again

        user = "userNotAuthorized";
        sc = new SecurityContextForTesting(user);
        schemas = new ArrayList();
        SchemaMetadata sm = new SchemaMetadata
                .Builder("Schema1")
                .schemaGroup("Group1")
                .type("avro")
                .build();
        SchemaMetadataInfo smi = new SchemaMetadataInfo(sm);
        schemas.add(smi);
        resEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resEmpty.isEmpty());

        // 3
        // Initial list contains one element, and there is a policy that allows access
        // The result list will contain one elment

        user = "user4";
        sc = new SecurityContextForTesting(user);
        schemas = new ArrayList();
        sm = new SchemaMetadata
                .Builder("Schema1")
                .schemaGroup("Group1")
                .type("avro")
                .build();
        smi = new SchemaMetadataInfo(sm);
        schemas.add(smi);
        Collection<SchemaMetadataInfo> resNonEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resNonEmpty.size() == 1);

        // 4
        // Initial list contains two elements, and there is a policy that allows access to only
        // to the first element of the list
        // The result list will contain one element

        sm = new SchemaMetadata
                .Builder("Schema1")
                .schemaGroup("Group7")
                .type("avro")
                .build();
        smi = new SchemaMetadataInfo(sm);
        schemas.add(smi);
        resNonEmpty = authorizationAgent.authorizeFindSchemas(sc, schemas);
        assertTrue(resNonEmpty.size() == 1);

    }

    @Test
    public void authorizeFindAggregatedSchemas() {
    }

    @Test
    public void authorizeFindSchemasByFields() {
    }

    @Test
    public void authorizeAddSchemaInfo() {
    }

    @Test
    public void authorizeUpdateSchemaInfo() {
    }

    @Test
    public void authorizeGetSchemaInfo() {
    }

    @Test
    public void authorizeDeleteSchemaMetadata() {
    }

    @Test
    public void addSchemaVersion() {
    }

    @Test
    public void authorizeGetLatestSchemaVersion() {
    }

    @Test
    public void authorizeGetSchemaVersion() {
    }

    @Test
    public void authorizeVersionStateOperation() {
    }

    @Test
    public void authorizeCheckCompatibilityWithSchema() {
    }

    @Test
    public void authorizeGetSerializers() {
    }

    @Test
    public void authorizeUploadFile() {
    }

    @Test
    public void authorizeDownloadFile() {
    }

    @Test
    public void authorizeAddSerDes() {
    }

    @Test
    public void authorizeGetSerDes() {
    }

    @Test
    public void authorizeMapSchemaWithSerDes() {
    }

    @Test
    public void authorizeDeleteSchemaVersion() {
    }

    @Test
    public void authorizeGetAllBranches() {
    }

    @Test
    public void authorizeCreateSchemaBranch() {
    }

    @Test
    public void authorizeMergeSchemaVersion() {
    }

    @Test
    public void authorizeDeleteSchemaBranch() {
    }

    @Test
    public void authorizeGetSubjects() {
    }

    @Test
    public void authorizeGetAllVersions() {
    }

    @Test
    public void authorizeGetAllSchemaVersions() {
    }
}