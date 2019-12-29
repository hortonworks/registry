package com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(BlockJUnit4ClassRunner.class)
public class RangerSchemaRegistryAuthorizerImplTest {

    private Authorizer authorizer = new RangerSchemaRegistryAuthorizerImpl();

    @Test
    public void authorizeSerDe() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows reading SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_READ, "user6", groups);
        assertTrue(res);

        ////////////////////////////// CREATE SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_CREATE, "user6", groups);
        assertTrue(res);

        ////////////////////////////// UPDATE SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_UPDATE, "user6", groups);
        assertTrue(res);

        ////////////////////////////// DELETE SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_DELETE, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_DELETE, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for creating SerDes
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_DELETE, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_DELETE, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSerDe(Authorizer.ACCESS_TYPE_DELETE, "user6", groups);
        assertTrue(res);

    }

    @Test
    public void authorizeSchemaGroup() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SchemaGroup
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_READ, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SchemaGroup
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_READ, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SchemaGroup
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_READ, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SchemaGroup
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_READ, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows reading SchemaGroup
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_READ, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_READ, "user6", groups);
        assertTrue(res);

        ///////////////////////////// CREATE SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SchemaGroup
        res = authorizer.authorizeSchemaGroup("Group2", Authorizer.ACCESS_TYPE_CREATE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows creating SchemaGroup
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_CREATE, "user6", groups);
        assertTrue(res);

        //Allow policy exists that allows creating  SchemaGroup. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaGroup("SGroup999", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        ///////////////////////////// DELETE SchemaGroup test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting Group2
        res = authorizer.authorizeSchemaGroup("Group2", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertFalse(res);

        // Allow policy exists that allows deleting SchemaGroup
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaGroup("Group1", Authorizer.ACCESS_TYPE_DELETE, "user6", groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' updating Group2
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorizeSchemaGroup("Group2", Authorizer.ACCESS_TYPE_DELETE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows updating SchemaGroup
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaGroup("SGroup1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorizeSchemaGroup("Group3", Authorizer.ACCESS_TYPE_DELETE, "user8", groups);
        assertTrue(res);

    }

    @Test
    public void authorizeSchema() {
    }

    @Test
    public void authorizeSchemaBranch() {
    }

    @Test
    public void authorizeSchemaVersion() {
    }
}