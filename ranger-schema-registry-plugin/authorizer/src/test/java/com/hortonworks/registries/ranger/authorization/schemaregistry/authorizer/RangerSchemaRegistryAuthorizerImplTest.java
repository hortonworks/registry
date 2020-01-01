package com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer;


import org.junit.Ignore;
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
    @Ignore // authorizeSchemaGroup is not used. Schema Group is not first class object yet.
    public void authorizeSchemaGroup() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SchemaGroup
        res = authorizer.authorizeSchemaGroup("Group10", Authorizer.ACCESS_TYPE_READ, "user1", groups);
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

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is not allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorizeSchemaGroup("GroupSpecial121", Authorizer.ACCESS_TYPE_READ, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaGroup("GroupSpecial121", Authorizer.ACCESS_TYPE_CREATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaGroup("GroupSpecial121", Authorizer.ACCESS_TYPE_UPDATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaGroup("GroupSpecial121", Authorizer.ACCESS_TYPE_DELETE, "UserSpecial", groups);
        assertFalse(res);

    }

    @Test
    public void authorizeSchema() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ Schema test cases ////////////////////////////

        // No policy for user1 that 'allows' reading Schema
        res = authorizer.authorizeSchema("Group1", "Schema2", Authorizer.ACCESS_TYPE_READ, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading Schema
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_READ, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading Schema
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_READ, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading Schema
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_READ, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows reading Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchema("Group1", "Schema3", Authorizer.ACCESS_TYPE_READ, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_READ, "user6", groups);
        assertTrue(res);

        ///////////////////////////// CREATE Schema test cases ////////////////////////////

        // No policy for user1 that 'allows' creating Schema
        res = authorizer.authorizeSchema("Group2", "Schema1", Authorizer.ACCESS_TYPE_CREATE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows creating Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_CREATE, "user6", groups);
        assertTrue(res);

        //Allow policy exists that allows creating  Schema. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchema("SGroup999", "__Schema4", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        ///////////////////////////// DELETE Schema test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting Schema
        res = authorizer.authorizeSchema("Group2", "Schema1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertFalse(res);

        // Allow policy exists that allows deleting Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchema("Group1", "Schema1", Authorizer.ACCESS_TYPE_DELETE, "user6", groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' updating Schema
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorizeSchema("Group2", "Schema1", Authorizer.ACCESS_TYPE_DELETE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows updating Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchema("SGroup1", "Schema5", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorizeSchema("Group3", "Schema1", Authorizer.ACCESS_TYPE_DELETE, "user8", groups);
        assertTrue(res);

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is not allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorizeSchema("GroupSpecial121", "ANY", Authorizer.ACCESS_TYPE_READ, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchema("GroupSpecial121", "ANY", Authorizer.ACCESS_TYPE_CREATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchema("GroupSpecial121", "ANY", Authorizer.ACCESS_TYPE_UPDATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchema("GroupSpecial121", "ANY", Authorizer.ACCESS_TYPE_DELETE, "UserSpecial", groups);
        assertFalse(res);

    }

    @Test
    public void authorizeSchemaBranch() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SchemaBranch test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SchemaBranch
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SchemaBranch
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SchemaBranch
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SchemaBranch
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows reading SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user6", groups);
        assertTrue(res);

        ///////////////////////////// CREATE SchemaBranch test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SchemaBranch
        res = authorizer.authorizeSchemaBranch("Group2", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows creating SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user6", groups);
        assertTrue(res);

        //Allow policy exists that allows creating  SchemaBranch. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaBranch("SGroup999", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        ///////////////////////////// DELETE SchemaBranch test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting SchemaBranch
        res = authorizer.authorizeSchemaBranch("Group2", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertFalse(res);

        // Allow policy exists that allows deleting SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaBranch("Group1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user6", groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaBranch test cases ////////////////////////////

        // No policy for user1 that 'allows' updating SchemaBranch
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorizeSchemaBranch("Group2", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows updating SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaBranch("SGroup1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorizeSchemaBranch("Group3", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user8", groups);
        assertTrue(res);

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorizeSchemaBranch("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_READ, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaBranch("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_CREATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaBranch("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_UPDATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaBranch("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_DELETE, "UserSpecial", groups);
        assertFalse(res);
    }

    @Test
    public void authorizeSchemaVersion() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SchemaVersion test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SchemaVersion
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SchemaVersion
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SchemaVersion
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SchemaVersion
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows reading SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_READ, "user6", groups);
        assertTrue(res);

        ///////////////////////////// CREATE SchemaVersion test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SchemaVersion
        res = authorizer.authorizeSchemaVersion("Group2", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows creating SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user6", groups);
        assertTrue(res);

        //Allow policy exists that allows creating  SchemaVersion. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaVersion("SchemaGroup999", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_CREATE, "user5", groups);
        assertTrue(res);

        ///////////////////////////// DELETE SchemaVersion test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting Schema
        res = authorizer.authorizeSchemaVersion("Group2", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertFalse(res);

        // Allow policy exists that allows deleting SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorizeSchemaVersion("Group10", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user6", groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' updating SchemaVersion
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorizeSchemaVersion("Group2", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows updating SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorizeSchemaVersion("SchemaGroup1", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorizeSchemaVersion("Group3", "Schema1", "Branch1", Authorizer.ACCESS_TYPE_DELETE, "user8", groups);
        assertTrue(res);

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorizeSchemaVersion("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_READ, "UserSpecial", groups);
        assertTrue(res);
        res = authorizer.authorizeSchemaVersion("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_CREATE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaVersion("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_DELETE, "UserSpecial", groups);
        assertFalse(res);
        res = authorizer.authorizeSchemaVersion("GroupSpecial121", "ANY", "ANY", Authorizer.ACCESS_TYPE_UPDATE, "UserSpecial", groups);
        assertFalse(res);

    }
}