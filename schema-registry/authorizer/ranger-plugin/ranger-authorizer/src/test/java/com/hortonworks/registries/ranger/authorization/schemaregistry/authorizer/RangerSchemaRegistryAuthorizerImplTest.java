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
package com.hortonworks.registries.ranger.authorization.schemaregistry.authorizer;


import com.hortonworks.registries.schemaregistry.authorizer.ranger.RangerSchemaRegistryAuthorizerImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;

@RunWith(BlockJUnit4ClassRunner.class)
public class RangerSchemaRegistryAuthorizerImplTest {

    private Authorizer authorizer = new RangerSchemaRegistryAuthorizerImpl();

    @Test
    public void authorizeSerDe() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.READ, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.READ, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.READ, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.READ, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows reading SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.READ, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.READ, "user6", groups);
        assertTrue(res);

        ////////////////////////////// CREATE SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.CREATE, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.CREATE, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.CREATE, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.CREATE, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.CREATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.CREATE, "user6", groups);
        assertTrue(res);

        ////////////////////////////// UPDATE SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.UPDATE, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.UPDATE, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.UPDATE, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.UPDATE, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.UPDATE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.UPDATE, "user6", groups);
        assertTrue(res);

        ////////////////////////////// DELETE SerDes test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.DELETE, "user1", groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.DELETE, "user2", groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for creating SerDes
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.DELETE, "user3", groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.DELETE, "user4", groups);
        assertTrue(res);

        // Allow policy exists that allows creating SerDes
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows creating registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SerdeResource(), Authorizer.AccessType.DELETE, "user6", groups);
        assertTrue(res);

    }

    @Test
    public void authorizeSchema() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ Schema test cases ////////////////////////////

        // No policy for user1 that 'allows' reading Schema
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema2"),
                Authorizer.AccessType.READ,
                "user1",
                groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading Schema
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.READ,
                "user2",
                groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading Schema
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.READ,
                "user3",
                groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading Schema
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.READ,
                "user4",
                groups);
        assertTrue(res);

        // Allow policy exists that allows reading Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema3"),
                Authorizer.AccessType.READ,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.READ,
                "user6",
                groups);
        assertTrue(res);

        ///////////////////////////// CREATE Schema test cases ////////////////////////////

        // No policy for user1 that 'allows' creating Schema
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group2", "Schema1"),
                Authorizer.AccessType.CREATE,
                "user1",
                groups);
        assertFalse(res);

        // Allow policy exists that allows creating Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.CREATE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.CREATE,
                "user6",
                groups);
        assertTrue(res);

        //Allow policy exists that allows creating  Schema. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("SGroup999", "__Schema4"),
                Authorizer.AccessType.CREATE,
                "user5",
                groups);
        assertTrue(res);

        ///////////////////////////// DELETE Schema test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting Schema
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group2", "Schema1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertFalse(res);

        // Allow policy exists that allows deleting Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.DELETE, "user5", groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group1", "Schema1"),
                Authorizer.AccessType.DELETE,
                "user6",
                groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' updating Schema
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group2", "Schema1"),
                Authorizer.AccessType.DELETE, "user1", groups);
        assertFalse(res);

        // Allow policy exists that allows updating Schema
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("SGroup1", "Schema5"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("Group3", "Schema1"),
                Authorizer.AccessType.DELETE,
                "user8",
                groups);
        assertTrue(res);

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is not allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("GroupSpecial121", "ANY"),
                Authorizer.AccessType.READ,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("GroupSpecial121", "ANY"),
                Authorizer.AccessType.CREATE,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("GroupSpecial121", "ANY"),
                Authorizer.AccessType.UPDATE,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaMetadataResource("GroupSpecial121", "ANY"),
                Authorizer.AccessType.DELETE,
                "UserSpecial",
                groups);
        assertFalse(res);

    }

    @Test
    public void authorizeSchemaBranch() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SchemaBranch test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SchemaBranch
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user1",
                groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SchemaBranch
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user2",
                groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SchemaBranch
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user3",
                groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SchemaBranch
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user4",
                groups);
        assertTrue(res);

        // Allow policy exists that allows reading SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user6",
                groups);
        assertTrue(res);

        ///////////////////////////// CREATE SchemaBranch test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SchemaBranch
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group2", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user1",
                groups);
        assertFalse(res);

        // Allow policy exists that allows creating SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user6",
                groups);
        assertTrue(res);

        //Allow policy exists that allows creating  SchemaBranch. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("SGroup999", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user5",
                groups);
        assertTrue(res);

        ///////////////////////////// DELETE SchemaBranch test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting SchemaBranch
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group2", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertFalse(res);

        // Allow policy exists that allows deleting SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group1", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user6",
                groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaBranch test cases ////////////////////////////

        // No policy for user1 that 'allows' updating SchemaBranch
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group2", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user1",
                groups);
        assertFalse(res);

        // Allow policy exists that allows updating SchemaBranch
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("SGroup1", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("Group3", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user8",
                groups);
        assertTrue(res);

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.READ,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.CREATE,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.UPDATE,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaBranchResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.DELETE,
                "UserSpecial",
                groups);
        assertFalse(res);
    }

    @Test
    public void authorizeSchemaVersion() {
        Set<String> groups = new HashSet<>();
        boolean res = false;

        ///////////////////////////// READ SchemaVersion test cases ////////////////////////////

        // No policy for user1 that 'allows' reading SchemaVersion
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user1",
                groups);
        assertFalse(res);

        // Deny policy for user2 that 'denies' reading SchemaVersion
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user2",
                groups);
        assertFalse(res);

        // Deny policy for user3 that 'excludes from allows' for reading SchemaVersion
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user3",
                groups);
        assertFalse(res);

        // Exclude from deny policy exists that allows reading SchemaVersion
        groups = new HashSet<>();
        groups.add("user4");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user4",
                groups);
        assertTrue(res);

        // Allow policy exists that allows reading SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows reading registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.READ,
                "user6",
                groups);
        assertTrue(res);

        ///////////////////////////// CREATE SchemaVersion test cases ////////////////////////////

        // No policy for user1 that 'allows' creating SchemaVersion
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group2", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user1",
                groups);
        assertFalse(res);

        // Allow policy exists that allows creating SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows creating for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user6",
                groups);
        assertTrue(res);

        //Allow policy exists that allows creating  SchemaVersion. The group is specified with regexp
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("SchemaGroup999", "Schema1", "Branch1"),
                Authorizer.AccessType.CREATE,
                "user5",
                groups);
        assertTrue(res);

        ///////////////////////////// DELETE SchemaVersion test cases ////////////////////////////

        // No policy for user5 that 'allows' deleting Schema
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group2", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertFalse(res);

        // Allow policy exists that allows deleting SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows deleting for registry-service
        groups = new HashSet<>();
        groups.add("user6");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group10", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user6",
                groups);
        assertTrue(res);

        ///////////////////////////// UPDATE SchemaGroup test cases ////////////////////////////

        // No policy for user1 that 'allows' updating SchemaVersion
        groups = new HashSet<>();
        groups.add("user1");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group2", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user1",
                groups);
        assertFalse(res);

        // Allow policy exists that allows updating SchemaVersion
        groups = new HashSet<>();
        groups.add("user5");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("SchemaGroup1", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user5",
                groups);
        assertTrue(res);

        // Allow policy exists that allows updating for registry-service
        groups = new HashSet<>();
        groups.add("user8");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("Group3", "Schema1", "Branch1"),
                Authorizer.AccessType.DELETE,
                "user8",
                groups);
        assertTrue(res);

        //////////////////////////////////////////////// Some special test cases ///////////////////////////////////////

        // Read is allowed and other operations are not allowed
        groups = new HashSet<>();
        groups.add("UserSpecial");
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.READ,
                "UserSpecial",
                groups);
        assertTrue(res);
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.CREATE,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.DELETE,
                "UserSpecial",
                groups);
        assertFalse(res);
        res = authorizer.authorize(new Authorizer.SchemaVersionResource("GroupSpecial121", "ANY", "ANY"),
                Authorizer.AccessType.UPDATE,
                "UserSpecial",
                groups);
        assertFalse(res);

    }
}