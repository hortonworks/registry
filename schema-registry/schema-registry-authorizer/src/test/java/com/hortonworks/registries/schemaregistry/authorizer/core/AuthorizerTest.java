/*
 * Copyright 2016-2020 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.authorizer.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuthorizerTest {

    @Test
    public void authorizerResourcesTests() {
        Authorizer.Resource resource = new Authorizer.SerdeResource();
        String res = "SerDe{ serDeName='*' }";
        assertEquals(res, resource.toString());
        assertEquals(Authorizer.ResourceType.SERDE, resource.getResourceType());

        resource = new Authorizer.SchemaMetadataResource("Group", "Schema");
        res = "SchemaMetadata{ schemaGroupName='Group', schemaMetadataName='Schema' }";
        assertEquals(res, resource.toString());
        assertEquals(Authorizer.ResourceType.SCHEMA_METADATA, resource.getResourceType());

        resource = new Authorizer.SchemaBranchResource("Group", "Schema", "Branch");
        res = "SchemaBranch{ schemaGroupName='Group', schemaMetadataName='Schema'" +
                ", schemaBranchName='Branch' }";
        assertEquals(res, resource.toString());
        assertEquals(Authorizer.ResourceType.SCHEMA_BRANCH, resource.getResourceType());

        resource = new Authorizer.SchemaVersionResource("Group", "Schema", "Branch");
        res = "SchemaVersion{ schemaGroupName='Group', schemaMetadataName='Schema'" +
                ", schemaBranchName='Branch', schemaVersionName='*' }";
        assertEquals(res, resource.toString());
        assertEquals(Authorizer.ResourceType.SCHEMA_VERSION, resource.getResourceType());
    }

}