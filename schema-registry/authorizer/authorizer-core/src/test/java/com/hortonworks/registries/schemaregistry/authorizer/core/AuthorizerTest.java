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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@RunWith(BlockJUnit4ClassRunner.class)
public class AuthorizerTest {

    @Test
    public void authorizerResourcesTests() {
        Authorizer.Resource resource = new Authorizer.SerdeResource();
        String res = "SerDe{ serDeName='*' }";
        assertThat(resource.toString(), is(res));
        assertThat(resource.getResourceType(), is(Authorizer.ResourceType.SERDE));

        resource = new Authorizer.SchemaMetadataResource("Group", "Schema");
        res = "SchemaMetadata{ schemaGroupName='Group', schemaMetadataName='Schema' }";
        assertThat(resource.toString(), is(res));
        assertThat(resource.getResourceType(), is(Authorizer.ResourceType.SCHEMA_METADATA));

        resource = new Authorizer.SchemaBranchResource("Group", "Schema", "Branch");
        res = "SchemaBranch{ schemaGroupName='Group', schemaMetadataName='Schema'" +
                ", schemaBranchName='Branch' }";
        assertThat(resource.toString(), is(res));
        assertThat(resource.getResourceType(), is(Authorizer.ResourceType.SCHEMA_BRANCH));

        resource = new Authorizer.SchemaVersionResource("Group", "Schema", "Branch");
        res = "SchemaVersion{ schemaGroupName='Group', schemaMetadataName='Schema'" +
                ", schemaBranchName='Branch', schemaVersionName='*' }";
        assertThat(resource.toString(), is(res));
        assertThat(resource.getResourceType(), is(Authorizer.ResourceType.SCHEMA_VERSION));
    }

}