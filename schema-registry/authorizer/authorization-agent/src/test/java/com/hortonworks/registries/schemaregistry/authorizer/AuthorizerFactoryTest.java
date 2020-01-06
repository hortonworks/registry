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
package com.hortonworks.registries.schemaregistry.authorizer;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.ranger.shim.RangerSchemaRegistryAuthorizer;
import com.hortonworks.registries.schemaregistry.authorizer.test.TestAuthorizer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

@RunWith(BlockJUnit4ClassRunner.class)
public class AuthorizerFactoryTest {

    @Test
    public void getAuthorizer() {

        Map<String, Object> props = null;
        Authorizer authorizer = AuthorizerFactory.getAuthorizer(props);
        assertTrue(authorizer instanceof RangerSchemaRegistryAuthorizer);

        Map<String, Object> props1 = new HashMap<>();
        Authorizer authorizer1 = AuthorizerFactory.getAuthorizer(props1);
        assertTrue(authorizer1 instanceof RangerSchemaRegistryAuthorizer);

        Map<String, Object> props2 = new HashMap<>();
        props2.put("authorizerClassName", RangerSchemaRegistryAuthorizer.class.getCanonicalName());
        Authorizer authorizer2 = AuthorizerFactory.getAuthorizer(props2);
        assertTrue(authorizer2 instanceof RangerSchemaRegistryAuthorizer);

        Map<String, Object> props3 = new HashMap<>();
        props3.put("authorizerClassName", TestAuthorizer.class.getCanonicalName());
        Authorizer authorizer3 = AuthorizerFactory.getAuthorizer(props3);
        assertTrue(authorizer3 instanceof TestAuthorizer);

    }

}
