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
package com.hortonworks.registries.schemaregistry.authorizer.ranger.shim;


import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;

public class RangerSchemaRegistryAuthorizerTest {

    @Test
    public void authorize() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        // Just sanity check. It shows that AuthorizerImpl is created correctly through reflection

        Authorizer authorizer = new RangerSchemaRegistryAuthorizer();
        authorizer.configure(new HashMap<>());

        Authorizer.Resource resource = new Authorizer.SerdeResource();
        String user = "user1";
        Set<String> uGroups = new HashSet<>();
        Authorizer.UserAndGroups uag = new Authorizer.UserAndGroups(user, uGroups);

        assertFalse(authorizer.authorize(resource, Authorizer.AccessType.READ, uag));

    }
}