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
package com.hortonworks.registries.schemaregistry.authorizer.agent;

import com.hortonworks.registries.schemaregistry.authorizer.test.FakeAuthorizationAgent;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class AuthorizationAgentFactoryTest {

    @Test
    public void getAuthorizationAgent() {

        Map<String, Object> props = null;
        AuthorizationAgent authorizationAgent = AuthorizationAgentFactory.getAuthorizationAgent(props);
        assertTrue(authorizationAgent instanceof DummyAuthorizationAgent);

        Map<String, Object> props1 = new HashMap<>();
        AuthorizationAgent authorizationAgent1 = AuthorizationAgentFactory.getAuthorizationAgent(props1);
        assertTrue(authorizationAgent1 instanceof DummyAuthorizationAgent);

        Map<String, Object> props2 = new HashMap<>();
        props2.put("authorizationAgentClassName", DummyAuthorizationAgent.class.getCanonicalName());
        AuthorizationAgent authorizationAgent2 = AuthorizationAgentFactory.getAuthorizationAgent(props2);
        assertTrue(authorizationAgent2 instanceof DummyAuthorizationAgent);

        Map<String, Object> props3 = new HashMap<>();
        props3.put("authorizationAgentClassName", DefaultAuthorizationAgent.class.getCanonicalName());
        AuthorizationAgent authorizationAgent3 = AuthorizationAgentFactory.getAuthorizationAgent(props3);
        assertTrue(authorizationAgent3 instanceof DefaultAuthorizationAgent);

        Map<String, Object> props4 = new HashMap<>();
        props4.put("authorizationAgentClassName", FakeAuthorizationAgent.class.getCanonicalName());
        AuthorizationAgent authorizationAgent4 = AuthorizationAgentFactory.getAuthorizationAgent(props4);
        assertTrue(authorizationAgent4 instanceof FakeAuthorizationAgent);

    }
}
