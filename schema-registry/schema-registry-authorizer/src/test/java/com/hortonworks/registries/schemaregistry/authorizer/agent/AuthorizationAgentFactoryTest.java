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

import com.hortonworks.registries.schemaregistry.authorizer.agent.util.TestAuthorizationAgent;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthorizationAgentFactoryTest {

    @Test
    public void getAuthorizationAgent() {

        Map<String, Object> props = null;
        AuthorizationAgent authorizationAgent = AuthorizationAgentFactory.getAuthorizationAgent(props);
        assertTrue(authorizationAgent instanceof NOOPAuthorizationAgent);

        Map<String, Object> props1 = new HashMap<>();
        AuthorizationAgent authorizationAgent1 = AuthorizationAgentFactory.getAuthorizationAgent(props1);
        assertTrue(authorizationAgent1 instanceof NOOPAuthorizationAgent);

        Map<String, Object> props2 = new HashMap<>();
        props2.put(AuthorizationAgent.AUTHORIZATION_AGENT_CONFIG, NOOPAuthorizationAgent.class.getCanonicalName());
        AuthorizationAgent authorizationAgent2 = AuthorizationAgentFactory.getAuthorizationAgent(props2);
        assertTrue(authorizationAgent2 instanceof NOOPAuthorizationAgent);

        Map<String, Object> props4 = new HashMap<>();
        props4.put(AuthorizationAgent.AUTHORIZATION_AGENT_CONFIG, TestAuthorizationAgent.class.getCanonicalName());
        AuthorizationAgent authorizationAgent4 = AuthorizationAgentFactory.getAuthorizationAgent(props4);
        assertTrue(authorizationAgent4 instanceof TestAuthorizationAgent);

    }
}
