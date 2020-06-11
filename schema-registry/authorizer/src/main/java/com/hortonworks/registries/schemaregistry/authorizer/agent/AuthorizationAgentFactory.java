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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * This is a factory class that is used to create an instance of  {@link AuthorizationAgent}.
 * The default authorizationAgent is {@link NOOPAuthorizationAgent}. So, authorization is off
 * by default.
 * User defined authorization agents are supported.
 * The exact type of authorization agent is configured by 'authorizationAgentClassName' property.
 */
public class AuthorizationAgentFactory {

    public static AuthorizationAgent getAuthorizationAgent(Map<String, Object> props) {

        final String authorizationAgentClassName;
        if(props == null || !props.containsKey(AuthorizationAgent.AUTHORIZATION_AGENT_CONFIG)) {
            authorizationAgentClassName = NOOPAuthorizationAgent.class.getCanonicalName();
        } else {
            authorizationAgentClassName = (String) props.get(AuthorizationAgent.AUTHORIZATION_AGENT_CONFIG);
        }

        try {
            Class<AuthorizationAgent> cl = (Class<AuthorizationAgent>) Class.forName(authorizationAgentClassName);
            Constructor<AuthorizationAgent> constr = cl.getConstructor();
            AuthorizationAgent agent = constr.newInstance();
            agent.configure(props);

            return agent;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException | NoSuchMethodException
                | NoClassDefFoundError | InvocationTargetException e) {
            throw new RuntimeException("Could not initialize agent " + authorizationAgentClassName, e);
        }
    }

}
