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
package com.hortonworks.registries.schemaregistry.authorizer.agent;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class AuthorizerAgentFactory {
    public static AuthorizerAgent getAuthorizationAgent(Map<String, Object> props) {
        if(props == null
                || props.size() == 0
                || !props.containsKey("className")) {
            return new DummyAuthorizerAgent();
        }

        String cName = (String) props.get("className");
        if(cName.equals("com.hortonworks.registries.schemaregistry.authorizer.agent.DefaultAuthorizerAgent")) {
            return DefaultAuthorizerAgent.INSTANCE;
        }

        // In case if in future we plan to add any other custom AuthorizationAgents
        try {
            Class<AuthorizerAgent> cl = (Class<AuthorizerAgent>) Class.forName(cName);
            Constructor<AuthorizerAgent> constr = cl.getConstructor();
            AuthorizerAgent res = constr.newInstance();

            return res;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException | NoSuchMethodException
                | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
