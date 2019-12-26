package com.hortonworks.registries.schemaregistry.authorization;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class AuthorizationAgentFactory {
    public static AuthorizationAgent getAuthorizationAgent(Map<String, Object> props) {
        if(props == null
                || props.size() == 0
                || !props.containsKey("className")) {
            return new DummyAuthorizationAgent();
        }

        String cName = (String) props.get("className");
        if(cName.equals("com.hortonworks.registries.schemaregistry.authorization.RangerSchemaRegistryAuthorizationAgent")) {
            return RangerSchemaRegistryAuthorizationAgent.INSTANCE;
        }

        // In case if in future we plan to add any other custom authorizer
        try {
            Class<AuthorizationAgent> cl = (Class<AuthorizationAgent>) Class.forName(cName);
            Constructor<AuthorizationAgent> constr = cl.getConstructor();
            AuthorizationAgent res = constr.newInstance();

            return res;
        } catch (ClassNotFoundException | InstantiationException
                | IllegalAccessException | NoSuchMethodException
                | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
