package com.hortonworks.registries.schemaregistry.authorization;

import java.util.Map;

public class AuthorizationAgentFactory {
    public static AuthorizationAgent getAuthorizationAgent(Map<String, Object> props) {
        if(props == null || props.size() == 0) {
            return new DummyAuthorizationAgent();
        }

        String cName = (String) props.get("className");
        if(cName == null) {
            return new DummyAuthorizationAgent();
        }
        if(cName.equals("com.hortonworks.registries.schemaregistry.authorization.RangerSchemaRegistryAuthorizationAgent")) {
            return RangerSchemaRegistryAuthorizationAgent.INSTANCE;
        }

        // In case if in future we plan to add any other custom authorizer
        try {
            Class<?> cl = Class.forName(cName);
            AuthorizationAgent res = (AuthorizationAgent) cl.newInstance();

            return res;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
