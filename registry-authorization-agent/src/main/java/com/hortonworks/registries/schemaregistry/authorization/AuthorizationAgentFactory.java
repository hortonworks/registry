package com.hortonworks.registries.schemaregistry.authorization;

public class AuthorizationAgentFactory {
    public static AuthorizationAgent getAuthorizationAgent(boolean isSecurityOn) {
        if (isSecurityOn) {
            return RangerSchemaRegistryAuthorizationAgent.INSTANCE;
        }

        return new DummyAuthorizationAgent();
    }
}
