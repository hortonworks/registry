package com.hortonworks.registries.schemaregistry.authorization.util;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;

public class SecurityContextForTesting implements SecurityContext {
    private Principal userPrincipal;

    public SecurityContextForTesting(String userPrincipal) {
        this.userPrincipal = () -> userPrincipal;
    }

    @Override
    public Principal getUserPrincipal() {
        return userPrincipal;
    }

    @Override
    public boolean isUserInRole(String s) {
        return false;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public String getAuthenticationScheme() {
        return null;
    }
}
