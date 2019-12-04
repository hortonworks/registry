package com.hortonworks.registries.common;

public class AuthMethodConfiguration {
    private String type;
    private String serverPrinciple;
    private String serverPrincipleKeytab;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getServerPrinciple() {
        return serverPrinciple;
    }

    public void setServerPrinciple(String serverPrinciple) {
        this.serverPrinciple = serverPrinciple;
    }

    public String getServerPrincipleKeytab() {
        return serverPrincipleKeytab;
    }

    public void setServerPrincipleKeytab(String serverPrincipleKeytab) {
        this.serverPrincipleKeytab = serverPrincipleKeytab;
    }

    @Override
    public String toString() {
        return "AuthMethodConfiguration{" +
                "type='" + type + '\'' +
                ", serverPrinciple='" + serverPrinciple + '\'' +
                ", serverPrincipleKeytab='" + serverPrincipleKeytab + '\'' +
                '}';
    }
}
