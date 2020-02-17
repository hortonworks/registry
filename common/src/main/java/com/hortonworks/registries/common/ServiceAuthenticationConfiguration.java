package com.hortonworks.registries.common;

import java.util.Map;

public class ServiceAuthenticationConfiguration {

    private String type;

    private Map<String,?> properties;

    public String getType() {
        return type;
    }

    public Map<String, ?> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "ServiceAuthenticationConfiguration{" +
                "type='" + type + '\'' +
                ", properties=" + properties +
                '}';
    }
}
