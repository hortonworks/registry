package com.hortonworks.registries.common;

import java.util.Map;

/**
 * A class representing information for a servlet filter to be registered while starting web server
 */
public class ServletFilterConfiguration {
    private String className;
    private Map<String, String> params;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return "ServletFilterConfiguration{" +
                "className='" + className + '\'' +
                ", params=" + params +
                '}';
    }


}
