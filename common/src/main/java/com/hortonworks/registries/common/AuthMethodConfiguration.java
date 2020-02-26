/**
 * Copyright 2016-2019 Cloudera, Inc.
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
 **/
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
