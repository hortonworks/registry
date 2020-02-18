/**
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
 **/

package com.hortonworks.registries.common;

import java.util.Map;

public class ServiceAuthenticationConfiguration {

    public enum AuthenticationType {
        KERBEROS("kerberos");

        String type;

        AuthenticationType(String type) {
            this.type = type;
        }
    }

    private AuthenticationType type;

    private Map<String, ?> properties;

    public AuthenticationType getType() {
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
