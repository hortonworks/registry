/*
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.common;

import com.hortonworks.registries.common.ha.LeadershipParticipant;

import java.util.Map;

/**
 * Configuration for HighAvailability.
 * {@code className} represents {@link LeadershipParticipant} implementation class
 * {@code config} respective configuration for the above class.
 *
 * If no HAConfiguration is not configured on schema registry cluster nodes then it is considered that each node in the
 * cluster is eligible for writes.
 *
 */
public class HAConfiguration {
    private String className;
    private Map<String, Object> config;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "HAConfiguration{" +
                "className='" + className + '\'' +
                ", config=" + config +
                '}';
    }
}
