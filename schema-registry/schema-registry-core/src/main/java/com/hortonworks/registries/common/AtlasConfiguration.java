/**
 * Copyright 2016-2022 Cloudera, Inc.
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

import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Configuration for the Atlas bundle. */
public class AtlasConfiguration {

    public static class BasicAuth {

        private String username;
        private String password;

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    private List<String> atlasUrls = new ArrayList<>();
    private BasicAuth basicAuth;
    private boolean enabled = false;
    private long waitBetweenAuditProcessing = 60000L;
    private String customClasspathLoader;
    private String customClasspath;
    private boolean connectWithKafka = true;
    private String customPluginProvider;

    /** List of Atlas REST URLs. */
    public List<String> getAtlasUrls() {
        return atlasUrls;
    }

    public void setAtlasUrls(List<String> atlasUrls) {
        this.atlasUrls = atlasUrls;
    }

    /** When connecting to Atlas via Basic authentication, a username and password needs to be provided. */
    public BasicAuth getBasicAuth() {
        return basicAuth;
    }

    public void setBasicAuth(BasicAuth basicAuth) {
        this.basicAuth = basicAuth;
    }

    /** Is Atlas integration enabled? If this is false, all other properties are ignored. */
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /** The AtlasEventProcessor runs a loop and checks the database for new events. By default, it does
     * the check every 1 minute. This value is given in ms. */
    public long getWaitBetweenAuditProcessing() {
        return waitBetweenAuditProcessing;
    }

    public void setWaitBetweenAuditProcessing(long waitBetweenAuditProcessing) {
        this.waitBetweenAuditProcessing = waitBetweenAuditProcessing;
    }

    /** Atlas is running on a separate classpath. We can provide a custom classpath loader if we want to load it differently. */
    public String getCustomClasspathLoader() {
        return customClasspathLoader;
    }

    public void setCustomClasspathLoader(String customClasspathLoader) {
        this.customClasspathLoader = customClasspathLoader;
    }

    /** The custom path to the Atlas JARs. */
    public String getCustomClasspath() {
        return customClasspath;
    }

    public void setCustomClasspath(String customClasspath) {
        this.customClasspath = customClasspath;
    }

    /** Connect schema with a Kafka topic */
    public boolean isConnectWithKafka() {
        return connectWithKafka;
    }

    public void setConnectWithKafka(boolean connectWithKafka) {
        this.connectWithKafka = connectWithKafka;
    }

    public String getCustomPluginProvider() {
        return customPluginProvider;
    }

    public void setCustomPluginProvider(String customPluginProvider) {
        this.customPluginProvider = customPluginProvider;
    }

    /** Convert this Java object to a Map. */
    public Map<String, Object> asMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("atlasUrls", atlasUrls);
        result.put("enabled", enabled);
        result.put("waitBetweenAuditProcessing", waitBetweenAuditProcessing);
        result.put("customClasspathLoader", customClasspathLoader);
        result.put("customClasspath", customClasspath);
        result.put("connectWithKafka", connectWithKafka);
        result.put("basicAuth", basicAuth == null ? null : ImmutableMap.of(
                "username", basicAuth.username,
                "password", basicAuth.password
        ));
        result.put("customPluginProvider", customPluginProvider);

        return Collections.unmodifiableMap(result);
    }
}
