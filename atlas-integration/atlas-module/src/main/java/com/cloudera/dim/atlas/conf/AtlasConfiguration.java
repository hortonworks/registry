/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.cloudera.dim.atlas.conf;

import com.google.common.collect.ImmutableMap;
import com.hortonworks.registries.common.ModuleDetailsConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Configuration for the Atlas bundle. */
public class AtlasConfiguration extends ModuleDetailsConfiguration {

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

    public List<String> getAtlasUrls() {
        return atlasUrls;
    }

    public void setAtlasUrls(List<String> atlasUrls) {
        this.atlasUrls = atlasUrls;
    }

    public BasicAuth getBasicAuth() {
        return basicAuth;
    }

    public void setBasicAuth(BasicAuth basicAuth) {
        this.basicAuth = basicAuth;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, Object> asMap() {
        Map<String, Object> result = new HashMap<>();
        result.put("atlasUrls", atlasUrls);
        result.put("enabled", enabled);
        result.put("basicAuth", basicAuth == null ? null : ImmutableMap.of(
                "username", basicAuth.username,
                "password", basicAuth.password
        ));

        return Collections.unmodifiableMap(result);
    }
}
