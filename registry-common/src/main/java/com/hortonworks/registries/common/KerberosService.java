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

import java.util.HashSet;
import java.util.Set;

/** Service to work with Kerberos. */
public interface KerberosService {

    /** Check if kerberos keytabs are configured and tries to load the user. */
    void loadKerberosUser(ServiceAuthenticationConfiguration conf);

    /** Get the groups the user belongs to. */
    Set<String> getGroupsForUser(String kerberosShortName);

    class NoopKerberosService implements KerberosService {

        @Override
        public void loadKerberosUser(ServiceAuthenticationConfiguration conf) { }

        @Override
        public Set<String> getGroupsForUser(String kerberosShortName) {
            return new HashSet<>();
        }
    }
}