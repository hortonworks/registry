/*
 * Copyright 2016-2020 Cloudera, Inc.
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
 */
package com.hortonworks.registries.schemaregistry.authorizer.agent;

import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthorizationUtils {
    public static Authorizer.UserAndGroups getUserAndGroups(SecurityContext sc) {

        Principal p = sc.getUserPrincipal();
        KerberosName kerberosName = new KerberosName(p.getName());

        try {
            String user = kerberosName.getShortName();
            Set<String> groups = new HashSet<>();
            List<String> tmp = UserGroupInformation.createRemoteUser(user).getGroups();
            groups.addAll(tmp);

            return new Authorizer.UserAndGroups(user,groups);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
