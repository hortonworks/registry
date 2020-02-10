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
package com.hortonworks.registries.schemaregistry.authorizer.core.util;

import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import org.apache.hadoop.security.UserGroupInformation;

import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AuthorizationUtils {

    private static Map<String, Authorizer.UserAndGroups> userGroupsStore = new ConcurrentHashMap<>();

    public static Authorizer.UserAndGroups getUserAndGroups(SecurityContext sc) {

        Principal p = sc.getUserPrincipal();
        if(p == null) {
            return null;
        }
        KerberosName kerberosName = new KerberosName(p.getName());

        try {
            String user = kerberosName.getShortName();
            Authorizer.UserAndGroups res = userGroupsStore.get(user);
            if(res != null) {
                return res;
            }
            List<String> groupsList = UserGroupInformation.createRemoteUser(user).getGroups();
            Set<String> groupsSet = new HashSet<>();
            groupsSet.addAll(groupsList);

            res = new Authorizer.UserAndGroups(user, groupsSet);

            userGroupsStore.put(user, res);

            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
