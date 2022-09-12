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
import com.hortonworks.registries.common.KerberosService;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.RangerAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

/** Authenticator which retrieves validates the principal with kerberos. */
@Singleton
public class RangerKerberosAuthenticator implements RangerAuthenticator {

    private static final Map<String, Authorizer.UserAndGroups> USER_GROUPS_STORE = new ConcurrentHashMap<>();

    private static final Logger LOG = LoggerFactory.getLogger(RangerKerberosAuthenticator.class);

    private final KerberosService kerberosService;

    @Inject
    public RangerKerberosAuthenticator(KerberosService kerberosService) {
        this.kerberosService = checkNotNull(kerberosService, "kerberosService");
    }

    @Nullable
    public Authorizer.UserAndGroups getUserAndGroups(SecurityContext sc) {

        Principal p = sc.getUserPrincipal();
        if (p == null) {
            return null;
        }
        KerberosName kerberosName = new KerberosName(p.getName());

        try {
            String user = kerberosName.getShortName();
            Authorizer.UserAndGroups res = USER_GROUPS_STORE.get(user);
            if (res != null) {
                return res;
            }

            Set<String> groupsSet = kerberosService.getGroupsForUser(user);

            res = new Authorizer.UserAndGroups(user, groupsSet);

            USER_GROUPS_STORE.put(user, res);

            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("Error while getting hadoop user and groups for the principal.", e);
            return null;
        }
    }

}
