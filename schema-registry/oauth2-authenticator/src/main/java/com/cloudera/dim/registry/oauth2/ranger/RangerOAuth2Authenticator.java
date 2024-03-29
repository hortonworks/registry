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
package com.cloudera.dim.registry.oauth2.ranger;

import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.RangerAuthenticator;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import javax.ws.rs.core.SecurityContext;
import java.util.HashSet;

/**
 * The principal was extracted in {@link com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler}
 * and we need to return a Ranger user containing the same principal.
 * */
@Singleton
public class RangerOAuth2Authenticator implements RangerAuthenticator {

    @Nullable
    @Override
    public Authorizer.UserAndGroups getUserAndGroups(SecurityContext sc) {
        if (sc.getUserPrincipal() == null) {
            return null;
        }

        return new Authorizer.UserAndGroups(sc.getUserPrincipal().getName(), new HashSet<>());
    }

}
