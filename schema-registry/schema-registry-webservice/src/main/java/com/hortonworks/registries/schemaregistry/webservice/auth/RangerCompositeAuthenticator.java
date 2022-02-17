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
package com.hortonworks.registries.schemaregistry.webservice.auth;

import com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler;
import com.cloudera.dim.registry.oauth2.ranger.RangerOAuth2Authenticator;
import com.hortonworks.registries.auth.server.AltKerberosAuthenticationHandler;
import com.hortonworks.registries.auth.server.JWTAuthenticationHandler;
import com.hortonworks.registries.auth.server.KerberosAuthenticationHandler;
import com.hortonworks.registries.auth.server.KerberosBasicAuthenticationHandler;
import com.hortonworks.registries.auth.server.PseudoAuthenticationHandler;
import com.hortonworks.registries.schemaregistry.authorizer.core.Authorizer;
import com.hortonworks.registries.schemaregistry.authorizer.core.RangerAuthenticator;
import com.hortonworks.registries.schemaregistry.authorizer.core.util.RangerKerberosAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.SecurityContext;

/**
 * When we have multiple authentication filters then the principal can
 * come from multiple sources. We need to send the principal to Ranger,
 * but this may be handled differently, depending on the context.
 * <p>
 * For example: when a user is authenticated by OAuth2 then there is no
 * need to contact the Kerberos server to check if the principal exists.
 * </p>
 */
@Singleton
public class RangerCompositeAuthenticator implements RangerAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(RangerCompositeAuthenticator.class);

    private final RangerKerberosAuthenticator kerberosAuthenticator;
    private final RangerOAuth2Authenticator oauth2Authenticator;

    @Inject
    public RangerCompositeAuthenticator(RangerKerberosAuthenticator kerberosAuthenticator,
                                        RangerOAuth2Authenticator oauth2Authenticator) {
        this.kerberosAuthenticator = kerberosAuthenticator;
        this.oauth2Authenticator = oauth2Authenticator;
    }

    @Nullable
    @Override
    public Authorizer.UserAndGroups getUserAndGroups(SecurityContext sc) {
        String authType = sc.getAuthenticationScheme();
        if (authType == null) {
            LOG.trace("No authentication scheme.");
            return null;
        }

        switch (authType) {
            case KerberosAuthenticationHandler.TYPE:
            case KerberosBasicAuthenticationHandler.TYPE:
            case AltKerberosAuthenticationHandler.TYPE:
            case PseudoAuthenticationHandler.TYPE:
                LOG.trace("Using kerberos principal.");
                return getKerberosAuthenticator().getUserAndGroups(sc);

            case JWTAuthenticationHandler.TYPE:
            case OAuth2AuthenticationHandler.TYPE:
                LOG.trace("Using oauth2 principal.");
                return getOAuth2Authenticator().getUserAndGroups(sc);

            default:
                LOG.warn("Unknown authentication scheme [{}], defaulting to kerberos.", authType);
                return getKerberosAuthenticator().getUserAndGroups(sc);
        }
    }

    private RangerKerberosAuthenticator getKerberosAuthenticator() {
        return kerberosAuthenticator;
    }

    private RangerOAuth2Authenticator getOAuth2Authenticator() {
        return oauth2Authenticator;
    }
}
