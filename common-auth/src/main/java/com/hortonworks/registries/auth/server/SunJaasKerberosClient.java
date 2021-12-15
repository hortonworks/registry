/*
 * Copyright 2009-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.hortonworks.registries.auth.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// Apache 2.0 notice: the logging framework is changed to slf4j

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.CredentialException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.HashMap;

/**
 * Implementation of {@link KerberosClient} which uses the SUN JAAS
 * login module, which is included in the SUN JRE, it will not work with an IBM JRE.
 * The whole configuration is done in this class, no additional JAAS configuration
 * is needed.
 *
 * @author Mike Wiesner
 * @since 1.0
 */
// Apache 2.0 notice: removed the KerberosClient interface
public class SunJaasKerberosClient {

    private static final Logger LOG = LoggerFactory.getLogger(SunJaasKerberosClient.class);
    private boolean debug = false;

    public String login(String username, String password) throws CredentialException {
        LOG.debug("Trying to authenticate " + username + " with Kerberos");
        String validatedUsername;

        try {
            LoginContext loginContext = new LoginContext("", null, new KerberosClientCallbackHandler(username, password),
                    new LoginConfig(this.debug));
            loginContext.login();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos authenticated user: {}", loginContext.getSubject());
            }
            validatedUsername = loginContext.getSubject().getPrincipals().iterator().next().toString();
            loginContext.logout();
        } catch (LoginException e) {
            // Apache 2 notice: exception thrown is changed
            throw new CredentialException("Kerberos authentication failed");
        }
        return validatedUsername;

    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    private static class LoginConfig extends Configuration {
        private boolean debug;

        public LoginConfig(boolean debug) {
            super();
            this.debug = debug;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            HashMap<String, String> options = new HashMap<String, String>();
            options.put("storeKey", "true");
            if (debug) {
                options.put("debug", "true");
            }

            return new AppConfigurationEntry[] { new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options), };
        }

    }

    private static class KerberosClientCallbackHandler implements CallbackHandler {
        private String username;
        private String password;

        public KerberosClientCallbackHandler(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) callback;
                    ncb.setName(username);
                } else if (callback instanceof PasswordCallback) {
                    PasswordCallback pwcb = (PasswordCallback) callback;
                    pwcb.setPassword(password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback, "We got a " + callback.getClass().getCanonicalName()
                            + ", but only NameCallback and PasswordCallback is supported");
                }
            }

        }

    }

}
