/**
 * Copyright 2016-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.hortonworks.registries.auth.server;

import java.io.IOException;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.util.Utils;

/**
 * The {@link AltKerberosAuthenticationHandler} behaves exactly the same way as
 * the {@link KerberosAuthenticationHandler}, except that it allows for an
 * alternative form of authentication for browsers while still using Kerberos
 * for Java access.  This is an abstract class that should be subclassed
 * to allow a developer to implement their own custom authentication for browser
 * access.  The alternateAuthenticate method will be called whenever a request
 * comes from a browser.
 */
public abstract class AltKerberosAuthenticationHandler
        extends KerberosAuthenticationHandler {

    /**
     * Constant that identifies the authentication mechanism.
     */
    public static final String TYPE = "alt-kerberos";

    /**
     * Constant for the configuration property that indicates which user agents
     * are not considered browsers (comma separated)
     */
    public static final String NON_BROWSER_USER_AGENTS =
            TYPE + ".non-browser.user-agents";
    private static final String NON_BROWSER_USER_AGENTS_DEFAULT =
            "java,curl,wget,perl";

    private String[] nonBrowserUserAgents;

    /**
     * Returns the authentication type of the authentication handler,
     * 'alt-kerberos'.
     *
     * @return the authentication type of the authentication handler,
     * 'alt-kerberos'.
     */
    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties config) throws ServletException {
        super.init(config);
        nonBrowserUserAgents = Utils.getNonBrowserUserAgents(config.getProperty(
                NON_BROWSER_USER_AGENTS, NON_BROWSER_USER_AGENTS_DEFAULT));
    }

    /**
     * It enforces the Kerberos SPNEGO authentication sequence returning an
     * {@link AuthenticationToken} only after the Kerberos SPNEGO sequence has
     * completed successfully (in the case of Java access) and only after the
     * custom authentication implemented by the subclass in alternateAuthenticate
     * has completed successfully (in the case of browser access).
     *
     * @param request the HTTP client request.
     * @param response the HTTP client response.
     *
     * @return an authentication token if the request is authorized or null
     *
     * @throws IOException thrown if an IO error occurred
     * @throws AuthenticationException thrown if an authentication error occurred
     */
    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
                                            HttpServletResponse response)
            throws IOException, AuthenticationException {
        AuthenticationToken token;
        if (Utils.isBrowser(nonBrowserUserAgents, request)) {
            token = alternateAuthenticate(request, response);
        } else {
            token = super.authenticate(request, response);
        }
        return token;
    }

    /**
     * Subclasses should implement this method to provide the custom
     * authentication to be used for browsers.
     *
     * @param request the HTTP client request.
     * @param response the HTTP client response.
     * @return an authentication token if the request is authorized, or null
     * @throws IOException thrown if an IO error occurs
     * @throws AuthenticationException thrown if an authentication error occurs
     */
    public abstract AuthenticationToken alternateAuthenticate(
            HttpServletRequest request, HttpServletResponse response)
            throws IOException, AuthenticationException;
}
