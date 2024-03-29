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

import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.client.PseudoAuthenticator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Properties;

/**
 * The <code>PseudoAuthenticationHandler</code> provides a pseudo authentication mechanism that accepts
 * the user name specified as a query string parameter.
 * <p>
 * This mimics the model of Hadoop Simple authentication which trust the 'user.name' property provided in
 * the configuration object.
 * <p>
 * This handler can be configured to support anonymous users.
 * <p>
 * The only supported configuration property is:
 * <ul>
 * <li>simple.anonymous.allowed: <code>true|false</code>, default value is <code>false</code></li>
 * </ul>
 */
public class PseudoAuthenticationHandler implements AuthenticationHandler {

    /**
     * Constant that identifies the authentication mechanism.
     */
    public static final String TYPE = "simple";

    /**
     * Constant for the configuration property that indicates if anonymous users are allowed.
     */
    public static final String ANONYMOUS_ALLOWED = TYPE + ".anonymous.allowed";

    private static final String PSEUDO_AUTH = "PseudoAuth";

    private boolean acceptAnonymous;
    private final String type;

    /**
     * Creates a pseudo authentication handler with the default auth-token
     * type, <code>simple</code>.
     */
    public PseudoAuthenticationHandler() {
        this(TYPE);
    }

    /**
     * Creates a pseudo authentication handler with a custom auth-token
     * type.
     *
     * @param type auth-token type.
     */
    public PseudoAuthenticationHandler(String type) {
        this.type = type;
    }

    /**
     * Initializes the authentication handler instance.
     * <p>
     * This method is invoked by the {@link AuthenticationFilter#init} method.
     *
     * @param config configuration properties to initialize the handler.
     *
     * @throws ServletException thrown if the handler could not be initialized.
     */
    @Override
    public void init(Properties config) throws ServletException {
        acceptAnonymous = Boolean.parseBoolean(config.getProperty(ANONYMOUS_ALLOWED, "false"));
    }

    /**
     * Returns if the handler is configured to support anonymous users.
     *
     * @return if the handler is configured to support anonymous users.
     */
    protected boolean getAcceptAnonymous() {
        return acceptAnonymous;
    }

    /**
     * Releases any resources initialized by the authentication handler.
     * <p>
     * This implementation does a NOP.
     */
    @Override
    public void destroy() {
    }

    /**
     * Returns the authentication type of the authentication handler, 'simple'.
     *
     * @return the authentication type of the authentication handler, 'simple'.
     */
    @Override
    public String getType() {
        return type;
    }

    private String getUserName(HttpServletRequest request) {
        if (request.getQueryString() != null) {
            final String[] pairs = request.getQueryString().split("&");
            for (String pair : pairs) {
                final int idx = pair.indexOf("=");
                final String key;
                try {
                    key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
                    final String value = idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
                    if (PseudoAuthenticator.USER_NAME.equals(key)) {
                        return value;
                    }
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return null;
    }

    /**
     * Authenticates an HTTP client request.
     * <p>
     * It extracts the {@link PseudoAuthenticator#USER_NAME} parameter from the query string and creates
     * an {@link AuthenticationToken} with it.
     * <p>
     * If the HTTP client request does not contain the {@link PseudoAuthenticator#USER_NAME} parameter and
     * the handler is configured to allow anonymous users it returns the {@link AuthenticationToken#ANONYMOUS}
     * token.
     * <p>
     * If the HTTP client request does not contain the {@link PseudoAuthenticator#USER_NAME} parameter and
     * the handler is configured to disallow anonymous users it throws an {@link AuthenticationException}.
     *
     * @param request the HTTP client request.
     * @param response the HTTP client response.
     *
     * @return an authentication token if the HTTP client request is accepted and credentials are valid.
     *
     * @throws IOException thrown if an IO error occurred.
     * @throws AuthenticationException thrown if HTTP client request was not accepted as an authentication request.
     */
    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
            throws IOException, AuthenticationException {
        AuthenticationToken token;
        String userName = getUserName(request);
        if (userName == null) {
            if (getAcceptAnonymous()) {
                token = AuthenticationToken.ANONYMOUS;
            } else {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.setHeader(WWW_AUTHENTICATE, PSEUDO_AUTH);
                token = null;
            }
        } else {
            token = new AuthenticationToken(userName, userName, getType());
        }
        return token;
    }

    @Override
    public boolean shouldAuthenticate(HttpServletRequest request) {
        return true;
    }

}
