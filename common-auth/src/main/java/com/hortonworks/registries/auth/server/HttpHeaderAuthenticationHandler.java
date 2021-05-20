/**
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

import com.hortonworks.registries.auth.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The <code>HttpHeaderAuthenticationHandler</code> provides a http authentication mechanism that restricts
 * the read and write operations to input HTTP header/values received.
 * <p>
 * This allows to restrict the registry access from calls coming from external ingresses, where the HOST
 * variable is populated with the ingress FQDN.
 * </p>
 * <p>
 * In absence of a specific readonly section, allows read-only anonymous access from any
 * network source.
 * </p>
 * <p>
 * The resources allowed for each of the two roles (readonly, readwrite) are a Java regex and
 * have the form: METHOD resource.
 * </p>
 * Configuration properties:
 * <ul>
 * <li>readonly.header.name: <code>HTTP header</code></li>
 * <li>readonly.header.value: <code>header values</code></li>
 * <li>readonly.resources: <code>Comma-separated of HTTP method and resources</code></CODE></li>
 * <li>readwrite.header.name: <code>HTTP header</code></li>
 * <li>readwrite.header.value: <code>header values</code></li>
 * <li>readwrite.resources: <code>Comma-separated of HTTP method and resources</code></CODE></li>
 * </ul>
 * Example:
 * <pre>
 *   readonly.header.name: "Host"
 *   readonly.header.value: "registry.external.mycompany.com"
 *   readonly.resources: "GET .*, POST /api/v1/bar"
 *
 *   readwrite.header.name: "Host"
 *   readwrite.header.value: "registry.internal"
 *   readwrite.resources: "(GET|PUT|POST|DELETE) .*"
 * </pre>
 */
public class HttpHeaderAuthenticationHandler implements AuthenticationHandler {
    private static final Logger logger = LoggerFactory.getLogger(HttpHeaderAuthenticationHandler.class);

    public static final String TYPE = "http";

    /**
     * Constants that identifies the access level.
     */
    public static final String READ_ONLY = "readonly";
    public static final String READ_WRITE = "readwrite";
    public static final String HEADER_NAME = "header.name";
    public static final String HEADER_VALUES = "header.values";
    public static final String RESOURCES = "resources";

    /**
     * HTTP authentication realm
     */
    public static final String HTTP_AUTH = "Http Header Authentication";

    private Optional<String> readonlyHeader = Optional.empty();
    private Set<String> readonlyValues = Collections.EMPTY_SET;
    private Optional<String> readwriteHeader = Optional.empty();
    private Set<String> readwriteValues = Collections.EMPTY_SET;

    private Collection<Pattern> readonlyResources = Collections.emptyList();
    private Collection<Pattern> readwriteResources = Collections.emptyList();

    private String type;

    /**
     * Creates an HTTP authentication handler of type <code>http</code>.
     */
    public HttpHeaderAuthenticationHandler() {
        this(TYPE);
    }

    /**
     * Creates a http authentication handler with a custom auth-token
     * type.
     *
     * @param type auth-token type.
     */
    public HttpHeaderAuthenticationHandler(String type) {
        this.type = type;
    }

    /**
     * Initializes the authentication handler instance.
     * <p>
     * This method is invoked by the {@link AuthenticationFilter#init} method.
     *
     * @param config configuration properties to initialize the handler.
     * @throws ServletException thrown if the handler could not be initialized.
     */
    @Override
    public void init(Properties config) throws ServletException {
        readonlyHeader = getOptionalProperty(config, READ_ONLY, HEADER_NAME);
        readonlyValues = getPropertyValues(config, READ_ONLY, HEADER_VALUES);
        readwriteHeader = getOptionalProperty(config, READ_WRITE, HEADER_NAME);
        readwriteValues = getPropertyValues(config, READ_WRITE, HEADER_VALUES);
        readonlyResources = getPropertyPatternList(config, READ_ONLY, RESOURCES);
        readwriteResources = getPropertyPatternList(config, READ_WRITE, RESOURCES);
    }

    private Set<String> getPropertyValues(Properties config, String object, String property) {
        return Optional.ofNullable(config.getProperty(object + "." + property))
                .map(v -> (Set<String>) new HashSet(Arrays.asList(v.split(","))))
                .orElse(Collections.EMPTY_SET);
    }

    private Collection<Pattern> getPropertyPatternList(Properties config, String object, String property) {
        return getPropertyValues(config, object, property)
                .stream()
                .map(String::trim)
                .map(Pattern::compile)
                .collect(Collectors.toList());
    }

    private Optional<String> getOptionalProperty(Properties config, String object, String property) {
        return Optional.ofNullable(config.getProperty(object + "." + property));
    }

    /**
     * Returns if the handler is configured to support anonymous users.
     *
     * @return if the handler is configured to support anonymous users.
     */
    protected boolean getAcceptAnonymous() {
        return !readonlyHeader.isPresent();
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
     * Returns the authentication type of the authentication handler, 'http'.
     *
     * @return the authentication type of the authentication handler, 'http'.
     */
    @Override
    public String getType() {
        return type;
    }

    /**
     * This is an empty implementation, it always returns <code>TRUE</code>.
     *
     * @param token    the authentication token if any, otherwise <code>NULL</code>.
     * @param request  the HTTP client request.
     * @param response the HTTP client response.
     * @return <code>TRUE</code>
     * @throws IOException             it is never thrown.
     * @throws AuthenticationException it is never thrown.
     */
    @Override
    public boolean managementOperation(AuthenticationToken token,
                                       HttpServletRequest request,
                                       HttpServletResponse response)
            throws IOException, AuthenticationException {
        return true;
    }

    private Optional<String> authUser(HttpServletRequest request) {
        return Optional.ofNullable(
                authUser(request, READ_WRITE, readwriteHeader, readwriteValues).orElseGet(() ->
                        authUser(request, READ_ONLY, readonlyHeader, readonlyValues).orElseGet(() -> null)));
    }

    private Optional<String> authUser(HttpServletRequest request, String user, Optional<String> header, Set<String> headerValues) {
        return header.flatMap((String h) ->
                Optional.ofNullable(request.getHeader(h))
                        .filter(headerValues::contains)
                        .map((String v) -> {
                            logger.info("Authenticating request {} as {} user for {}:{}", request.getRequestURI(), user, h, v);
                            return v;
                        })
                        .map(u -> user));
    }

    /**
     * Authenticates an HTTP client request.
     * <p>
     * It extracts the HTTP header parameter from the request and creates
     * an {@link AuthenticationToken} with it.
     * <p>
     * If the HTTP client request does not contain the HTTP authentication header and
     * the handler is configured to allow anonymous users it returns the {@link AuthenticationToken#ANONYMOUS}
     * token.
     * <p>
     * If the HTTP client request does not contain the HTTP authentication header and
     * the handler is configured to disallow anonymous users it throws an {@link AuthenticationException}.
     *
     * @param request  the HTTP client request.
     * @param response the HTTP client response.
     * @return an authentication token if the HTTP client request is accepted and credentials are valid.
     * @throws IOException thrown if an IO error occurred.
     */
    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws
            IOException {
        Optional<String> username = authUser(request);
        logger.info("Authorizing request {} as {}", request.getRequestURI(), username.orElse("<anonymous>"));
        Optional<AuthenticationToken> token = username.map(u -> new AuthenticationToken(u, u, getType()));

        if (!token.isPresent() && !getAcceptAnonymous()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setHeader(WWW_AUTHENTICATE, HTTP_AUTH);
            return null;
        }

        if (!isAuthorized(username, request)) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }

        return token.orElse(AuthenticationToken.ANONYMOUS);
    }

    private boolean isAuthorized(Optional<String> username, HttpServletRequest request) {
        String resource = String.format("%s %s", request.getMethod(), request.getPathInfo());

        if (username.filter(user -> user.equals(READ_WRITE)).isPresent()) {
            return anyMatchOf(readwriteResources, resource);
        }

        if (username.filter(user -> user.equals(READ_ONLY)).isPresent()) {
            return anyMatchOf(readonlyResources, resource);
        }

        return getAcceptAnonymous();
    }

    private boolean anyMatchOf(Collection<Pattern> resourcePatterns, String resource) {
        return resourcePatterns.stream().anyMatch(pattern -> pattern.matcher(resource).matches());
    }

    @Override
    public boolean shouldAuthenticate(HttpServletRequest request) {
        return true;
    }

}
