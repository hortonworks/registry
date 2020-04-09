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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import static com.hortonworks.registries.auth.server.HttpHeaderAuthenticationHandler.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestHttpHeaderAuthenticationHandler {
    private static final String AUTHORIZED_RESOURCE_PATH = "/resource";
    private static final String UNAUTHORIZED_RESOURCE_PATH = "/private/resource";
    private static final String AUTH_HEADER = "User";
    private static final String AUTH_FOO_USER = "foo";
    private static final String AUTH_BAR_USER = "bar";

    HttpHeaderAuthenticationHandler handler;
    Properties props;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Before
    public void setup() throws Exception, NoSuchAlgorithmException {
        handler = new HttpHeaderAuthenticationHandler();
        props = new Properties();
    }

    @After
    public void teardown() throws Exception {
        handler.destroy();
    }

    @Test
    public void testAllowAnonymousAccess() throws Exception {
        handler.init(props);

        AuthenticationToken token = handler.authenticate(request,
                response);
        assertEquals(AuthenticationToken.ANONYMOUS, token);
    }

    @Test
    public void testRestrictAccessToSingleReadOnlyUserRole() throws Exception {
        testRestrictAccessToRole(READ_ONLY, AUTH_FOO_USER, AUTH_FOO_USER);
    }

    @Test
    public void testRestrictAccessToMultipleReadOnlyUserRole() throws Exception {
        testRestrictAccessToRole(READ_ONLY, AUTH_FOO_USER, AUTH_FOO_USER + "," + AUTH_BAR_USER);
        testRestrictAccessToRole(READ_ONLY, AUTH_BAR_USER, AUTH_FOO_USER + "," + AUTH_BAR_USER);
    }

    @Test
    public void testRestrictAccessToSingleReadWriteRole() throws Exception {
        testRestrictAccessToRole(READ_WRITE, AUTH_FOO_USER, AUTH_FOO_USER);
    }

    @Test
    public void testRestrictAccessToMultipleReadWriteRole() throws Exception {
        testRestrictAccessToRole(READ_WRITE, AUTH_FOO_USER, AUTH_FOO_USER + "," + AUTH_BAR_USER);
        testRestrictAccessToRole(READ_WRITE, AUTH_BAR_USER, AUTH_FOO_USER + "," + AUTH_BAR_USER);
    }

    private void testRestrictAccessToRole(String userRole, String userName, String allowedUserRoles) throws Exception {
        props.setProperty(userRole + "." + HEADER_NAME, AUTH_HEADER);
        props.setProperty(userRole + "." + HEADER_VALUES, allowedUserRoles);
        props.setProperty(userRole + "." + RESOURCES, "GET " + AUTHORIZED_RESOURCE_PATH);
        handler.init(props);

        when(request.getHeader(AUTH_HEADER)).thenReturn(userName);
        when(request.getMethod()).thenReturn("GET");
        when(request.getPathInfo()).thenReturn(AUTHORIZED_RESOURCE_PATH);

        AuthenticationToken token = handler.authenticate(request,
                response);
        assertNotNull(token);
        assertEquals(userRole, token.getUserName());
    }

    @Test
    public void testDenyAccessToReadOnlyUserForUnauthorizedHttpMethod() throws Exception {
        testDenyAccessToRoleForUnauthorizedHttpMethod(READ_ONLY);
    }

    @Test
    public void testDenyAccessToReadWriteUserForUnauthorizedHttpMethod() throws Exception {
        testDenyAccessToRoleForUnauthorizedHttpMethod(READ_WRITE);
    }

    private void testDenyAccessToRoleForUnauthorizedHttpMethod(String userRole) throws Exception {
        props.setProperty(userRole + "." + HEADER_NAME, AUTH_HEADER);
        props.setProperty(userRole + "." + HEADER_VALUES, AUTH_FOO_USER);
        props.setProperty(userRole + "." + RESOURCES, "GET " + AUTHORIZED_RESOURCE_PATH);
        handler.init(props);

        when(request.getHeader(AUTH_HEADER)).thenReturn(AUTH_FOO_USER);
        when(request.getMethod()).thenReturn("PUT");
        when(request.getPathInfo()).thenReturn(AUTHORIZED_RESOURCE_PATH);

        AuthenticationToken token = handler.authenticate(request,
                response);
        verify(response).sendError(HttpServletResponse.SC_FORBIDDEN);
        assertNull(token);
    }

    @Test
    public void testDenyAccessToReadOnlyUserForUnauthorizedResource() throws Exception {
        testDenyAccessToRoleForUnauthorizedResource(READ_ONLY);
    }

    @Test
    public void testDenyAccessToReadWriteUserForUnauthorizedResource() throws Exception {
        testDenyAccessToRoleForUnauthorizedResource(READ_WRITE);
    }

    private void testDenyAccessToRoleForUnauthorizedResource(String userRole) throws Exception {
        props.setProperty(userRole + "." + HEADER_NAME, AUTH_HEADER);
        props.setProperty(userRole + "." + HEADER_VALUES, AUTH_FOO_USER);
        props.setProperty(userRole + "." + RESOURCES, "GET " + AUTHORIZED_RESOURCE_PATH);
        handler.init(props);

        when(request.getHeader(AUTH_HEADER)).thenReturn(AUTH_FOO_USER);
        when(request.getMethod()).thenReturn("GET");
        when(request.getPathInfo()).thenReturn(UNAUTHORIZED_RESOURCE_PATH);

        AuthenticationToken token = handler.authenticate(request,
                response);
        verify(response).sendError(HttpServletResponse.SC_FORBIDDEN);
        assertNull(token);
    }

    @Test
    public void testRequestAuthenticationWhenConfiguredReadOnlyUser() throws Exception {
        props.setProperty(READ_ONLY + "." + HEADER_NAME, AUTH_HEADER);
        props.setProperty(READ_ONLY + "." + HEADER_VALUES, AUTH_FOO_USER);
        props.setProperty(READ_ONLY + "." + RESOURCES, "GET " + AUTHORIZED_RESOURCE_PATH);
        handler.init(props);

        AuthenticationToken token = handler.authenticate(request,
                response);

        assertNull(token);
        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        verify(response).setHeader(WWW_AUTHENTICATE, HTTP_AUTH);
    }
}
