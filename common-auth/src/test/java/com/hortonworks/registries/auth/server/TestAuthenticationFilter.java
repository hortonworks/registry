/**
 * Copyright 2016-2023 Cloudera, Inc.
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

import com.hortonworks.registries.auth.client.AuthenticatedURL;
import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.util.Signer;
import com.hortonworks.registries.auth.util.SignerSecretProvider;
import com.hortonworks.registries.auth.util.StringSignerSecretProviderCreator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpCookie;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import static com.hortonworks.registries.auth.server.AuthenticationFilter.ENABLE_TRUSTED_PROXY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class TestAuthenticationFilter {

    private static final long TOKEN_VALIDITY_SEC = 1000;

    @Test
    public void testGetConfiguration() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        FilterConfig config = mock(FilterConfig.class);
        when(config.getInitParameter(AuthenticationFilter.CONFIG_PREFIX)).thenReturn("");
        when(config.getInitParameter("a")).thenReturn("A");
        when(config.getInitParameterNames()).thenReturn(new Vector<String>(Arrays.asList("a")).elements());
        Properties props = filter.getConfiguration("", config);
        assertEquals("A", props.getProperty("a"));

        config = mock(FilterConfig.class);
        when(config.getInitParameter(AuthenticationFilter.CONFIG_PREFIX)).thenReturn("foo");
        when(config.getInitParameter("foo.a")).thenReturn("A");
        when(config.getInitParameterNames()).thenReturn(new Vector<String>(Arrays.asList("foo.a")).elements());
        props = filter.getConfiguration("foo.", config);
        assertEquals("A", props.getProperty("a"));
    }

    @Test
    public void testInitEmpty() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameterNames()).thenReturn(new Vector<String>().elements());
            filter.init(config);
            fail();
        } catch (ServletException ex) {
            // Expected
            assertEquals("Authentication type must be specified: simple|kerberos|<class>",
                    ex.getMessage());
        } catch (Exception ex) {
            fail();
        } finally {
            filter.destroy();
        }
    }

    public static class DummyAuthenticationHandler implements AuthenticationHandler {
        public static boolean init;
        public static boolean managementOperationReturn;
        public static boolean destroy;
        public static boolean expired;

        public static final String TYPE = "dummy";

        public static void reset() {
            init = false;
            destroy = false;
        }

        @Override
        public void init(Properties config) throws ServletException {
            init = true;
            managementOperationReturn =
                    config.getProperty("management.operation.return", "true").equals("true");
            expired = config.getProperty("expired.token", "false").equals("true");
        }

        @Override
        public void destroy() {
            destroy = true;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
                throws IOException, AuthenticationException {
            AuthenticationToken token = null;
            String param = request.getParameter("authenticated");
            if (param != null && param.equals("true")) {
                token = new AuthenticationToken("u", "p", "t");
                token.setExpires((expired) ? 0 : System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
            } else {
                if (request.getHeader("WWW-Authenticate") == null) {
                    response.setHeader("WWW-Authenticate", "dummyauth");
                } else {
                    throw new AuthenticationException("AUTH FAILED");
                }
            }
            return token;
        }

        @Override
        public boolean shouldAuthenticate(HttpServletRequest request) {
            return true;
        }
    }

    @Test
    public void testFallbackToRandomSecretProvider() throws Exception {
        // minimal configuration & simple auth handler (Pseudo)
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
            when(config.getInitParameter(
                    AuthenticationFilter.AUTH_TOKEN_VALIDITY)).thenReturn(
                    (new Long(TOKEN_VALIDITY_SEC)).toString());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                            AuthenticationFilter.AUTH_TOKEN_VALIDITY)).elements());
            ServletContext context = mock(ServletContext.class);
            when(context.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
                    .thenReturn(null);
            when(config.getServletContext()).thenReturn(context);
            filter.init(config);
            assertEquals(PseudoAuthenticationHandler.class, filter.getAuthenticationHandler().getClass());
            assertTrue(filter.isRandomSecret());
            assertFalse(filter.isCustomSignerSecretProvider());
            assertNull(filter.getCookieDomain());
            assertNull(filter.getCookiePath());
            assertEquals(TOKEN_VALIDITY_SEC, filter.getValidity());
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testInit() throws Exception {
        // custom secret as inline
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<>(Arrays.asList(AuthenticationFilter.AUTH_TYPE))
                            .elements());
            ServletContext context = mock(ServletContext.class);
            when(context.getAttribute(
                    AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE)).thenReturn(
                    new SignerSecretProvider() {
                        @Override
                        public void init(Properties config, ServletContext servletContext,
                                         long tokenValidity) {
                        }

                        @Override
                        public byte[] getCurrentSecret() {
                            return null;
                        }

                        @Override
                        public byte[][] getAllSecrets() {
                            return null;
                        }
                    });
            when(config.getServletContext()).thenReturn(context);
            filter.init(config);
            assertFalse(filter.isRandomSecret());
            assertTrue(filter.isCustomSignerSecretProvider());
        } finally {
            filter.destroy();
        }

        // custom secret by file
        File testDir = new File(System.getProperty("test.build.data",
                "target/test-dir"));
        testDir.mkdirs();
        String secretValue = "hadoop";
        File secretFile = new File(testDir, "http-secret.txt");
        Writer writer = new FileWriter(secretFile);
        writer.write(secretValue);
        writer.close();

        filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter(
                    AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
            when(config.getInitParameter(
                    AuthenticationFilter.SIGNATURE_SECRET_FILE))
                    .thenReturn(secretFile.getAbsolutePath());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                            AuthenticationFilter.SIGNATURE_SECRET_FILE)).elements());
            ServletContext context = mock(ServletContext.class);
            when(context.getAttribute(
                    AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
                    .thenReturn(null);
            when(config.getServletContext()).thenReturn(context);
            filter.init(config);
            assertFalse(filter.isRandomSecret());
            assertFalse(filter.isCustomSignerSecretProvider());
        } finally {
            filter.destroy();
        }

        // custom cookie domain and cookie path
        filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("simple");
            when(config.getInitParameter(AuthenticationFilter.COOKIE_DOMAIN)).thenReturn(".foo.com");
            when(config.getInitParameter(AuthenticationFilter.COOKIE_PATH)).thenReturn("/bar");
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                            AuthenticationFilter.COOKIE_DOMAIN,
                            AuthenticationFilter.COOKIE_PATH)).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);
            assertEquals(".foo.com", filter.getCookieDomain());
            assertEquals("/bar", filter.getCookiePath());
        } finally {
            filter.destroy();
        }

        // authentication handler lifecycle, and custom impl
        DummyAuthenticationHandler.reset();
        filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);
            assertTrue(DummyAuthenticationHandler.init);
        } finally {
            filter.destroy();
            assertTrue(DummyAuthenticationHandler.destroy);
        }

        // kerberos auth handler
        filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            ServletContext sc = mock(ServletContext.class);
            when(config.getServletContext()).thenReturn(sc);
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("kerberos");
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE)).elements());
            filter.init(config);
        } catch (ServletException ex) {
            // Expected
        } finally {
            assertEquals(KerberosAuthenticationHandler.class, filter.getAuthenticationHandler().getClass());
            filter.destroy();
        }
    }

    @Test
    public void testInitCaseSensitivity() throws Exception {
        // minimal configuration & simple auth handler (Pseudo)
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn("SimPle");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TOKEN_VALIDITY)).thenReturn(
                    (new Long(TOKEN_VALIDITY_SEC)).toString());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                            AuthenticationFilter.AUTH_TOKEN_VALIDITY)).elements());
            getMockedServletContextWithStringSigner(config);

            filter.init(config);
            assertEquals(PseudoAuthenticationHandler.class,
                    filter.getAuthenticationHandler().getClass());
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testGetRequestURL() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
            when(request.getRequestURI()).thenReturn("/bar");
            when(request.getQueryString()).thenReturn("a=A&b=B");

            assertEquals("http://foo:8080/bar?a=A&b=B", filter.getRequestURL(request));
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testGetToken() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();

        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn("secret");
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    AuthenticationFilter.SIGNATURE_SECRET,
                                    "management.operation.return")).elements());
            SignerSecretProvider secretProvider =
                    getMockedServletContextWithStringSigner(config);
            filter.init(config);

            AuthenticationToken token = new AuthenticationToken("u", "p", DummyAuthenticationHandler.TYPE);
            token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);

            Signer signer = new Signer(secretProvider);
            String tokenSigned = signer.sign(token.toString());

            Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getCookies()).thenReturn(new Cookie[]{cookie});
            when(request.getScheme()).thenReturn("https");

            AuthenticationToken newToken = filter.getToken(request);

            assertEquals(token.toString(), newToken.toString());
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testGetTokenExpired() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn("secret");
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    AuthenticationFilter.SIGNATURE_SECRET,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            AuthenticationToken token =
                    new AuthenticationToken("u", "p", DummyAuthenticationHandler.TYPE);
            token.setExpires(System.currentTimeMillis() - TOKEN_VALIDITY_SEC);
            SignerSecretProvider secretProvider =
                    StringSignerSecretProviderCreator.newStringSignerSecretProvider();
            Properties secretProviderProps = new Properties();
            secretProviderProps.setProperty(
                    AuthenticationFilter.SIGNATURE_SECRET, "secret");
            secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
            Signer signer = new Signer(secretProvider);
            String tokenSigned = signer.sign(token.toString());

            Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getCookies()).thenReturn(new Cookie[]{cookie});
            when(request.getScheme()).thenReturn("https");

            boolean failed = false;
            try {
                filter.getToken(request);
            } catch (AuthenticationException ex) {
                assertEquals("AuthenticationToken expired", ex.getMessage());
                failed = true;
            } finally {
                assertTrue(failed, "token not expired");
            }
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testGetTokenInvalidType() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn("secret");
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    AuthenticationFilter.SIGNATURE_SECRET,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            AuthenticationToken token = new AuthenticationToken("u", "p", "invalidtype");
            token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
            SignerSecretProvider secretProvider =
                    StringSignerSecretProviderCreator.newStringSignerSecretProvider();
            Properties secretProviderProps = new Properties();
            secretProviderProps.setProperty(
                    AuthenticationFilter.SIGNATURE_SECRET, "secret");
            secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
            Signer signer = new Signer(secretProvider);
            String tokenSigned = signer.sign(token.toString());

            Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getCookies()).thenReturn(new Cookie[]{cookie});
            when(request.getScheme()).thenReturn("https");

            boolean failed = false;
            try {
                filter.getToken(request);
            } catch (AuthenticationException ex) {
                assertEquals("Invalid AuthenticationToken type", ex.getMessage());
                failed = true;
            } finally {
                assertTrue(failed, "token not invalid type");
            }
        } finally {
            filter.destroy();
        }
    }

    private static SignerSecretProvider getMockedServletContextWithStringSigner(
            FilterConfig config) throws Exception {
        Properties secretProviderProps = new Properties();
        secretProviderProps.setProperty(AuthenticationFilter.SIGNATURE_SECRET,
                "secret");
        SignerSecretProvider secretProvider =
                StringSignerSecretProviderCreator.newStringSignerSecretProvider();
        secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);

        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(
                AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
                .thenReturn(secretProvider);
        when(config.getServletContext()).thenReturn(context);
        return secretProvider;
    }

    @Test
    public void testDoFilterNotAuthenticated() throws Exception {
        doFilterNotAuthenticated(Collections.emptyMap(), true);
    }

    @Test
    public void testDoFilterNotAuthenticatedExpect100() throws Exception {
        doFilterNotAuthenticated(Collections.singletonMap("Expect", "100-continue"), false);
    }

    public void doFilterNotAuthenticated(Map<String, String> extraHeaders, boolean expectDrainInputStream) throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
            when(request.getRequestURI()).thenReturn("/bar");
            // Simulate unknown content length to trigger draining the request input stream
            when(request.getContentLength()).thenReturn(-1);
            ServletInputStream requestInputStream = new TestServletInputStream("Hello World!");
            when(request.getInputStream()).thenReturn(requestInputStream);
            when(request.getHeader(anyString())).thenAnswer(
                invocation -> extraHeaders.get((String) invocation.getArguments()[0]));

            HttpServletResponse response = mock(HttpServletResponse.class);

            FilterChain chain = mock(FilterChain.class);

            Mockito.doAnswer(
                    new Answer<Object>() {
                        @Override
                        public Object answer(InvocationOnMock invocation) throws Throwable {
                            fail();
                            return null;
                        }
                    }
            ).when(chain).doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

            when(response.containsHeader("WWW-Authenticate")).thenReturn(true);
            filter.doFilter(request, response, chain);

            Mockito.verify(response).sendError(
                    HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");

            // Since the input stream is backed by a byte array it is safe to assume that the stream is empty when
            // avaliable() return 0
            if (expectDrainInputStream) {
                assertEquals(0, requestInputStream.available(), "Request input stream is expected to be drained, but is't.");
            } else {
                assertTrue(requestInputStream.available() > 0, "Request input stream expected not to be drained, but is.");
            }
        } finally {
            filter.destroy();
        }
    }

    private void testDoFilterAuthentication(boolean withDomainPath,
                                            boolean invalidToken,
                                            boolean expired) throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        FilterConfig config = mock(FilterConfig.class);
        when(config.getInitParameter("management.operation.return")).
                thenReturn("true");
        when(config.getInitParameter("expired.token")).
                thenReturn(Boolean.toString(expired));
        when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE))
                .thenReturn(DummyAuthenticationHandler.class.getName());
        when(config.getInitParameter(AuthenticationFilter
                .AUTH_TOKEN_VALIDITY)).thenReturn(new Long(TOKEN_VALIDITY_SEC).toString());
        when(config.getInitParameter(AuthenticationFilter
                .SIGNATURE_SECRET)).thenReturn("secret");
        when(config.getInitParameterNames()).thenReturn(new
                Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                AuthenticationFilter.AUTH_TOKEN_VALIDITY,
                AuthenticationFilter.SIGNATURE_SECRET, "management.operation" +
                        ".return", "expired.token")).elements());
        getMockedServletContextWithStringSigner(config);

        if (withDomainPath) {
            when(config.getInitParameter(AuthenticationFilter
                    .COOKIE_DOMAIN)).thenReturn(".foo.com");
            when(config.getInitParameter(AuthenticationFilter.COOKIE_PATH))
                    .thenReturn("/bar");
            when(config.getInitParameterNames()).thenReturn(new
                    Vector<String>(Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                    AuthenticationFilter.AUTH_TOKEN_VALIDITY,
                    AuthenticationFilter.SIGNATURE_SECRET,
                    AuthenticationFilter.COOKIE_DOMAIN, AuthenticationFilter
                            .COOKIE_PATH, "management.operation.return")).elements());
        }

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("authenticated")).thenReturn("true");
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
        when(request.getRequestURI()).thenReturn("/bar");
        when(request.getQueryString()).thenReturn("authenticated=true");
        when(request.getScheme()).thenReturn("https");

        if (invalidToken) {
            when(request.getCookies()).thenReturn(new Cookie[]{new Cookie(AuthenticatedURL.AUTH_COOKIE, "foo")});
        }

        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        final HashMap<String, String> cookieMap = new HashMap<String, String>();
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                String cookieHeader = (String) invocation.getArguments()[1];
                parseCookieMap(cookieHeader, cookieMap);
                return null;
            }
        }).when(response).addHeader(Mockito.eq("Set-Cookie"), anyString());

        try {
            filter.init(config);
            filter.doFilter(request, response, chain);

            if (expired) {
                Mockito.verify(response, Mockito.never()).
                        addHeader(Mockito.eq("Set-Cookie"), anyString());
            } else {
                String v = cookieMap.get(AuthenticatedURL.AUTH_COOKIE);
                assertNotNull(v, "cookie missing");
                assertTrue(v.contains("u=") && v.contains("p=") && v.contains("t=") && v.contains("e=") && v.contains("s="));
                Mockito.verify(chain).doFilter(Mockito.any(ServletRequest.class),
                        Mockito.any(ServletResponse.class));

                SignerSecretProvider secretProvider =
                        StringSignerSecretProviderCreator.newStringSignerSecretProvider();
                Properties secretProviderProps = new Properties();
                secretProviderProps.setProperty(
                        AuthenticationFilter.SIGNATURE_SECRET, "secret");
                secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
                Signer signer = new Signer(secretProvider);
                String value = signer.verifyAndExtract(v);
                AuthenticationToken token = AuthenticationToken.parse(value);
                assertNotEquals(token.getExpires(), 0L);

                if (withDomainPath) {
                    assertEquals(".foo.com", cookieMap.get("Domain"));
                    assertEquals("/bar", cookieMap.get("Path"));
                } else {
                    assertFalse(cookieMap.containsKey("Domain"));
                    assertFalse(cookieMap.containsKey("Path"));
                }
            }
        } finally {
            filter.destroy();
        }
    }

    private static void parseCookieMap(String cookieHeader, HashMap<String,
            String> cookieMap) {
        List<HttpCookie> cookies = HttpCookie.parse(cookieHeader);
        for (HttpCookie cookie : cookies) {
            if (AuthenticatedURL.AUTH_COOKIE.equals(cookie.getName())) {
                cookieMap.put(cookie.getName(), cookie.getValue());
                if (cookie.getPath() != null) {
                    cookieMap.put("Path", cookie.getPath());
                }
                if (cookie.getDomain() != null) {
                    cookieMap.put("Domain", cookie.getDomain());
                }
            }
        }
    }

    @Test
    public void testDoFilterAuthentication() throws Exception {
        testDoFilterAuthentication(false, false, false);
    }

    @Test
    public void testDoFilterAuthenticationImmediateExpiration() throws Exception {
        testDoFilterAuthentication(false, false, true);
    }

    @Test
    public void testDoFilterAuthenticationWithInvalidToken() throws Exception {
        testDoFilterAuthentication(false, true, false);
    }

    @Test
    public void testDoFilterAuthenticationWithDomainPath() throws Exception {
        testDoFilterAuthentication(true, false, false);
    }

    @Test
    public void testDoFilterAuthenticated() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
            when(request.getRequestURI()).thenReturn("/bar");

            AuthenticationToken token = new AuthenticationToken("u", "p", "t");
            token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
            SignerSecretProvider secretProvider =
                    StringSignerSecretProviderCreator.newStringSignerSecretProvider();
            Properties secretProviderProps = new Properties();
            secretProviderProps.setProperty(
                    AuthenticationFilter.SIGNATURE_SECRET, "secret");
            secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
            Signer signer = new Signer(secretProvider);
            String tokenSigned = signer.sign(token.toString());

            Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
            when(request.getCookies()).thenReturn(new Cookie[]{cookie});

            HttpServletResponse response = mock(HttpServletResponse.class);

            FilterChain chain = mock(FilterChain.class);

            Mockito.doAnswer(
                    new Answer<Object>() {
                        @Override
                        public Object answer(InvocationOnMock invocation) throws Throwable {
                            Object[] args = invocation.getArguments();
                            HttpServletRequest request = (HttpServletRequest) args[0];
                            assertEquals("u", request.getRemoteUser());
                            assertEquals("p", request.getUserPrincipal().getName());
                            return null;
                        }
                    }
            ).when(chain).doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

            filter.doFilter(request, response, chain);

        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testDoFilterAuthenticationFailure() throws Exception {
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
            when(request.getRequestURI()).thenReturn("/bar");
            when(request.getCookies()).thenReturn(new Cookie[]{});
            when(request.getHeader("WWW-Authenticate")).thenReturn("dummyauth");
            HttpServletResponse response = mock(HttpServletResponse.class);

            FilterChain chain = mock(FilterChain.class);

            final HashMap<String, String> cookieMap = new HashMap<String, String>();
            Mockito.doAnswer(
                    new Answer<Object>() {
                        @Override
                        public Object answer(InvocationOnMock invocation) throws Throwable {
                            Object[] args = invocation.getArguments();
                            parseCookieMap((String) args[1], cookieMap);
                            return null;
                        }
                    }
            ).when(response).addHeader(Mockito.eq("Set-Cookie"), anyString());

            Mockito.doAnswer(
                    new Answer<Object>() {
                        @Override
                        public Object answer(InvocationOnMock invocation) throws Throwable {
                            fail("shouldn't get here");
                            return null;
                        }
                    }
            ).when(chain).doFilter(Mockito.<ServletRequest>any(), Mockito.<ServletResponse>any());

            filter.doFilter(request, response, chain);

            Mockito.verify(response).sendError(
                    HttpServletResponse.SC_FORBIDDEN, "AUTH FAILED");
            Mockito.verify(response, Mockito.never()).setHeader(Mockito.eq("WWW-Authenticate"), anyString());

            String value = cookieMap.get(AuthenticatedURL.AUTH_COOKIE);
            assertNull(value, "cookie should not be set for not authenticated request");
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testDoFilterAuthenticatedExpired() throws Exception {
        String secret = "secret";
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn(
                    secret);
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    AuthenticationFilter.SIGNATURE_SECRET,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
            when(request.getRequestURI()).thenReturn("/bar");

            AuthenticationToken token = new AuthenticationToken("u", "p", DummyAuthenticationHandler.TYPE);
            token.setExpires(System.currentTimeMillis() - TOKEN_VALIDITY_SEC);
            SignerSecretProvider secretProvider =
                    StringSignerSecretProviderCreator.newStringSignerSecretProvider();
            Properties secretProviderProps = new Properties();
            secretProviderProps.setProperty(
                    AuthenticationFilter.SIGNATURE_SECRET, secret);
            secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
            Signer signer = new Signer(secretProvider);
            String tokenSigned = signer.sign(token.toString());

            Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
            when(request.getCookies()).thenReturn(new Cookie[]{cookie});

            HttpServletResponse response = mock(HttpServletResponse.class);
            when(response.containsHeader("WWW-Authenticate")).thenReturn(true);
            FilterChain chain = mock(FilterChain.class);

            verifyUnauthorized(filter, request, response, chain);
        } finally {
            filter.destroy();
        }
    }

    private static void verifyUnauthorized(AuthenticationFilter filter,
                                           HttpServletRequest request,
                                           HttpServletResponse response,
                                           FilterChain chain) throws
            IOException,
            ServletException {
        final HashMap<String, String> cookieMap = new HashMap<String, String>();
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                String cookieHeader = (String) invocation.getArguments()[1];
                parseCookieMap(cookieHeader, cookieMap);
                return null;
            }
        }).when(response).addHeader(Mockito.eq("Set-Cookie"), anyString());

        filter.doFilter(request, response, chain);

        Mockito.verify(response).sendError(Mockito.eq(HttpServletResponse
                .SC_UNAUTHORIZED), anyString());
        Mockito.verify(chain, Mockito.never()).doFilter(Mockito.any(ServletRequest.class), Mockito.any(ServletResponse.class));

        assertFalse(cookieMap.containsKey(AuthenticatedURL.AUTH_COOKIE), "cookie should not be set for unauthenticated request");
    }

    @Test
    public void testDoFilterAuthenticatedInvalidType() throws Exception {
        String secret = "secret";
        AuthenticationFilter filter = new AuthenticationFilter();
        try {
            FilterConfig config = mock(FilterConfig.class);
            when(config.getInitParameter("management.operation.return")).
                    thenReturn("true");
            when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(
                    DummyAuthenticationHandler.class.getName());
            when(config.getInitParameter(AuthenticationFilter.SIGNATURE_SECRET)).thenReturn(
                    secret);
            when(config.getInitParameterNames()).thenReturn(
                    new Vector<String>(
                            Arrays.asList(AuthenticationFilter.AUTH_TYPE,
                                    AuthenticationFilter.SIGNATURE_SECRET,
                                    "management.operation.return")).elements());
            getMockedServletContextWithStringSigner(config);
            filter.init(config);

            HttpServletRequest request = mock(HttpServletRequest.class);
            when(request.getRequestURL()).thenReturn(new StringBuffer("http://foo:8080/bar"));
            when(request.getRequestURI()).thenReturn("/bar");

            AuthenticationToken token = new AuthenticationToken("u", "p", "invalidtype");
            token.setExpires(System.currentTimeMillis() + TOKEN_VALIDITY_SEC);
            SignerSecretProvider secretProvider =
                    StringSignerSecretProviderCreator.newStringSignerSecretProvider();
            Properties secretProviderProps = new Properties();
            secretProviderProps.setProperty(
                    AuthenticationFilter.SIGNATURE_SECRET, secret);
            secretProvider.init(secretProviderProps, null, TOKEN_VALIDITY_SEC);
            Signer signer = new Signer(secretProvider);
            String tokenSigned = signer.sign(token.toString());

            Cookie cookie = new Cookie(AuthenticatedURL.AUTH_COOKIE, tokenSigned);
            when(request.getCookies()).thenReturn(new Cookie[]{cookie});

            HttpServletResponse response = mock(HttpServletResponse.class);
            when(response.containsHeader("WWW-Authenticate")).thenReturn(true);
            FilterChain chain = mock(FilterChain.class);

            verifyUnauthorized(filter, request, response, chain);
        } finally {
            filter.destroy();
        }
    }

    @Test
    public void testTrustedProxyNilConfig() {
        AuthenticationFilter.ProxyUserAuthorization proxyUserAuthorization = new AuthenticationFilter.ProxyUserAuthorization(new Properties());
        Assertions.assertFalse(proxyUserAuthorization.authorize("knox", "127.0.0.1"));
    }

    @Test
    public void testTrustedProxyConfig() {
        AuthenticationFilter.ProxyUserAuthorization proxyUserAuthorization = new AuthenticationFilter.ProxyUserAuthorization(new Properties() {{
            put("proxyuser.knox.hosts", "127.0.0.1");
            put("proxyuser.admin.hosts", "10.222.0.0,10.113.221.221");
            put("proxyuser.user1.hosts", "*");
        }});
        Assertions.assertTrue(proxyUserAuthorization.authorize("knox", "127.0.0.1"));
        Assertions.assertFalse(proxyUserAuthorization.authorize("knox", "10.222.0.0"));

        Assertions.assertTrue(proxyUserAuthorization.authorize("admin", "10.222.0.0"));
        Assertions.assertTrue(proxyUserAuthorization.authorize("admin", "10.113.221.221"));
        Assertions.assertFalse(proxyUserAuthorization.authorize("admin", "127.0.0.1"));

        Assertions.assertTrue(proxyUserAuthorization.authorize("user1", "10.222.0.0"));
        Assertions.assertTrue(proxyUserAuthorization.authorize("user1", "10.113.221.221"));
        Assertions.assertTrue(proxyUserAuthorization.authorize("user1", "127.0.0.1"));
    }

    @Test
    public void testGetDoAsUser() throws UnsupportedEncodingException {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        Mockito.when(request.getParameter("doAs")).thenReturn("user1");
        Assertions.assertEquals("user1", AuthenticationFilter.getDoasUser(request));

        Mockito.when(request.getParameter("doAs")).thenReturn(null);
        Assertions.assertNull(AuthenticationFilter.getDoasUser(request));

        Mockito.when(request.getParameter("doAs")).thenReturn("");
        Assertions.assertNull(AuthenticationFilter.getDoasUser(request));

        Mockito.when(request.getParameter("doAs")).thenReturn(URLDecoder.decode("%20abc", "UTF-8"));
        Assertions.assertEquals(" abc", AuthenticationFilter.getDoasUser(request));
    }

    @Test
    public void testProxyUserWithDoAsFromWhitelistedHost() throws Exception {
        String authNUser = "client";
        String doAsUser = "user1";

        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
            .thenReturn(null);

        FilterConfig config = mock(FilterConfig.class);
        when(config.getInitParameterNames()).thenReturn(new StringTokenizer(String.join(" ",
            AuthenticationFilter.AUTH_TYPE,
            ENABLE_TRUSTED_PROXY,
            "proxyuser." + authNUser + ".hosts",
            StubAuthenticationHandler.USER_KEY,
            StubAuthenticationHandler.PRINCIPAL_KEY
        )));
        when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(StubAuthenticationHandler.class.getName());
        when(config.getInitParameter(ENABLE_TRUSTED_PROXY)).thenReturn("true");
        when(config.getInitParameter("proxyuser." + authNUser + ".hosts")).thenReturn("10.222.0.0");
        when(config.getInitParameter(StubAuthenticationHandler.USER_KEY)).thenReturn(authNUser);
        when(config.getInitParameter(StubAuthenticationHandler.PRINCIPAL_KEY)).thenReturn(authNUser);
        when(config.getServletContext()).thenReturn(context);

        //Request comes from proxyuser from whitelisted host
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServerName()).thenReturn("localhost");
        when(request.getRemoteAddr()).thenReturn("10.222.0.0");
        when(request.getParameter("doAs")).thenReturn(doAsUser);
        when(request.getRequestURL()).thenReturn(new StringBuffer(""));
        when(request.getScheme()).thenReturn("https");

        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock(FilterChain.class);

        AuthenticationFilter authFilter = new AuthenticationFilter();
        authFilter.init(config);

        authFilter.doFilter(request, response, filterChain);

        // Assert for authNUser in cookie
        verify(response).addHeader(eq("Set-Cookie"), contains(
            AuthenticatedURL.AUTH_COOKIE + "=\"u=" + authNUser + "&p=" + authNUser + "&t=" + StubAuthenticationHandler.TYPE + "&e="
        ));
        // Assert for doAsUser in token passed to next filter
        ArgumentCaptor<HttpServletRequest> reqWrapperCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);
        verify(filterChain).doFilter(reqWrapperCaptor.capture(), any());
        HttpServletRequest reqPassedToNextFilter = reqWrapperCaptor.getValue();
        assertTrue(reqPassedToNextFilter instanceof AuthTokenRequestWrapper);
        AuthTokenRequestWrapper reqWrapper = (AuthTokenRequestWrapper) reqPassedToNextFilter;
        assertEquals(doAsUser, reqWrapper.getUserPrincipal().getName());
        assertEquals(doAsUser, reqWrapper.getRemoteUser());
    }

    @Test
    public void testProxyUserWithDoAsFromNonWhitelistedHost() throws Exception {
        String authNUser = "client";
        String doAsUser = "user1";

        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
            .thenReturn(null);

        FilterConfig config = mock(FilterConfig.class);
        when(config.getInitParameterNames()).thenReturn(new StringTokenizer(String.join(" ",
            AuthenticationFilter.AUTH_TYPE,
            ENABLE_TRUSTED_PROXY,
            "proxyuser." + authNUser + ".hosts",
            StubAuthenticationHandler.USER_KEY,
            StubAuthenticationHandler.PRINCIPAL_KEY
        )));
        when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(StubAuthenticationHandler.class.getName());
        when(config.getInitParameter(ENABLE_TRUSTED_PROXY)).thenReturn("true");
        when(config.getInitParameter("proxyuser." + authNUser + ".hosts")).thenReturn("10.222.0.0");
        when(config.getInitParameter(StubAuthenticationHandler.USER_KEY)).thenReturn(authNUser);
        when(config.getInitParameter(StubAuthenticationHandler.PRINCIPAL_KEY)).thenReturn(authNUser);
        when(config.getServletContext()).thenReturn(context);

        //Request comes from proxyuser from non-whitelisted host with a doAsUser
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServerName()).thenReturn("localhost");
        when(request.getRemoteAddr()).thenReturn("10.222.0.1");
        when(request.getParameter("doAs")).thenReturn(doAsUser);
        when(request.getRequestURL()).thenReturn(new StringBuffer(""));
        when(request.getScheme()).thenReturn("https");

        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock(FilterChain.class);

        AuthenticationFilter authFilter = new AuthenticationFilter();
        authFilter.init(config);

        authFilter.doFilter(request, response, filterChain);

        // Assert for auth cookie (authentication is successful, only the authenticated party is not authorized as a proxy user)
        verify(response).addHeader(eq("Set-Cookie"), contains(
            AuthenticatedURL.AUTH_COOKIE + "=\"u=" + authNUser + "&p=" + authNUser + "&t=" + StubAuthenticationHandler.TYPE + "&e="
        ));
        // Assert that doAs user is not extracted and passed on to authorization
        verify(filterChain, never()).doFilter(any(), any());
        // Assert that error response is sent back to client
        verify(response).sendError(eq(HttpServletResponse.SC_FORBIDDEN), any());
    }

    @Test
    public void testProxyUserWithoutDoAsFromNonWhitelistedHost() throws Exception {
        String authNUser = "client";

        ServletContext context = mock(ServletContext.class);
        when(context.getAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE))
            .thenReturn(null);

        FilterConfig config = mock(FilterConfig.class);
        when(config.getInitParameterNames()).thenReturn(new StringTokenizer(String.join(" ",
            AuthenticationFilter.AUTH_TYPE,
            ENABLE_TRUSTED_PROXY,
            "proxyuser." + authNUser + ".hosts",
            StubAuthenticationHandler.USER_KEY,
            StubAuthenticationHandler.PRINCIPAL_KEY
        )));
        when(config.getInitParameter(AuthenticationFilter.AUTH_TYPE)).thenReturn(StubAuthenticationHandler.class.getName());
        when(config.getInitParameter(ENABLE_TRUSTED_PROXY)).thenReturn("true");
        when(config.getInitParameter("proxyuser." + authNUser + ".hosts")).thenReturn("10.222.0.0");
        when(config.getInitParameter(StubAuthenticationHandler.USER_KEY)).thenReturn(authNUser);
        when(config.getInitParameter(StubAuthenticationHandler.PRINCIPAL_KEY)).thenReturn(authNUser);
        when(config.getServletContext()).thenReturn(context);

        //Request comes from proxyuser from non-whitelisted host without a doAsUser
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServerName()).thenReturn("localhost");
        when(request.getRemoteAddr()).thenReturn("10.222.0.0");
        when(request.getRequestURL()).thenReturn(new StringBuffer(""));
        when(request.getScheme()).thenReturn("https");

        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock(FilterChain.class);

        AuthenticationFilter authFilter = new AuthenticationFilter();
        authFilter.init(config);

        authFilter.doFilter(request, response, filterChain);

        // Assert for authNUser in cookie
        verify(response).addHeader(eq("Set-Cookie"), contains(
            AuthenticatedURL.AUTH_COOKIE + "=\"u=" + authNUser + "&p=" + authNUser + "&t=" + StubAuthenticationHandler.TYPE + "&e="
        ));
        // Assert for authNUser in token passed to next filter
        ArgumentCaptor<HttpServletRequest> reqWrapperCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);
        verify(filterChain).doFilter(reqWrapperCaptor.capture(), any());
        HttpServletRequest reqPassedToNextFilter = reqWrapperCaptor.getValue();
        assertTrue(reqPassedToNextFilter instanceof AuthTokenRequestWrapper);
        AuthTokenRequestWrapper reqWrapper = (AuthTokenRequestWrapper) reqPassedToNextFilter;
        assertEquals(authNUser, reqWrapper.getUserPrincipal().getName());
        assertEquals(authNUser, reqWrapper.getRemoteUser());
    }

    class TestServletInputStream extends ServletInputStream {
        private InputStream backingInputStram;

        TestServletInputStream(String requestBody) {
            backingInputStram = new ByteArrayInputStream(requestBody.getBytes());
        }

        @Override
        public int read() throws IOException {
            return backingInputStram.read();
        }

        @Override
        public int available() throws IOException {
            return backingInputStram.available();
        }
    }

    static class StubAuthenticationHandler implements AuthenticationHandler {
        static String USER_KEY = "authHandlerUser";
        static String PRINCIPAL_KEY = "authHandlerPrincipal";

        static String TYPE = StubAuthenticationHandler.class.getName();

        private String user;
        private String principal;


        @Override
        public String getType() {
            return TYPE;
        }

        @Override
        public void init(Properties config) throws ServletException {
            user = config.getProperty(USER_KEY);
            principal = config.getProperty(PRINCIPAL_KEY);
        }

        @Override
        public void destroy() {

        }

        @Override
        public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
            return new AuthenticationToken(user, principal, TYPE);
        }

        @Override
        public boolean shouldAuthenticate(HttpServletRequest request) {
            return true;
        }
    }
}
