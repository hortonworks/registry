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
package com.hortonworks.registries.auth.client;

import com.hortonworks.registries.auth.server.AuthenticationFilter;
import com.hortonworks.registries.auth.server.PseudoAuthenticationHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class TestPseudoAuthenticator {

    private Properties getAuthenticationHandlerConfiguration(boolean anonymousAllowed) {
        Properties props = new Properties();
        props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
        props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, Boolean.toString(anonymousAllowed));
        return props;
    }

    @Test
    public void testGetUserName() throws Exception {
        PseudoAuthenticator authenticator = new PseudoAuthenticator();
        Assertions.assertEquals(System.getProperty("user.name"), authenticator.getUserName());
    }

    @Test
    public void testAnonymousAllowed() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase();
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration(true));
        auth.start();
        try {
            URL url = new URL(auth.getBaseURL());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            Assertions.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } finally {
            auth.stop();
        }
    }

    @Test
    public void testAnonymousDisallowed() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase();
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration(false));
        auth.start();
        try {
            URL url = new URL(auth.getBaseURL());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            Assertions.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
            Assertions.assertTrue(conn.getHeaderFields().containsKey("WWW-Authenticate"));
        } finally {
            auth.stop();
        }
    }

    @Test
    public void testAuthenticationAnonymousAllowed() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase();
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration(true));
        auth.testAuthentication(new PseudoAuthenticator(), false);
    }

    @Test
    public void testAuthenticationAnonymousDisallowed() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase();
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration(false));
        auth.testAuthentication(new PseudoAuthenticator(), false);
    }

    @Test
    public void testAuthenticationAnonymousAllowedWithPost() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase();
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration(true));
        auth.testAuthentication(new PseudoAuthenticator(), true);
    }

    @Test
    public void testAuthenticationAnonymousDisallowedWithPost() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase();
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration(false));
        auth.testAuthentication(new PseudoAuthenticator(), true);
    }

}
