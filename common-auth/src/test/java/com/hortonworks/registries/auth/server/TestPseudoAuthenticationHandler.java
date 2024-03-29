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

import com.hortonworks.registries.auth.client.PseudoAuthenticator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Properties;

public class TestPseudoAuthenticationHandler {

    @Test
    public void testInit() throws Exception {
        PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
        try {
            Properties props = new Properties();
            props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
            handler.init(props);
            Assertions.assertEquals(false, handler.getAcceptAnonymous());
        } finally {
            handler.destroy();
        }
    }

    @Test
    public void testType() throws Exception {
        PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
        Assertions.assertEquals(PseudoAuthenticationHandler.TYPE, handler.getType());
    }

    @Test
    public void testAnonymousOn() throws Exception {
        PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
        try {
            Properties props = new Properties();
            props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
            handler.init(props);

            HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
            HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
            Mockito.when(request.getQueryString()).thenReturn("");

            AuthenticationToken token = handler.authenticate(request, response);

            Assertions.assertEquals(AuthenticationToken.ANONYMOUS, token);
        } finally {
            handler.destroy();
        }
    }

    @Test
    public void testAnonymousOff() throws Exception {
        PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
        try {
            Properties props = new Properties();
            props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
            handler.init(props);

            HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
            HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
            Mockito.when(request.getQueryString()).thenReturn("");

            AuthenticationToken token = handler.authenticate(request, response);
            Assertions.assertNull(token);
        } finally {
            handler.destroy();
        }
    }

    private void testUserName(boolean anonymous) throws Exception {
        PseudoAuthenticationHandler handler = new PseudoAuthenticationHandler();
        try {
            Properties props = new Properties();
            props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, Boolean.toString(anonymous));
            handler.init(props);

            HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
            HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
            Mockito.when(request.getQueryString()).thenReturn(PseudoAuthenticator.USER_NAME + "=" + "user");

            AuthenticationToken token = handler.authenticate(request, response);

            Assertions.assertNotNull(token);
            Assertions.assertEquals("user", token.getUserName());
            Assertions.assertEquals("user", token.getName());
            Assertions.assertEquals(PseudoAuthenticationHandler.TYPE, token.getType());
        } finally {
            handler.destroy();
        }
    }

    @Test
    public void testUserNameAnonymousOff() throws Exception {
        testUserName(false);
    }

    @Test
    public void testUserNameAnonymousOn() throws Exception {
        testUserName(true);
    }

}
