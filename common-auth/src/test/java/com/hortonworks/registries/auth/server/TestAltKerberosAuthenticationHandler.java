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
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.hortonworks.registries.auth.client.AuthenticationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

public class TestAltKerberosAuthenticationHandler
        extends TestKerberosAuthenticationHandler {

    @Override
    protected KerberosAuthenticationHandler getNewAuthenticationHandler() {
        // AltKerberosAuthenticationHandler is abstract; a subclass would normally
        // perform some other authentication when alternateAuthenticate() is called.
        // For the test, we'll just return an AuthenticationToken as the other
        // authentication is left up to the developer of the subclass
        return new AltKerberosAuthenticationHandler() {
            @Override
            public AuthenticationToken alternateAuthenticate(
                    HttpServletRequest request,
                    HttpServletResponse response)
                    throws IOException, AuthenticationException {
                return new AuthenticationToken("A", "B", getType());
            }
        };
    }

    @Override
    protected String getExpectedType() {
        return AltKerberosAuthenticationHandler.TYPE;
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testAlternateAuthenticationAsBrowser() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        // By default, a User-Agent without "java", "curl", "wget", or "perl" in it
        // is considered a browser
        Mockito.when(request.getHeader("User-Agent")).thenReturn("Some Browser");

        AuthenticationToken token = handler.authenticate(request, response);
        Assertions.assertEquals("A", token.getUserName());
        Assertions.assertEquals("B", token.getName());
        Assertions.assertEquals(getExpectedType(), token.getType());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testNonDefaultNonBrowserUserAgentAsBrowser() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        if (handler != null) {
            handler.destroy();
            handler = null;
        }
        handler = getNewAuthenticationHandler();
        Properties props = getDefaultProperties();
        props.setProperty("alt-kerberos.non-browser.user-agents", "foo, bar");
        try {
            handler.init(props);
        } catch (Exception ex) {
            handler = null;
            throw ex;
        }

        // Pretend we're something that will not match with "foo" (or "bar")
        Mockito.when(request.getHeader("User-Agent")).thenReturn("blah");
        // Should use alt authentication
        AuthenticationToken token = handler.authenticate(request, response);
        Assertions.assertEquals("A", token.getUserName());
        Assertions.assertEquals("B", token.getName());
        Assertions.assertEquals(getExpectedType(), token.getType());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testNonDefaultNonBrowserUserAgentAsNonBrowser() throws Exception {
        if (handler != null) {
            handler.destroy();
            handler = null;
        }
        handler = getNewAuthenticationHandler();
        Properties props = getDefaultProperties();
        props.setProperty("alt-kerberos.non-browser.user-agents", "foo, bar");
        try {
            handler.init(props);
        } catch (Exception ex) {
            handler = null;
            throw ex;
        }

        // Run the kerberos tests again
        testRequestWithoutAuthorization();
        testRequestWithInvalidAuthorization();
        testRequestWithAuthorization();
        testRequestWithInvalidKerberosAuthorization();
    }
}
