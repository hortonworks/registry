/**
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

import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import com.hortonworks.registries.auth.KerberosTestUtils;
import com.hortonworks.registries.auth.server.AuthenticationFilter;
import com.hortonworks.registries.auth.server.PseudoAuthenticationHandler;
import com.hortonworks.registries.auth.server.KerberosAuthenticationHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import org.junit.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Callable;

@Ignore
@RunWith(Parameterized.class)
public class TestKerberosAuthenticator extends KerberosSecurityTestcase {

    private boolean useTomcat = false;

    public TestKerberosAuthenticator(boolean useTomcat) {
        this.useTomcat = useTomcat;
    }

    @Parameterized.Parameters
    public static Collection booleans() {
        return Arrays.asList(new Object[][]{
                {false},
                {true}
        });
    }

    @Before
    public void setup() throws Exception {
        // create keytab
        File keytabFile = new File(KerberosTestUtils.getKeytabFile());
        String clientPrincipal = KerberosTestUtils.getClientPrincipal();
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
    }

    private Properties getAuthenticationHandlerConfiguration() {
        Properties props = new Properties();
        props.setProperty(AuthenticationFilter.AUTH_TYPE, "kerberos");
        props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, KerberosTestUtils.getServerPrincipal());
        props.setProperty(KerberosAuthenticationHandler.KEYTAB, KerberosTestUtils.getKeytabFile());
        props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm() + ")s/@.*//\n");
        return props;
    }

    @Test(timeout = 60000)
    public void testFallbacktoPseudoAuthenticator() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        Properties props = new Properties();
        props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
        props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "false");
        AuthenticatorTestCase.setAuthenticationHandlerConfig(props);
        auth._testAuthentication(new KerberosAuthenticator(), false);
    }

    @Test(timeout = 60000)
    public void testFallbacktoPseudoAuthenticatorAnonymous() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        Properties props = new Properties();
        props.setProperty(AuthenticationFilter.AUTH_TYPE, "simple");
        props.setProperty(PseudoAuthenticationHandler.ANONYMOUS_ALLOWED, "true");
        AuthenticatorTestCase.setAuthenticationHandlerConfig(props);
        auth._testAuthentication(new KerberosAuthenticator(), false);
    }

    @Test(timeout = 60000)
    public void testNotAuthenticated() throws Exception {
        AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        AuthenticatorTestCase.setAuthenticationHandlerConfig(getAuthenticationHandlerConfiguration());
        auth.start();
        try {
            URL url = new URL(auth.getBaseURL());
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
            Assert.assertTrue(conn.getHeaderField(KerberosAuthenticator.WWW_AUTHENTICATE) != null);
        } finally {
            auth.stop();
        }
    }

    @Test(timeout = 60000)
    public void testAuthentication() throws Exception {
        final AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration());
        KerberosTestUtils.doAsClient(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                auth._testAuthentication(new KerberosAuthenticator(), false);
                return null;
            }
        });
    }

    @Test(timeout = 60000)
    public void testAuthenticationPost() throws Exception {
        final AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration());
        KerberosTestUtils.doAsClient(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                auth._testAuthentication(new KerberosAuthenticator(), true);
                return null;
            }
        });
    }

    @Test(timeout = 60000)
    public void testAuthenticationHttpClient() throws Exception {
        final AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration());
        KerberosTestUtils.doAsClient(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                auth._testAuthenticationHttpClient(new KerberosAuthenticator(), false);
                return null;
            }
        });
    }

    @Test(timeout = 60000)
    public void testAuthenticationHttpClientPost() throws Exception {
        final AuthenticatorTestCase auth = new AuthenticatorTestCase(useTomcat);
        AuthenticatorTestCase.setAuthenticationHandlerConfig(
                getAuthenticationHandlerConfiguration());
        KerberosTestUtils.doAsClient(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                auth._testAuthenticationHttpClient(new KerberosAuthenticator(), true);
                return null;
            }
        });
    }
}
