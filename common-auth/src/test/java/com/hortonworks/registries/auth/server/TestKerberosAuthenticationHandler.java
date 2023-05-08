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

import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import com.hortonworks.registries.auth.KerberosTestUtils;
import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.client.KerberosAuthenticator;
import org.apache.commons.codec.binary.Base64;
import com.hortonworks.registries.auth.util.KerberosName;
import com.hortonworks.registries.auth.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.ietf.jgss.Oid;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.security.Principal;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TestKerberosAuthenticationHandler
        extends KerberosSecurityTestcase {

    protected KerberosAuthenticationHandler handler;

    protected Properties kerberosHandlerProps = null;

    protected KerberosAuthenticationHandler getNewAuthenticationHandler() {
        return new KerberosAuthenticationHandler();
    }

    protected String getExpectedType() {
        return KerberosAuthenticationHandler.TYPE;
    }

    protected Properties getDefaultProperties() {
        Properties props = new Properties();
        props.setProperty(KerberosAuthenticationHandler.PRINCIPAL,
                KerberosTestUtils.getServerPrincipal());
        props.setProperty(KerberosAuthenticationHandler.LOAD_BALANCER_PRINCIPAL,
                KerberosTestUtils.getLoadBalancerPrincipal());
        props.setProperty(KerberosAuthenticationHandler.KEYTAB,
                KerberosTestUtils.getKeytabFile());
        props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm() + ")s/@.*//\n");
        return props;
    }

    @BeforeEach
    public void setup() throws Exception {
        Properties props = loadProperties();
        try {
            handler.init(props);
        } catch (Exception ex) {
            handler = null;
            throw ex;
        }
    }

    Properties loadProperties() throws Exception {
        if (kerberosHandlerProps != null) {
            return kerberosHandlerProps;
        }

        kerberosHandlerProps = getDefaultProperties();
        // create keytab
        File keytabFile = new File(KerberosTestUtils.getKeytabFile());
        String clientPrincipal = KerberosTestUtils.getClientPrincipal();
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        startMiniKdc();
        getKdc().createPrincipal(keytabFile, clientPrincipal, serverPrincipal);
        // handler
        handler = getNewAuthenticationHandler();
        return kerberosHandlerProps;
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testNameRules() throws Exception {
        KerberosName kn = new KerberosName(KerberosTestUtils.getServerPrincipal());
        Assertions.assertEquals(KerberosTestUtils.getRealm(), kn.getRealm());

        //destroy handler created in setUp()
        handler.destroy();

        KerberosName.setRules("RULE:[1:$1@$0](.*@FOO)s/@.*//\nDEFAULT");

        handler = getNewAuthenticationHandler();
        Properties props = getDefaultProperties();
        props.setProperty(KerberosAuthenticationHandler.NAME_RULES, "RULE:[1:$1@$0](.*@BAR)s/@.*//\nDEFAULT");
        try {
            handler.init(props);
        } catch (Exception ex) {
        }
        kn = new KerberosName("bar@BAR");
        Assertions.assertEquals("bar", kn.getShortName());
        kn = new KerberosName("bar@FOO");
        Assertions.assertEquals("bar@FOO", kn.getShortName());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testInit() throws Exception {
        Assertions.assertEquals(KerberosTestUtils.getKeytabFile(), handler.getKeytab());
        Set<KerberosPrincipal> principals = handler.getPrincipals();
        Principal expectedPrincipal = new KerberosPrincipal(KerberosTestUtils.getServerPrincipal());
        Principal loadBalancerPrincipal = new KerberosPrincipal(KerberosTestUtils.getLoadBalancerPrincipal());
        Assertions.assertTrue(principals.contains(expectedPrincipal));
        Assertions.assertTrue(principals.contains(loadBalancerPrincipal));
        Assertions.assertEquals(2, principals.size());
    }

    // dynamic configuration of HTTP principals
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testDynamicPrincipalDiscovery() throws Exception {
        String[] keytabUsers = new String[]{
                "HTTP/host1", "HTTP/host2", "HTTP2/host1", "XHTTP/host"
        };
        String keytab = KerberosTestUtils.getKeytabFile();
        getKdc().createPrincipal(new File(keytab), keytabUsers);

        // destroy handler created in setUp()
        handler.destroy();
        Properties props = new Properties();
        props.setProperty(KerberosAuthenticationHandler.KEYTAB, keytab);
        props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, "*");
        handler = getNewAuthenticationHandler();
        handler.init(props);

        Assertions.assertEquals(KerberosTestUtils.getKeytabFile(), handler.getKeytab());

        Set<KerberosPrincipal> loginPrincipals = handler.getPrincipals();
        for (String user : keytabUsers) {
            Principal principal = new KerberosPrincipal(
                    user + "@" + KerberosTestUtils.getRealm());
            boolean expected = user.startsWith("HTTP/");
            Assertions.assertEquals(expected, loginPrincipals.contains(principal),
                    "checking for " + user);
        }
    }

    // dynamic configuration of HTTP principals
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testDynamicPrincipalDiscoveryMissingPrincipals() throws Exception {
        String[] keytabUsers = new String[]{"hdfs/localhost"};
        String keytab = KerberosTestUtils.getKeytabFile();
        getKdc().createPrincipal(new File(keytab), keytabUsers);

        // destroy handler created in setUp()
        handler.destroy();
        Properties props = new Properties();
        props.setProperty(KerberosAuthenticationHandler.KEYTAB, keytab);
        props.setProperty(KerberosAuthenticationHandler.PRINCIPAL, "*");
        handler = getNewAuthenticationHandler();
        try {
            handler.init(props);
            Assertions.fail("init should have failed");
        } catch (ServletException ex) {
            Assertions.assertEquals("Principals do not exist in the keytab",
                    ex.getCause().getMessage());
        } catch (Throwable t) {
            Assertions.fail("wrong exception: " + t);
        }
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testType() throws Exception {
        Assertions.assertEquals(getExpectedType(), handler.getType());
    }

    public void testRequestWithoutAuthorization() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assertions.assertNull(handler.authenticate(request, response));
        Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
        Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    public void testRequestWithInvalidAuthorization() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION)).thenReturn("invalid");
        Assertions.assertNull(handler.authenticate(request, response));
        Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
        Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testRequestWithIncompleteAuthorization() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
                .thenReturn(KerberosAuthenticator.NEGOTIATE);
        try {
            handler.authenticate(request, response);
            Assertions.fail();
        } catch (AuthenticationException ex) {
            // Expected
        } catch (Exception ex) {
            Assertions.fail();
        }
    }

    public void testRequestWithAuthorization() throws Exception {
        String token = generateClientToken();

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
                .thenReturn(KerberosAuthenticator.NEGOTIATE + " " + token);
        Mockito.when(request.getServerName()).thenReturn("localhost");

        AuthenticationToken authToken = handler.authenticate(request, response);

        if (authToken != null) {
            Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                    Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
            Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);

            Assertions.assertEquals(KerberosTestUtils.getClientPrincipal(), authToken.getName());
            Assertions.assertTrue(KerberosTestUtils.getClientPrincipal().startsWith(authToken.getUserName()));
            Assertions.assertEquals(getExpectedType(), authToken.getType());
        } else {
            Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                    Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
            Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    public void testRequestWithInvalidKerberosAuthorization() throws Exception {

        String token = new Base64(0).encodeToString(new byte[]{0, 1, 2});

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION)).thenReturn(
                KerberosAuthenticator.NEGOTIATE + token);

        try {
            handler.authenticate(request, response);
            Assertions.fail();
        } catch (AuthenticationException ex) {
            // Expected
        } catch (Exception ex) {
            Assertions.fail();
        }
    }

    String generateClientToken() throws Exception {
        String token = KerberosTestUtils.doAsClient(new Callable<String>() {
            @Override
            public String call() throws Exception {
                GSSManager gssManager = GSSManager.getInstance();
                GSSContext gssContext = null;
                try {
                    String servicePrincipal = KerberosTestUtils.getServerPrincipal();
                    Oid oid = KerberosUtil.NT_GSS_KRB5_PRINCIPAL_OID;
                    GSSName serviceName = gssManager.createName(servicePrincipal,
                            oid);
                    oid = KerberosUtil.GSS_KRB5_MECH_OID;
                    gssContext = gssManager.createContext(serviceName, oid, null,
                            GSSContext.DEFAULT_LIFETIME);
                    gssContext.requestCredDeleg(true);
                    gssContext.requestMutualAuth(true);

                    byte[] inToken = new byte[0];
                    byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
                    Base64 base64 = new Base64(0);
                    return base64.encodeToString(outToken);

                } finally {
                    if (gssContext != null) {
                        gssContext.dispose();
                    }
                }
            }
        });
        return token;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (handler != null) {
            handler.destroy();
            handler = null;
        }
        stopMiniKdc();
    }
}
