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
package com.hortonworks.registries.auth;

import com.hortonworks.registries.auth.client.KerberosAuthenticator;
import com.hortonworks.registries.auth.server.AuthenticationToken;
import com.hortonworks.registries.auth.server.KerberosAuthenticationHandler;
import com.hortonworks.registries.auth.util.JaasConfiguration;
import com.hortonworks.registries.auth.util.KerberosUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestKerberosLogin extends KerberosSecurityTestcase {

    protected KerberosAuthenticationHandler handler;

    protected KerberosAuthenticationHandler getNewAuthenticationHandler() {
        return new KerberosAuthenticationHandler();
    }

    protected Properties getDefaultProperties() {
        Properties props = new Properties();
        props.setProperty(KerberosAuthenticationHandler.PRINCIPAL,
                KerberosTestUtils.getServerPrincipal());
        props.setProperty(KerberosAuthenticationHandler.KEYTAB,
                KerberosTestUtils.getKeytabFile());
        props.setProperty(KerberosAuthenticationHandler.NAME_RULES,
                "RULE:[1:$1@$0](.*@" + KerberosTestUtils.getRealm() + ")s/@.*//\n");
        return props;
    }

    @BeforeEach
    public void setup() throws Exception {
        // create keytab
        File keytabFile = new File(KerberosTestUtils.getKeytabFile());
        String clientPrincipal = KerberosTestUtils.getClientPrincipal();
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
        startMiniKdc();
        getKdc().createPrincipal(keytabFile, serverPrincipal, clientPrincipal);
        // handler
        handler = getNewAuthenticationHandler();
        Properties props = getDefaultProperties();
        try {
            handler.init(props);
        } catch (Exception ex) {
            handler = null;
            throw ex;
        }
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    public void testRequestWithKerberosAuthorization() throws Exception {
        KerberosLogin login = new KerberosLogin();
        System.out.println(KerberosTestUtils.getJaasConfigForClientPrincipal());
        login.configure(new HashMap<>(), "RegistryClient",
                new JaasConfiguration("RegistryClient", KerberosTestUtils.getJaasConfigForClientPrincipal()));
        login.login();

        String token = Subject.doAs(login.loginContext.getSubject(), (PrivilegedExceptionAction<String>) () ->  {
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
        );

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
                .thenReturn(KerberosAuthenticator.NEGOTIATE + " " + token);
        Mockito.when(request.getServerName()).thenReturn("localhost");
        Mockito.when(request.getMethod()).thenReturn("GET");

        AuthenticationToken authToken = handler.authenticate(request, response);

        if (authToken != null) {
            Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                    Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
            Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);

            Assertions.assertEquals(KerberosTestUtils.getClientPrincipal(), authToken.getName());
            Assertions.assertTrue(KerberosTestUtils.getClientPrincipal().startsWith(authToken.getUserName()));
        } else {
            Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                    Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
            Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (handler != null) {
            handler.destroy();
            handler = null;
        }
    }
}
