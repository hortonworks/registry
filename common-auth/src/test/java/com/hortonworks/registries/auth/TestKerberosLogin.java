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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;

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

    @Before
    public void setup() throws Exception {
        // create keytab
        File keytabFile = new File(KerberosTestUtils.getKeytabFile());
        String clientPrincipal = KerberosTestUtils.getClientPrincipal();
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
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

    @Test(timeout = 60000)
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
                    Oid oid = KerberosUtil.getOidInstance("NT_GSS_KRB5_PRINCIPAL");
                    GSSName serviceName = gssManager.createName(servicePrincipal,
                            oid);
                    oid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
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

            Assert.assertEquals(KerberosTestUtils.getClientPrincipal(), authToken.getName());
            Assert.assertTrue(KerberosTestUtils.getClientPrincipal().startsWith(authToken.getUserName()));
        } else {
            Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                    Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
            Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (handler != null) {
            handler.destroy();
            handler = null;
        }
    }
}
