package com.hortonworks.registries.auth.server;

import com.hortonworks.registries.auth.KerberosTestUtils;
import com.hortonworks.registries.auth.client.KerberosAuthenticator;
import com.hortonworks.registries.auth.util.AuthToken;
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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.times;

public class TestKerberosBasicAuthenticationHandler
        extends KerberosSecurityTestcase {

    protected KerberosBasicAuthenticationHandler handler;

    protected KerberosBasicAuthenticationHandler getNewAuthenticationHandler() {
        return new KerberosBasicAuthenticationHandler();
    }

    protected String getExpectedType() {
        return KerberosBasicAuthenticationHandler.TYPE;
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
        String clientPrincipal1 = KerberosTestUtils.getClientPrincipal1();
        String serverPrincipal = KerberosTestUtils.getServerPrincipal();
        serverPrincipal = serverPrincipal.substring(0, serverPrincipal.lastIndexOf("@"));
        clientPrincipal = clientPrincipal.substring(0, clientPrincipal.lastIndexOf("@"));
        clientPrincipal1 = clientPrincipal1.substring(0, clientPrincipal1.lastIndexOf("@"));
        getKdc().createPrincipal(keytabFile, serverPrincipal, clientPrincipal);
        getKdc().createPrincipal(clientPrincipal1, "client123");
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
    public void testType() throws Exception {
        Assert.assertEquals(getExpectedType(), handler.getType());
    }


    @Test(timeout = 60000)
    public void testRequestWithoutAuthorization() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        request = new HttpServletRequestWrapper(request) {
            @Override
            public String getHeader(String headerName) {
                return null;
            }
            @Override
            public String getMethod() {
                return "POST";
            }
        };
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Assert.assertNull(handler.authenticate(request, response));
        Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
        Mockito.verify(response, times(2)).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    public void testRequestWithInvalidAuthorization() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getHeader(KerberosBasicAuthenticationHandler.AUTHORIZATION_HEADER)).thenReturn("invalid");
        Mockito.when(request.getMethod()).thenReturn("POST");

        Assert.assertNull(handler.authenticate(request, response));
        Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
        Mockito.verify(response, times(2)).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    public void testRequestWithInvalidCredentials() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getHeader(KerberosBasicAuthenticationHandler.AUTHORIZATION_HEADER)).thenReturn("Basic invalid");
        Mockito.when(request.getMethod()).thenReturn("POST");

        Assert.assertNull(handler.authenticate(request, response));
        Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
        Mockito.verify(response, times(2)).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test(timeout = 60000)
    public void testRequestWithValidCredentials() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getHeader(KerberosBasicAuthenticationHandler.AUTHORIZATION_HEADER)).thenReturn("Basic Y2xpZW50MTpjbGllbnQxMjM=");
        Mockito.when(request.getMethod()).thenReturn("POST");
        Mockito.when(request.isSecure()).thenReturn(true);

        AuthToken authToken = handler.authenticate(request, response);
        if (authToken != null) {
            Mockito.verify(response).setStatus(HttpServletResponse.SC_OK);

            Assert.assertEquals(KerberosTestUtils.getClientPrincipal1(), authToken.getName());
            Assert.assertTrue(KerberosTestUtils.getClientPrincipal1().startsWith(authToken.getUserName()));
            Assert.assertEquals(getExpectedType(), authToken.getType());
        } else {
            Assert.fail("AuthToken should not have been null.");
        }
    }

    @Test(timeout = 60000)
    public void testRequestWithValidCredentialsInvalidMethod() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getHeader(KerberosBasicAuthenticationHandler.AUTHORIZATION_HEADER)).thenReturn("Basic Y2xpZW50MTpjbGllbnQxMjM=");
        Mockito.when(request.getMethod()).thenReturn("GET");
        Mockito.when(request.isSecure()).thenReturn(true);

        AuthToken authToken = handler.authenticate(request, response);
        if (authToken != null) {
            Assert.fail("AuthToken should have been null if the HTTP method is not POST.");
        } else {
            Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
            Mockito.verify(response, times(2)).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @Test(timeout = 60000)
    public void testRequestWithValidCredentialsNonSecure() throws Exception {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(request.getHeader(KerberosBasicAuthenticationHandler.AUTHORIZATION_HEADER)).thenReturn("Basic Y2xpZW50MTpjbGllbnQxMjM=");
        Mockito.when(request.getMethod()).thenReturn("POST");
        Mockito.when(request.isSecure()).thenReturn(false);

        AuthToken authToken = handler.authenticate(request, response);
        if (authToken != null) {
            Assert.fail("AuthToken should have been null if the request is not secure.");
        } else {
            Mockito.verify(response).setHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
            Mockito.verify(response, times(2)).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }


    @Test(timeout = 60000)
    public void testRequestWithKerberosAuthorization() throws Exception {
        String token = KerberosTestUtils.doAsClient(new Callable<String>() {
            @Override
            public String call() throws Exception {
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
        });

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
            Assert.assertEquals(getExpectedType(), authToken.getType());
        } else {
            Mockito.verify(response).setHeader(Mockito.eq(KerberosAuthenticator.WWW_AUTHENTICATE),
                    Mockito.matches(KerberosAuthenticator.NEGOTIATE + " .*"));
            Mockito.verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    @Test(timeout = 60000)
    public void testRequestWithSPNEGODisabled() throws Exception {
        AuthenticationHandler handler = getNewAuthenticationHandler();
        Properties props = getDefaultProperties();
        props.setProperty(KerberosBasicAuthenticationHandler.SPNEGO_ENABLED_CONFIG, Boolean.FALSE.toString());
        try {
            handler.init(props);
        } catch (Exception ex) {
            handler = null;
            throw ex;
        }

        String token = KerberosTestUtils.doAsClient(new Callable<String>() {
            @Override
            public String call() throws Exception {
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
        });

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
                .thenReturn(KerberosAuthenticator.NEGOTIATE + " " + token);
        Mockito.when(request.getServerName()).thenReturn("localhost");
        Mockito.when(request.getMethod()).thenReturn("GET");

        AuthenticationToken authToken = handler.authenticate(request, response);

        if (authToken != null) {
            Assert.fail("AuthToken should have been null if the SPNEGO is disabled in KerberosAuthenticationLoginHandler.");
        } else {
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

