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
package com.hortonworks.registries.auth.client;

import com.hortonworks.registries.auth.server.AuthenticationFilter;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.DispatcherType;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.security.KeyStore;
import java.security.Principal;
import java.util.EnumSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuthenticatorTestCase {
    public static final String KEYSTORE_PATH = "/testserver-kst.jks";
    public static final String KEYSTORE_PW = "cloudera";
    private Server server;
    private String host = null;
    private int port = -1;
    ServletContextHandler context;

    private static Properties authenticatorConfig;

    public static SSLSocketFactory getClientSocketFactory() throws Exception {
        InputStream keyStoreStream = AuthenticatorTestCase.class.getResourceAsStream(KEYSTORE_PATH);
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(keyStoreStream, KEYSTORE_PW.toCharArray());

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);

        SSLContext sslCtx = SSLContext.getInstance("TLS");
        sslCtx.init(null, tmf.getTrustManagers(), null);
        SSLSocketFactory sslSF = sslCtx.getSocketFactory();
        return sslSF;
    }

    protected static void setAuthenticationHandlerConfig(Properties config) {
        authenticatorConfig = config;
    }

    public static class TestFilter extends AuthenticationFilter {

        @Override
        protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
            return authenticatorConfig;
        }
    }

    @SuppressWarnings("serial")
    public static class TestServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            resp.setStatus(HttpServletResponse.SC_OK);
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            InputStream is = req.getInputStream();
            OutputStream os = resp.getOutputStream();
            int c = is.read();
            while (c > -1) {
                os.write(c);
                c = is.read();
            }
            is.close();
            os.close();
            resp.setStatus(HttpServletResponse.SC_OK);
        }
    }

    protected int getLocalPort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int ret = ss.getLocalPort();
        ss.close();
        return ret;
    }

    protected void start() throws Exception {
        host = "localhost";
        port = getLocalPort();
        server = new Server();

        SslContextFactory sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setKeyStorePath(this.getClass().getResource(KEYSTORE_PATH).toExternalForm());
        sslContextFactory.setKeyStorePassword(KEYSTORE_PW);

        ServerConnector  connector = new ServerConnector(server, sslContextFactory);
        connector.setHost(host);
        connector.setPort(port);
        context = new ServletContextHandler();
        context.setContextPath("/foo");
        context.addFilter(new FilterHolder(new TestFilter()), "/*", EnumSet.of(DispatcherType.REQUEST));
        context.addServlet(new ServletHolder(new TestServlet()), "/bar");
        server.setHandler(context);
        server.setConnectors(new Connector[]{connector});
        server.start();
        System.out.println("Running embedded servlet container at: https://" + host + ":" + port);
    }

    protected void stop() {
        try {
            server.stop();
        } catch (Exception e) {
        }

        try {
            server.destroy();
        } catch (Exception e) {
        }
    }

    protected String getBaseURL() {
        return "https://" + host + ":" + port + "/foo/bar";
    }

    protected HttpsURLConnection getConnection() throws Exception {
        HttpsURLConnection urlConnection = (HttpsURLConnection) new URL(getBaseURL()).openConnection();
        return urlConnection;
    }

    private static class TestConnectionConfigurator
            implements ConnectionConfigurator {
        boolean invoked;

        @Override
        public HttpURLConnection configure(HttpURLConnection conn)
                throws IOException {
            invoked = true;
            return conn;
        }
    }

    private String post = "test";

    protected void testAuthentication(Authenticator authenticator, boolean doPost) throws Exception {
        start();
        try {
            URL url = new URL(getBaseURL());
            AuthenticatedURL.Token token = new AuthenticatedURL.Token();
            assertFalse(token.isSet());
            TestConnectionConfigurator connConf = new TestConnectionConfigurator();
            AuthenticatedURL aUrl = new AuthenticatedURL(authenticator, connConf);
            HttpURLConnection conn = aUrl.openConnection(url, token);
            assertTrue(connConf.invoked);
            String tokenStr = token.toString();
            if (doPost) {
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
            }
            conn.connect();
            if (doPost) {
                Writer writer = new OutputStreamWriter(conn.getOutputStream());
                writer.write(post);
                writer.close();
            }
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            if (doPost) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String echo = reader.readLine();
                assertEquals(post, echo);
                assertNull(reader.readLine());
            }
            aUrl = new AuthenticatedURL();
            conn = aUrl.openConnection(url, token);
            conn.connect();
            assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
            assertEquals(tokenStr, token.toString());
        } finally {
            stop();
        }
    }

    private HttpClient getHttpClient() throws Exception {
        SSLContext sslContext = SSLContexts.custom()
            .loadTrustMaterial(new File(getClass().getResource(KEYSTORE_PATH).toURI()), KEYSTORE_PW.toCharArray())
            .build();
        Credentials useJaasCreds = new Credentials() {
            public String getPassword() {
                return null;
            }

            public Principal getUserPrincipal() {
                return null;
            }
        };
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, useJaasCreds);

        return HttpClients.custom()
            .setSSLContext(sslContext)
            .setDefaultAuthSchemeRegistry(
                RegistryBuilder.<AuthSchemeProvider>create()
                    .register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory(true))
                    .build()
            )
            .setDefaultCredentialsProvider(credentialsProvider)
            .build();
    }

    private void doHttpClientRequest(HttpClient httpClient, HttpUriRequest request) throws Exception {
        HttpResponse response = null;
        try {
            response = httpClient.execute(request);
            final int httpStatus = response.getStatusLine().getStatusCode();
            assertEquals(HttpURLConnection.HTTP_OK, httpStatus);
        } finally {
            if (response != null) {
                EntityUtils.consumeQuietly(response.getEntity());
            }
        }
    }

    protected void testAuthenticationHttpClient(Authenticator authenticator, boolean doPost) throws Exception {
        start();
        try {
            HttpClient httpClient = getHttpClient();
            doHttpClientRequest(httpClient, new HttpGet(getBaseURL()));

            // Always do a GET before POST to trigger the SPNego negotiation
            if (doPost) {
                HttpPost post = new HttpPost(getBaseURL());
                byte[] postBytes = this.post.getBytes();
                HttpEntity entity = new ByteArrayEntity(postBytes);

                post.setEntity(entity);
                doHttpClientRequest(httpClient, post);
            }
        } finally {
            stop();
        }
    }
}
