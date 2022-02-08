/**
 * Copyright 2016-2022 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.cloudera.dim.registry.oauth2;

import com.hortonworks.registries.shaded.javax.ws.rs.client.Client;
import com.hortonworks.registries.shaded.javax.ws.rs.client.ClientBuilder;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Entity;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Invocation;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;
import com.hortonworks.registries.shaded.org.glassfish.jersey.SslConfigurator;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.JerseyClientBuilder;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import com.hortonworks.registries.shaded.org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

/**
 * In case we need to communicate with an OAuth2 server
 * then we can use this http client.
 */
public class HttpClientForOAuth2 {

    /** Config properties for this http client need to start with this prefix. */
    public static final String PROPERTY_PREFIX = "httpClient";

    private final Client client;

    public HttpClientForOAuth2(Map<String, String> conf) {
        ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder();
        for (String setting : conf.keySet()) {
            clientBuilder.property(setting, conf.get(setting));
        }

        if (conf.containsKey(OAuth2Config.OAUTH_HTTP_TRUST_STORE_TYPE)) {
            clientBuilder.sslContext(createSSLContext(conf));
        }

        client = clientBuilder.build();
        client.register(MultiPartFeature.class);
        configureClientForBasicAuth(conf, client);
    }

    /** Download any content from the given URL. */
    public String download(URL url, String httpMethod, Entity<?> body) {
        try {
            Invocation.Builder request = client.target(url.toURI()).request();
            Response response;
            switch (httpMethod.toLowerCase()) {
                case "post":
                    response = request.post(body);
                    break;
                case "put":
                    response = request.put(body);
                    break;
                case "get":
                default:
                    response = request.get();
                    break;
            }

            return response.readEntity(String.class);
        } catch (URISyntaxException urex) {
            throw new RuntimeException("Unexpected exception.", urex);
        }
    }

    /** Download key from the given URL. */
    public String readKeyFromUrl(Properties config, String url) throws IOException {
        if (isBlank(url)) {
            throw new RuntimeException("URL is missing, don't know where to download keys from.");
        }
        URL keyUrl = new URL(url);

        switch (keyUrl.getProtocol().toLowerCase()) {
            // read from the network
            case "http":
            case "https":
                String httpMethod = config.getProperty(OAuth2Config.OAUTH_GET_CERT_METHOD, "get").toLowerCase();
                Entity<?> body = null;
                if (httpMethod.equals("post") || httpMethod.equals("put")) {
                    String bodyTxt = config.getProperty(OAuth2Config.OAUTH_GET_CERT_METHOD_BODY);
                    String contentType = config.getProperty(OAuth2Config.OAUTH_GET_CERT_METHOD_CONTENT_TYPE, "application/x-www-form-urlencoded");
                    if (!isBlank(bodyTxt)) {
                        body = Entity.entity(bodyTxt, contentType);
                    }
                }
                return this.download(keyUrl, httpMethod, body);

            // read from elsewhere (eg. file)
            default:
                return readUrl(keyUrl, StandardCharsets.UTF_8.name());
        }
    }

    private static String readUrl(URL url, String encoding) throws IOException {
        try (InputStream inputStream = url.openStream()) {
            StringBuilder textBuilder = new StringBuilder();
            try (Reader reader = new BufferedReader(new InputStreamReader(inputStream, encoding))) {
                int c;
                while ((c = reader.read()) != -1) {
                    textBuilder.append((char) c);
                }
                return textBuilder.toString();
            }
        }
    }

    private static boolean isBlank(String str) {
        return (str == null || str.trim().isEmpty());
    }

    private void configureClientForBasicAuth(Map<String, String> config, Client client) {
        String userName = config.get(OAuth2Config.OAUTH_HTTP_BASIC_USER);
        String password = config.get(OAuth2Config.OAUTH_HTTP_BASIC_PASSWORD);
        if (!isBlank(userName) && !isBlank(password)) {
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(userName, password);
            client.register(feature);
        }
    }

    private SSLContext createSSLContext(Map<String, String> sslConfigurations) {
        SslConfigurator sslConfigurator = SslConfigurator.newInstance();
        if (sslConfigurations.containsKey(OAuth2Config.OAUTH_HTTP_KEY_STORE_PATH)) {
            sslConfigurator.keyStoreType(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_STORE_TYPE))
                    .keyStoreFile(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_STORE_PATH))
                    .keyStorePassword(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_STORE_PASSWORD))
                    .keyStoreProvider(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_STORE_PROVIDER))
                    .keyManagerFactoryAlgorithm(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_MANAGER_FACTORY_ALGORITHM))
                    .keyManagerFactoryProvider(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_MANAGER_FACTORY_PROVIDER));
            if (sslConfigurations.containsKey(OAuth2Config.OAUTH_HTTP_KEY_PASSWORD)) {
                sslConfigurator.keyPassword(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_KEY_PASSWORD));
            }
        }

        sslConfigurator.trustStoreType(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_TRUST_STORE_TYPE))
                .trustStoreFile(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_TRUST_STORE_PATH))
                .trustStorePassword(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_TRUST_STORE_PASSWORD))
                .trustStoreProvider(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_TRUST_STORE_PROVIDER))
                .trustManagerFactoryAlgorithm(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_TRUST_MANAGER_FACTORY_ALGORITHM))
                .trustManagerFactoryProvider(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_TRUST_MANAGER_FACTORY_PROVIDER));

        sslConfigurator.securityProtocol(sslConfigurations.get(OAuth2Config.OAUTH_HTTP_SECURITY_PROTOCOL));

        return sslConfigurator.createSSLContext();
    }
    
    public void close() {
        if (client != null) {
            client.close();
        }
    }

}
