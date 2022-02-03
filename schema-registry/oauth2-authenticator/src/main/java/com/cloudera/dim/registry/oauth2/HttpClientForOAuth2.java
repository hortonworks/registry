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
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

/**
 * In case we need to communicate with an OAuth2 server
 * then we can use this http client.
 */
class HttpClientForOAuth2 {

    public static final String PROPERTY_PREFIX = "httpClient";
    public static final String OAUTH_GET_CERT_METHOD = "oauth2.get.cert.method";
    public static final String OAUTH_GET_CERT_METHOD_BODY = "oauth2.get.cert.body";
    public static final String OAUTH_GET_CERT_METHOD_CONTENT_TYPE = "oauth2.get.cert.content.type";

    private final Client client;

    public HttpClientForOAuth2(Map<String, String> conf) {
        ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder();
        for (String setting : conf.keySet()) {
            clientBuilder.property(setting, conf.get(setting));
        }

        if (conf.containsKey("trustStoreType")) {
            clientBuilder.sslContext(createSSLContext(conf));
        }

        client = clientBuilder.build();
        client.register(MultiPartFeature.class);
        configureClientForBasicAuth(conf, client);
    }

    public String download(URL url, String httpMethod, @Nullable Entity<?> body) {
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

    private void configureClientForBasicAuth(Map<String, String> config, Client client) {
        String userName = config.get("basic.user");
        String password = config.get("basic.password");
        if (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password)) {
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(userName, password);
            client.register(feature);
        }
    }

    private SSLContext createSSLContext(Map<String, String> sslConfigurations) {
        SslConfigurator sslConfigurator = SslConfigurator.newInstance();
        if (sslConfigurations.containsKey("keyStorePath")) {
            sslConfigurator.keyStoreType(sslConfigurations.get("keyStoreType"))
                    .keyStoreFile(sslConfigurations.get("keyStorePath"))
                    .keyStorePassword(sslConfigurations.get("keyStorePassword"))
                    .keyStoreProvider(sslConfigurations.get("keyStoreProvider"))
                    .keyManagerFactoryAlgorithm(sslConfigurations.get("keyManagerFactoryAlgorithm"))
                    .keyManagerFactoryProvider(sslConfigurations.get("keyManagerFactoryProvider"));
            if (sslConfigurations.containsKey("keyPassword")) {
                sslConfigurator.keyPassword(sslConfigurations.get("keyPassword"));
            }
        }

        sslConfigurator.trustStoreType(sslConfigurations.get("trustStoreType"))
                .trustStoreFile(sslConfigurations.get("trustStorePath"))
                .trustStorePassword(sslConfigurations.get("trustStorePassword"))
                .trustStoreProvider(sslConfigurations.get("trustStoreProvider"))
                .trustManagerFactoryAlgorithm(sslConfigurations.get("trustManagerFactoryAlgorithm"))
                .trustManagerFactoryProvider(sslConfigurations.get("trustManagerFactoryProvider"));

        sslConfigurator.securityProtocol(sslConfigurations.get("protocol"));

        return sslConfigurator.createSSLContext();
    }

}
