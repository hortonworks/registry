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
package com.cloudera.dim.schemaregistry;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.lang.JoseException;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.mock.Expectation;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.MediaType;
import org.mockserver.model.RequestDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.StringBody.exact;

public class TestOAuth2Server extends AbstractTestServer {

    private static final Logger LOG = LoggerFactory.getLogger(TestOAuth2Server.class);

    private volatile static TestOAuth2Server instance;

    private ClientAndServer mockWebServer;
    private Expectation issueAuthTokenExpectation;

    private String clientId = "test-client";
    private String secret = "secret";

    private TestOAuth2Server() { }

    public static TestOAuth2Server getInstance() {
        TestOAuth2Server localRef = instance;
        if (localRef == null) {
            synchronized (TestOAuth2Server.class) {
                if (localRef == null) {
                    instance = localRef = new TestOAuth2Server();
                }
            }
        }
        return localRef;
    }

    @Override
    public void start() throws Exception {
        boolean alreadyStarted = started.getAndSet(true);
        if (alreadyStarted) {
            return;
        }
        LOG.info("Starting mock OAuth2 server");

        mockWebServer = ClientAndServer.startClientAndServer();

        resetExpectations();

        running.set(true);
    }

    public int getPort() {
        return mockWebServer.getLocalPort();
    }

    public void resetExpectations() {
        mockWebServer.clear((RequestDefinition) null);
        mockWebServer.reset();

        issueAuthTokenExpectation = issueAuthToken();
    }

    private Expectation issueAuthToken() {
        String subject = "test-user";
        int expirationInMinutes = 5;

        HttpRequest expectedRequest = request()
            .withMethod("POST")
            .withPath("/auth")
            .withContentType(MediaType.APPLICATION_FORM_URLENCODED)
            .withBody(exact("grant_type=client_credentials"));

        Expectation[] expectations = mockWebServer
            .when(expectedRequest)
            .respond(request ->
                HttpResponse.response(generateJwt(subject, expirationInMinutes))
                    .withContentType(MediaType.parse("application/jwt")));

        return expectations[0];
    }

    private String generateJwt(String subject, int expirationInMinutes) throws JoseException {
        final JwtClaims claims = new JwtClaims();
        claims.setSubject(subject);
        claims.setExpirationTimeMinutesInTheFuture(expirationInMinutes);

        final JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
        jws.setKey(readPrivateKey());

        return jws.getCompactSerialization();
    }

    private RSAPrivateKey readPrivateKey() {
        try {
            String temp = IOUtils.toString(getClass().getResourceAsStream("/template/test_rsa.private"), StandardCharsets.UTF_8);
            String privateKeyPEM = temp.replaceAll("\n", "");

            byte[] decoded = Base64.decodeBase64(privateKeyPEM);

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");

            EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(decoded);
            return (RSAPrivateKey) keyFactory.generatePrivate(privateKeySpec);
        } catch (Exception ex) {
            throw new RuntimeException("Error reading the private key.", ex);
        }
    }

    public String getClientId() {
        return clientId;
    }

    public String getSecret() {
        return secret;
    }
}
