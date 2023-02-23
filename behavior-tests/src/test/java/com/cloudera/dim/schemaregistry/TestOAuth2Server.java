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

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.StringBody.exact;

public class TestOAuth2Server {

    private static final Logger LOG = LoggerFactory.getLogger(TestOAuth2Server.class);

    private volatile static TestOAuth2Server instance;

    private ClientAndServer mockWebServer;
    private Expectation issueAuthTokenExpectation;

    private String clientId = "test-client";
    private String secret = "secret";

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private TestOAuth2Server() {
    }

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

    public boolean isRunning() {
        return running.get();
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
        RSAPrivateKey rsaPrivateKey;
        try {
            String privateKeyTxt = IOUtils.toString(
                    getClass().getResourceAsStream("/template/test_rsa.private"), StandardCharsets.UTF_8);
            rsaPrivateKey = readPrivateKey(privateKeyTxt);
        } catch (IOException iex) {
            throw new RuntimeException("Failed to read private key from resource file.", iex);
        }

        JwtClaims claims = new JwtClaims();
        claims.setExpirationTimeMinutesInTheFuture(expirationInMinutes);
        claims.setGeneratedJwtId();
        claims.setIssuedAtToNow();
        claims.setSubject(subject);

        // sign the JWT
        JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setKey(rsaPrivateKey);
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);

        // this is the result
        return jws.getCompactSerialization();
    }

    public String getClientId() {
        return clientId;
    }

    public String getSecret() {
        return secret;
    }

    private static RSAPrivateKey readPrivateKey(String privateKeyTxt) {
        try {
            privateKeyTxt = privateKeyTxt
                    .replaceAll("-----BEGIN PRIVATE KEY-----", "")
                    .replaceAll("-----END PRIVATE KEY-----", "")
                    .replaceAll("\n", "");

            byte[] decoded = java.util.Base64.getDecoder().decode(privateKeyTxt);

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");

            EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(decoded);
            return (RSAPrivateKey) keyFactory.generatePrivate(privateKeySpec);
        } catch (Exception ex) {
            throw new RuntimeException("Error reading the private key.", ex);
        }
    }

    @Nonnull
    private static RSAPublicKey parseRSAPublicKey(@Nonnull String publicKeyText) {
        // The input can be a public key OR a certificate. Attempt to guess what we got.
        boolean isPublicKey = publicKeyText.contains("-----BEGIN PUBLIC KEY-----");

        publicKeyText = publicKeyText
                .replaceAll("-----BEGIN PUBLIC KEY-----", "")
                .replaceAll("-----END PUBLIC KEY-----", "")
                .replaceAll("\n", "");

        byte[] decoded = java.util.Base64.getDecoder().decode(publicKeyText);

        RSAPublicKey result;
        if (isPublicKey) {
            result = readBytesAsPublicKey(decoded);
            if (result == null) {
                result = readBytesAsX509Certificate(decoded);
            }
        } else {
            result = readBytesAsX509Certificate(decoded);
            if (result == null) {
                result = readBytesAsPublicKey(decoded);
            }
        }

        if (result == null) {
            throw new RuntimeException("Could not generate public RSA key.");
        }

        return result;
    }
    private static RSAPublicKey readBytesAsX509Certificate(byte[] decoded) {
        try {
            InputStream certstream = new ByteArrayInputStream(decoded);
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(certstream);
            return (RSAPublicKey) cert.getPublicKey();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private static RSAPublicKey readBytesAsPublicKey(byte[] decoded) {
        try {
            X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
