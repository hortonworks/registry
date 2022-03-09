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
package com.cloudera.dim.registry.oauth2.variant;

import com.cloudera.dim.registry.oauth2.HttpClientForOAuth2;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jose4j.jwk.JsonWebKeySet;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.keys.resolvers.JwksVerificationKeyResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.CLOCK_SKEW;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.EXPECTED_JWT_AUDIENCES;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.EXPECTED_JWT_ISSUER;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWK_REFRESH_MS;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWK_URL;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWT_PRINCIPAL_CLAIM;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

/**
 * Validate a JWT with the keys obtained from a JWK.
 */
public class JwkValidator implements JwtValidatorVariant {

    private static final Logger LOG = LoggerFactory.getLogger(JwkValidator.class);

    private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicReference<JsonWebKeySet> keys;
    private final HttpClientForOAuth2 httpClient;

    private final Integer clockSkew;
    private final String[] expectedAudiences;
    private final String expectedIssuer;
    private final long refreshIntervalMs;
    private final String jwtPrincipalClaim;

    public JwkValidator(Properties config, @Nonnull HttpClientForOAuth2 httpClient) throws ServletException {
        this.httpClient = httpClient;
        this.clockSkew = config.containsKey(CLOCK_SKEW) ? Integer.parseInt(config.getProperty(CLOCK_SKEW)) : null;
        this.expectedAudiences = config.containsKey(EXPECTED_JWT_AUDIENCES) ? ((String) config.get(EXPECTED_JWT_AUDIENCES)).split("\\n") : null;
        this.expectedIssuer = config.containsKey(EXPECTED_JWT_ISSUER) ? (String) config.get(EXPECTED_JWT_ISSUER) : null;
        this.refreshIntervalMs = Long.parseLong((String) config.getOrDefault(JWK_REFRESH_MS, String.valueOf(5 * 60000L)));
        this.keys = new AtomicReference<>(retrieveKeys(config, httpClient));
        this.jwtPrincipalClaim = config.containsKey(JWT_PRINCIPAL_CLAIM) ? (String) config.get(JWT_PRINCIPAL_CLAIM) : "sub";

        // refresh the JWKs every 5 minutes
        threadPool.submit(new JwkRefresher(config, httpClient));
    }

    @Override
    public boolean validateSignature(SignedJWT jwtToken) {
        LOG.debug("Validating signature for JWT");
        try {

            final JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

            if (clockSkew != null) {
                jwtConsumerBuilder.setAllowedClockSkewInSeconds(clockSkew);
            }
            if (expectedAudiences != null && expectedAudiences.length > 0) {
                jwtConsumerBuilder.setExpectedAudience(expectedAudiences);
            }

            if (expectedIssuer != null) {
                jwtConsumerBuilder.setExpectedIssuer(expectedIssuer);
            }

            JwksVerificationKeyResolver keyResolver = new JwksVerificationKeyResolver(keys.get().getJsonWebKeys());

            JwtConsumer jwtConsumer = jwtConsumerBuilder
                    .setJwsAlgorithmConstraints(DISALLOW_NONE)
                    .setRequireExpirationTime()
                    .setRequireIssuedAt()
                    .setVerificationKeyResolver(keyResolver)
                    .build();

            LOG.debug("Received JWT: {}", jwtToken.getParsedString());

            JwtContext jwtContext = jwtConsumer.process(jwtToken.getParsedString());
            if (jwtContext == null) {
                throw new RuntimeException("Could not validate JWT.");
            }

            String subject = jwtContext.getJwtClaims().getClaimValueAsString(jwtPrincipalClaim);
            if (subject == null) {
                throw new RuntimeException(String.format("Could not read the claim '%s' from the access token.", jwtPrincipalClaim));
            }

            return true;
        } catch (InvalidJwtException ex) {
            LOG.warn("Failed to validate JWT.", ex);
        }
        return false;
    }

    private JsonWebKeySet retrieveKeys(Properties config, HttpClientForOAuth2 httpClient) throws ServletException {
        String url = config.getProperty(JWK_URL);
        LOG.info("Loading JWK from {}", url);
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("Property is required: " + JWK_URL);
        }

        URL jwksEndpointUrl;
        try {
            jwksEndpointUrl = new URL(url);
        } catch (MalformedURLException mlex) {
            throw new ServletException("The provided JWK url is not valid: " + url, mlex);
        }

        if (jwksEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            return jwkFromFile(jwksEndpointUrl);
        } else {
            return jwkFromNetwork(config, url, httpClient);
        }
    }

    private JsonWebKeySet jwkFromNetwork(Properties config, String url, HttpClientForOAuth2 httpClient) throws ServletException {
        try {
            String response = httpClient.readKeyFromUrl(config, url);
            if (StringUtils.isBlank(response)) {
                throw new ServletException(String.format("Could not download JWK from %s, received an empty response.", url));
            }

            return new JsonWebKeySet(response);
        } catch (ServletException slex) {
            throw slex;
        } catch (Exception ex) {
            throw new ServletException("Could not download JWK from the network.", ex);
        }
    }

    @Nonnull
    private JsonWebKeySet jwkFromFile(URL jwksEndpointUrl) throws ServletException {
        try {
            File file = new File(jwksEndpointUrl.toURI().getRawPath()).getAbsoluteFile();

            if (!file.exists()) {
                throw new ServletException("JWKS file does not exist: " + file.getAbsolutePath());
            }

            if (!file.canRead()) {
                throw new ServletException("JWKS file can't be read: " + file.getAbsolutePath());
            }

            if (file.isDirectory()) {
                throw new ServletException("JWKS file is a directory: " + file.getAbsolutePath());
            }

            String json = IOUtils.toString(jwksEndpointUrl.toURI(), StandardCharsets.UTF_8);
            if (StringUtils.isBlank(json)) {
                throw new ServletException("JWKS file contents is blank: " + file.getAbsolutePath());
            }

            return new JsonWebKeySet(json);
        } catch (ServletException slex) {
            throw slex;
        } catch (URISyntaxException ue) {
            throw new ServletException(String.format("The JWKS URL is malformed: %s", jwksEndpointUrl), ue);
        } catch (Exception ex) {
            throw new ServletException("Could not read JWKS file.", ex);
        }
    }

    @Override
    public void close() {
        shutdown.set(true);
        threadPool.shutdown();
        if (httpClient != null) {
            httpClient.close();
        }
    }

    /** We'll query the JWK URL every 5 minutes to ensure we've got the latest keys. */
    private class JwkRefresher implements Runnable {

        private final Properties config;
        private final HttpClientForOAuth2 httpClient;

        public JwkRefresher(Properties config, HttpClientForOAuth2 httpClient) {
            this.config = config;
            this.httpClient = httpClient;
        }

        @Override
        public synchronized void run() {
            while (!shutdown.get() && !Thread.interrupted()) {
                try {
                    wait(refreshIntervalMs);
                } catch (InterruptedException iex) {
                    LOG.warn("JWK refresh thread was interrupted.");
                    return;
                }

                try {
                    JsonWebKeySet newKeyset = retrieveKeys(config, httpClient);
                    if (newKeyset != null && newKeyset.getJsonWebKeys().size() > 0) {
                        keys.set(newKeyset);
                    }
                } catch (Exception ex) {
                    LOG.warn("Exception while refreshing JWKs.", ex);
                }
            }
        }
    }

}
