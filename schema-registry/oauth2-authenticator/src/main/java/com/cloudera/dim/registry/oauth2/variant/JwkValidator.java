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
import com.hortonworks.registries.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import java.security.interfaces.RSAPublicKey;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWK_URL;

/**
 * Validate a JWT with the keys obtained from a JWK.
 */
public class JwkValidator implements JwtValidatorVariant {

    private static final Logger LOG = LoggerFactory.getLogger(JwkValidator.class);

    private static final String KEYTYPE_RSA = "RSA";
    private static final String KEYTYPE_OCTET = "oct";

    private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Jwk> keys;
    private final HttpClientForOAuth2 httpClient;

    public JwkValidator(Properties config, @Nonnull HttpClientForOAuth2 httpClient) throws ServletException {
        this.httpClient = httpClient;
        this.keys = Collections.synchronizedList(retrieveKeys(config, httpClient));

        // refresh the JWKs every 5 minutes
        threadPool.submit(new JwkRefresher(config, httpClient));
    }

    @Override
    public boolean validateSignature(SignedJWT jwtToken) {
        LOG.debug("Validating signature for JWT");
        try {
            String kid = jwtToken.getHeader().getKeyID();
            JWSAlgorithm alg = jwtToken.getHeader().getAlgorithm();

            Jwk matchingKey = findKeyForJwt(kid, alg);
            if (matchingKey == null) {
                LOG.info("No matching key found for {}", kid);
            } else {
                switch (matchingKey.getKty()) {
                    case KEYTYPE_RSA:
                        return validateRsa(jwtToken, matchingKey);
                    case KEYTYPE_OCTET:
                        return validateOctet(jwtToken, matchingKey);
                    default:
                        // indicates a bug in the code
                        throw new RuntimeException("Unsupported keytype: " + matchingKey.getKty());
                }
            }
        } catch (Exception ex) {
            LOG.warn("Failed to validate JWT.", ex);
        }
        return false;
    }

    private boolean validateRsa(SignedJWT jwtToken, Jwk jwk) {
        String keyText = jwk.getX5c() == null ? null : jwk.getX5c().getValue();
        if (keyText == null) {
            keyText = jwk.getN();
        }

        RSAPublicKey rsaPublicKey = RsaUtil.parseRSAPublicKey(keyText);
        return RsaUtil.validateSignature(rsaPublicKey, jwtToken);
    }

    private boolean validateOctet(SignedJWT jwtToken, Jwk jwk) {
        JWSAlgorithm algType = JWSAlgorithm.parse(jwk.getAlg());
        if (algType == JWSAlgorithm.HS256 || algType == JWSAlgorithm.HS384 || algType == JWSAlgorithm.HS512) {
            try {
                JWSVerifier verifier = new MACVerifier(jwk.getK());
                return jwtToken.verify(verifier);
            } catch (Exception ex) {
                LOG.warn("Failed to validate HMAC signature.", ex);
            }
        } else {
            throw new RuntimeException("Unsupported algorithm: " + jwk.getAlg());
        }
        return false;
    }

    private Jwk findKeyForJwt(String kid, JWSAlgorithm alg) {
        Jwk matchingKey = null;
        for (Jwk jwk : keys) {
            if (kid != null) {
                // Azure AD doesn't provide alg, so we can only use kid
                if (kid.equals(jwk.getKid())) {
                    matchingKey = jwk;
                    break;
                }
            } else {
                if (alg.getName().equals(jwk.getAlg())) {
                    matchingKey = jwk;
                    break;
                }
            }
        }
        return matchingKey;
    }

    private List<Jwk> retrieveKeys(Properties config, HttpClientForOAuth2 httpClient) throws ServletException {
        String url = config.getProperty(JWK_URL);
        LOG.info("Loading JWK from {}", url);
        if (StringUtils.isBlank(url)) {
            throw new IllegalArgumentException("Property is required: " + JWK_URL);
        }

        try {
            String response = httpClient.readKeyFromUrl(config, url);
            if (StringUtils.isBlank(response)) {
                throw new ServletException("Could not download JWK from " + url + ", received an empty response.");
            }

            JwkList jwkList = objectMapper.readValue(response, JwkList.class);
            if (jwkList == null) {
                throw new ServletException("Failed to parse response from " + url);
            }

            return jwkList.getKeys().stream()
                // only select keys with an ID
                .filter(jwk -> StringUtils.isNotBlank(jwk.getKid()))
                // only select keys intended for signing
                .filter(jwk -> "sig".equals(jwk.getUse()) || jwk.getUse() == null)
                // only select RSA and HMAC/AES keys
                .filter(jwk -> KEYTYPE_RSA.equals(jwk.getKty()) || KEYTYPE_OCTET.equals(jwk.getKty()))
                .collect(Collectors.toList());
        } catch (ServletException slex) {
            throw slex;
        } catch (Exception ex) {
            throw new ServletException("Couldn't read JWK.", ex);
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
                    wait(5 * 60000L);
                } catch (InterruptedException iex) {
                    LOG.warn("JWK refresh thread was interrupted.");
                    return;
                }

                try {
                    List<Jwk> jwks = retrieveKeys(config, httpClient);
                    if (jwks != null && jwks.size() > 0) {
                        keys.clear();
                        keys.addAll(jwks);
                    }
                } catch (Exception ex) {
                    LOG.warn("Exception while refreshing JWKs.", ex);
                }
            }
        }
    }

}
