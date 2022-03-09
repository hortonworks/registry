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
import com.cloudera.dim.registry.oauth2.JwtKeyStoreType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWK_URL;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWT_PRINCIPAL_CLAIM;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.KEY_STORE_TYPE;
import static com.cloudera.dim.registry.oauth2.TestJwtGenerator.generateSignedJwt;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwkValidatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(JwkValidatorTest.class);

    private JwkValidator validator;
    private String jwksJsonText;

    @BeforeEach
    public void setUp() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/jwks.json")) {
            jwksJsonText = IOUtils.toString(in, StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testHmacValidation() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.JWK;
        String url = "https://my.auth.server/jwks";
        String secretKey = "FdFYFzERwC2uCBB46pZQi4GG85LujR8obt-KWRBICVQ";
        String kid = "3";

        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);
        when(httpClient.readKeyFromUrl(any(), eq(url))).thenReturn(jwksJsonText);

        Properties config = new Properties();
        config.setProperty(JWK_URL, url);
        config.setProperty(KEY_STORE_TYPE, type.getValue());

        String jwtAsString = generateSignedJwt(JWSAlgorithm.HS256, kid, "abigel", secretKey);
        SignedJWT signedJWT = SignedJWT.parse(jwtAsString);

        validator = new JwkValidator(config, httpClient);
        boolean result = validator.validateSignature(signedJWT);

        assertTrue(result);
    }

    @Test
    public void testRsaValidation() throws Exception {
        Properties config = new Properties();
        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);
        String jwtAsString = generateRsaJwt(config, httpClient);

        SignedJWT signedJWT = SignedJWT.parse(jwtAsString);

        validator = new JwkValidator(config, httpClient);
        boolean result = validator.validateSignature(signedJWT);

        assertTrue(result);
    }

    @Test
    public void testNoSubClaim() throws Exception {
        Properties config = new Properties();
        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);
        String jwtAsString = generateRsaJwt(config, httpClient);

        SignedJWT signedJWT = SignedJWT.parse(jwtAsString);

        config.setProperty(JWT_PRINCIPAL_CLAIM, "xxx");
        validator = new JwkValidator(config, httpClient);

        assertThrows(RuntimeException.class, () -> validator.validateSignature(signedJWT));
    }

    private String generateRsaJwt(Properties config, HttpClientForOAuth2 httpClient) throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.JWK;
        String url = "https://my.auth.server/jwks";
        String kid = "2";

        when(httpClient.readKeyFromUrl(any(), eq(url))).thenReturn(jwksJsonText);

        config.setProperty(JWK_URL, url);
        config.setProperty(KEY_STORE_TYPE, type.getValue());

        String jwtAsString = generateSignedJwt(JWSAlgorithm.RS256, kid, "marton");
        LOG.debug("Generated JWT: {}", jwtAsString);
        return jwtAsString;
    }

}
