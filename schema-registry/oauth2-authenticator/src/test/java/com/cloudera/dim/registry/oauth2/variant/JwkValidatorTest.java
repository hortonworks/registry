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
import org.apache.commons.lang3.StringUtils;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.JWK_URL;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.KEY_STORE_TYPE;
import static com.cloudera.dim.registry.oauth2.TestHmacJwtGenerator.generateSignedJwt;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwkValidatorTest {

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
        String secretKey = "969c55677e9f397060a21e4bbef1dcc9";
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
        JwtKeyStoreType type = JwtKeyStoreType.JWK;
        String url = "https://my.auth.server/jwks";
        String kid = "2";

        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);
        when(httpClient.readKeyFromUrl(any(), eq(url))).thenReturn(jwksJsonText);

        Properties config = new Properties();
        config.setProperty(JWK_URL, url);
        config.setProperty(KEY_STORE_TYPE, type.getValue());

        String jwtAsString = generateRsaJwt(JWSAlgorithm.RS256, kid, "marton");
        SignedJWT signedJWT = SignedJWT.parse(jwtAsString);

        validator = new JwkValidator(config, httpClient);
        boolean result = validator.validateSignature(signedJWT);

        assertTrue(result);
    }

    private String generateRsaJwt(JWSAlgorithm alg, @Nullable String kid, String subject) throws Exception {
        String rsaPrivKeyTxt;
        try (InputStream in = getClass().getResourceAsStream("/test.key.pcks8")) {
            rsaPrivKeyTxt = IOUtils.toString(in, StandardCharsets.UTF_8);
        }
        RSAPrivateKey rsaPrivateKey = readPrivateKey(rsaPrivKeyTxt);
        assertNotNull(rsaPrivateKey);

        final JwtClaims claims = new JwtClaims();
        claims.setSubject(subject);
        claims.setExpirationTimeMinutesInTheFuture(15);

        final JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setAlgorithmHeaderValue(alg.getName());
        jws.setKey(rsaPrivateKey);
        if (StringUtils.isNotBlank(kid)) {
            jws.setKeyIdHeaderValue(kid);
        }

        return jws.getCompactSerialization();
    }

    public static RSAPrivateKey readPrivateKey(String privateKeyTxt) {
        try {
            privateKeyTxt = privateKeyTxt
                    .replaceAll("-----BEGIN PRIVATE KEY-----", "")
                    .replaceAll("-----END PRIVATE KEY-----", "")
                    .replaceAll("\n", "");

            byte[] decoded = Base64.getDecoder().decode(privateKeyTxt);

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");

            EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(decoded);
            return (RSAPrivateKey) keyFactory.generatePrivate(privateKeySpec);
        } catch (Exception ex) {
            throw new RuntimeException("Error reading the private key.", ex);
        }
    }

}
