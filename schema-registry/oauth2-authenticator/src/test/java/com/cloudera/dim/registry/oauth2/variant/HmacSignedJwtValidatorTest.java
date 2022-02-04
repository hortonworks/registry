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
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.HMAC_SECRET_KEY_PROPERTY;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.HMAC_SECRET_KEY_URL;
import static com.cloudera.dim.registry.oauth2.TestHmacJwtGenerator.generateSignedJwt;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HmacSignedJwtValidatorTest {

    private HmacSignedJwtValidator validator;

    @Test
    public void testValidationFromProperty() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.PROPERTY;
        Properties config = new Properties();
        String secretKey = "969c55677e9f397060a21e4bbef1dcc9";
        config.setProperty(HMAC_SECRET_KEY_PROPERTY, secretKey);

        String jwtAsString = generateSignedJwt(JWSAlgorithm.HS256, null, "abigel", secretKey);

        // parse the JWS and verify its HMAC
        SignedJWT signedJWT = SignedJWT.parse(jwtAsString);

        validator = new HmacSignedJwtValidator(type, config, null);
        boolean result = validator.validateSignature(signedJWT);
        assertTrue(result);
    }

    @Test
    public void testValidationFromUrl() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.URL;

        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);

        Properties config = new Properties();
        String secretKey = "969c55677e9f397060a21e4bbef1dcc9";
        config.setProperty(HMAC_SECRET_KEY_PROPERTY, secretKey);

        String jwtAsString = generateSignedJwt(JWSAlgorithm.HS256, null, "marton", secretKey);

        String url = "https://my.auth.server";
        config.setProperty(HMAC_SECRET_KEY_URL, url);

        when(httpClient.readKeyFromUrl(any(), eq(url))).thenReturn(secretKey);

        SignedJWT signedJWT = SignedJWT.parse(jwtAsString);
        validator = new HmacSignedJwtValidator(type, config, httpClient);
        boolean result = validator.validateSignature(signedJWT);
        assertTrue(result);
    }
}
