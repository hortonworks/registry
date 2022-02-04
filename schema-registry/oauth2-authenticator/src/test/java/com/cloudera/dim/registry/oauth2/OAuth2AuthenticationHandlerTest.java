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

import com.hortonworks.registries.auth.server.AuthenticationToken;
import com.nimbusds.jose.JWSAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler.AUTHORIZATION;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.HMAC_SECRET_KEY_PROPERTY;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.KEY_ALGORITHM;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.KEY_STORE_TYPE;
import static com.cloudera.dim.registry.oauth2.TestHmacJwtGenerator.generateSignedJwt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OAuth2AuthenticationHandlerTest {

    private OAuth2AuthenticationHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new OAuth2AuthenticationHandler();
    }

    @Test
    public void testHmac() throws Exception {
        final String subject = "marton";
        Properties config = new Properties();
        config.setProperty(KEY_STORE_TYPE, JwtKeyStoreType.PROPERTY.getValue());
        config.setProperty(KEY_ALGORITHM, JWSAlgorithm.HS256.getName());
        String secretKey = "969c55677e9f397060a21e4bbef1dcc9";
        config.setProperty(HMAC_SECRET_KEY_PROPERTY, secretKey);

        handler.init(config);

        String jwt = generateSignedJwt(JWSAlgorithm.HS256, null, subject, secretKey);

        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(request.getHeader(AUTHORIZATION)).thenReturn("Bearer " + jwt);

        AuthenticationToken token = handler.authenticate(request, response);
        assertNotNull(token);
        assertEquals(subject, token.getUserName());

    }

}
