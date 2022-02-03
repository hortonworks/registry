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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler.PUBLIC_KEY_KEYSTORE;
import static com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler.PUBLIC_KEY_KEYSTORE_ALIAS;
import static com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler.PUBLIC_KEY_KEYSTORE_PASSWORD;
import static com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler.PUBLIC_KEY_PROPERTY;
import static com.cloudera.dim.registry.oauth2.OAuth2AuthenticationHandler.PUBLIC_KEY_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// check README.md for details how the keys and the keystore were created
public class OAuth2AuthenticationHandlerTest {

    private OAuth2AuthenticationHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new OAuth2AuthenticationHandler();
    }

    @Test
    public void testReadCertFromProperty() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.PROPERTY;
        Properties config = new Properties();
        String value;
        try (InputStream in = getClass().getResourceAsStream("/test.pub")) {
            assertNotNull(in, "Failed to read public key");
            value = IOUtils.toString(in, StandardCharsets.UTF_8);
        }
        config.setProperty(PUBLIC_KEY_PROPERTY, value);

        PublicKey result = handler.parseKey(config, type, JwtCertificateType.RSA);

        assertNotNull(result);
        assertEquals("RSA", result.getAlgorithm());
    }

    @Test
    public void testReadCertFromUrl() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.URL;

        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);
        handler.setHttpClient(httpClient);

        Properties config = new Properties();
        final String value;
        try (InputStream in = getClass().getResourceAsStream("/test.pub")) {
            assertNotNull(in, "Failed to read public key");
            value = IOUtils.toString(in, StandardCharsets.UTF_8);
        }

        String url = "https://my.auth.server";
        config.setProperty(PUBLIC_KEY_URL, url);

        when(httpClient.download(eq(new URL(url)), anyString(), any())).thenReturn(value);

        PublicKey result = handler.parseKey(config, type, JwtCertificateType.RSA);

        assertNotNull(result);
        assertEquals("RSA", result.getAlgorithm());
    }

    @Test
    public void testReadCertFromKeystore() throws Exception {
        Properties config = new Properties();
        String keystorePath = getKeystorePath().getAbsolutePath();
        String ktAlias = "oauth";
        String ksPassword = "test123";

        config.setProperty(PUBLIC_KEY_KEYSTORE, keystorePath);
        config.setProperty(PUBLIC_KEY_KEYSTORE_ALIAS, ktAlias);
        config.setProperty(PUBLIC_KEY_KEYSTORE_PASSWORD, ksPassword);

        PublicKey publicKey = handler.readFromKeystore(config);

        assertNotNull(publicKey);
        assertEquals("RSA", publicKey.getAlgorithm());
    }

    private File getKeystorePath() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/testkeystore.jks")) {
            byte[] bytes = IOUtils.toByteArray(is);
            File tmpFile = File.createTempFile("kys", "jks");
            try (FileOutputStream out = new FileOutputStream(tmpFile)) {
                IOUtils.write(bytes, out);
            }
            tmpFile.deleteOnExit();
            return tmpFile;
        }
    }
}
