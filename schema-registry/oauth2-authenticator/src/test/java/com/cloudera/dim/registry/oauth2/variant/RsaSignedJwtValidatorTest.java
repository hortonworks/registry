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
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// check README.md for details how the keys and the keystore were created
public class RsaSignedJwtValidatorTest {

    private RsaSignedJwtValidator validator;

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

        validator = new RsaSignedJwtValidator(type, JWSAlgorithm.RS256, config, null);
        PublicKey result = validator.parsePublicKey(config, type, JWSAlgorithm.RS256, null);

        assertNotNull(result);
        assertEquals("RSA", result.getAlgorithm());
    }

    @Test
    public void testReadCertFromUrl() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.URL;

        HttpClientForOAuth2 httpClient = mock(HttpClientForOAuth2.class);

        Properties config = new Properties();
        final String value;
        try (InputStream in = getClass().getResourceAsStream("/test.pub")) {
            assertNotNull(in, "Failed to read public key");
            value = IOUtils.toString(in, StandardCharsets.UTF_8);
        }

        String url = "https://my.auth.server";
        config.setProperty(PUBLIC_KEY_URL, url);

        when(httpClient.readKeyFromUrl(any(), eq(url))).thenReturn(value);

        validator = new RsaSignedJwtValidator(type, JWSAlgorithm.RS256, config, httpClient);
        PublicKey result = validator.parsePublicKey(config, type, JWSAlgorithm.RS256, httpClient);

        assertNotNull(result);
        assertEquals("RSA", result.getAlgorithm());
    }

    @Test
    public void testReadCertFromKeystore() throws Exception {
        JwtKeyStoreType type = JwtKeyStoreType.KEYSTORE;

        Properties config = new Properties();
        String keystorePath = getKeystorePath().getAbsolutePath();
        String ktAlias = "oauth";
        String ksPassword = "test123";

        config.setProperty(PUBLIC_KEY_KEYSTORE, keystorePath);
        config.setProperty(PUBLIC_KEY_KEYSTORE_ALIAS, ktAlias);
        config.setProperty(PUBLIC_KEY_KEYSTORE_PASSWORD, ksPassword);

        validator = new RsaSignedJwtValidator(type, JWSAlgorithm.RS256, config, null);
        PublicKey publicKey = validator.readFromKeystore(config);

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
