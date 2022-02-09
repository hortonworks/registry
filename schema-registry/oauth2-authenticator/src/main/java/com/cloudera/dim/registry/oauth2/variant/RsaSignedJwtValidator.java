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
import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.JWSAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.KEY_ALGORITHM;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.PUBLIC_KEY_KEYSTORE;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.PUBLIC_KEY_KEYSTORE_ALIAS;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.PUBLIC_KEY_KEYSTORE_PASSWORD;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.PUBLIC_KEY_PROPERTY;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.PUBLIC_KEY_URL;

/**
 * <p>Validate JWT with a public key.</p>
 * <p>The public key can be located in one of the following storages:
 *   <ul>
 *       <li>property (eg. in registry.yaml)</li>
 *       <li>url (it would be downloaded from this URL)</li>
 *       <li>keystore</li>
 *   </ul>
 * </p>
 */
public class RsaSignedJwtValidator extends StoredKeyValidator {

    private static final Logger LOG = LoggerFactory.getLogger(RsaSignedJwtValidator.class);

    private final HttpClientForOAuth2 httpClient;

    public RsaSignedJwtValidator(JwtKeyStoreType keyStoreType, JWSAlgorithm algType, Properties config, @Nullable HttpClientForOAuth2 httpClient) throws ServletException {
        super(readPublicKey(keyStoreType, algType, config, httpClient), config);
        this.httpClient = httpClient;
    }

    @Nonnull
    private static PublicKey readPublicKey(@Nonnull JwtKeyStoreType keyStoreType, @Nullable JWSAlgorithm algType,
                                    Properties config, HttpClientForOAuth2 httpClient) throws ServletException {
        try {
            switch (keyStoreType) {
                case PROPERTY:
                case URL:
                    if (algType == null) {
                        throw new IllegalArgumentException("Please provide the algorithm with: " + KEY_ALGORITHM);
                    } else {
                        return parsePublicKey(config, keyStoreType, algType, httpClient);
                    }
                case KEYSTORE:
                    return readFromKeystore(config);
                default:
                    throw new IllegalArgumentException("Unsupported keystore type: " + keyStoreType);
            }
        } catch (RuntimeException slex) {
            throw slex;
        } catch (Exception ex) {
            throw new ServletException("Failed to read public key.", ex);
        }
    }

    @VisibleForTesting
    static PublicKey parsePublicKey(Properties config, JwtKeyStoreType keyStoreType, @Nonnull JWSAlgorithm certType,
                                    @Nullable HttpClientForOAuth2 httpClient) throws IOException {
        String result;
        switch (keyStoreType) {
            case PROPERTY:
                result = config.getProperty(PUBLIC_KEY_PROPERTY);
                break;
            case URL:
                if (httpClient == null) {
                    throw new IllegalArgumentException("HTTPClient is null.");
                }
                result = httpClient.readKeyFromUrl(config, config.getProperty(PUBLIC_KEY_URL));
                break;
            // store type KEYSTORE is handled elsewhere
            default:
                throw new RuntimeException("Unsupported keystore type: " + keyStoreType);
        }

        if (StringUtils.isBlank(result)) {
            throw new RuntimeException("Failed to read the key of type " + keyStoreType + ", empty string returned.");
        }

        if (certType == JWSAlgorithm.RS256 || certType == JWSAlgorithm.RS384 || certType == JWSAlgorithm.RS512) {
            return RsaUtil.parseRSAPublicKey(result);
        } else {
            throw new IllegalArgumentException("Unsupported certificate type: " + config.getProperty(KEY_ALGORITHM));
        }
    }

    @VisibleForTesting
    @Nonnull
    static PublicKey readFromKeystore(Properties config) throws KeyStoreException, IOException {
        String keystorePath = config.getProperty(PUBLIC_KEY_KEYSTORE);
        String ksAlias = config.getProperty(PUBLIC_KEY_KEYSTORE_ALIAS);
        String ksPassword = config.getProperty(PUBLIC_KEY_KEYSTORE_PASSWORD);

        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream in = new FileInputStream(keystorePath)) {
            ks.load(in, ksPassword.toCharArray());

            Certificate certificate = ks.getCertificate(ksAlias);
            if (certificate == null) {
                throw new RuntimeException("No certificate with alias " + ksAlias);
            }

            PublicKey publicKey = certificate.getPublicKey();
            if (publicKey == null) {
                throw new RuntimeException("Certificate did not contain a public key. Alias: " + ksAlias);
            }

            return publicKey;
        } catch (Exception ex) {
            throw new RuntimeException("Could not read from keystore.", ex);
        }
    }

    @Override
    public void close() {
        if (httpClient != null) {
            httpClient.close();
        }
    }

}
