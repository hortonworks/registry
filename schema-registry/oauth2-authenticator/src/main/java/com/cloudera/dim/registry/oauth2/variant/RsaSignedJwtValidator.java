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
import com.cloudera.dim.registry.oauth2.JwtCertificateType;
import com.cloudera.dim.registry.oauth2.JwtKeyStoreType;
import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.*;

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
public class RsaSignedJwtValidator implements JwtValidatorVariant {

    private static final Logger LOG = LoggerFactory.getLogger(RsaSignedJwtValidator.class);

    private final HttpClientForOAuth2 httpClient;
    private final PublicKey publicKey;

    public RsaSignedJwtValidator(JwtKeyStoreType keyStoreType, JwtCertificateType certType, Properties config, @Nullable HttpClientForOAuth2 httpClient) throws ServletException {
        this.httpClient = httpClient;

        publicKey = readPublicKey(keyStoreType, certType, config);
    }

    public boolean validateSignature(SignedJWT jwtToken) {
        boolean valid = false;
        if (JWSObject.State.SIGNED == jwtToken.getState()) {
            LOG.debug("JWT token is in a SIGNED state");
            if (jwtToken.getSignature() != null) {
                LOG.debug("JWT token signature is not null");
                if (publicKey == null) {
                    throw new RuntimeException("Public key is null, cannot verify signature.");
                }
                try {
                    JWSVerifier verifier = new RSASSAVerifier((RSAPublicKey) publicKey);
                    if (jwtToken.verify(verifier)) {
                        valid = true;
                        LOG.debug("JWT token has been successfully verified");
                    } else {
                        LOG.warn("JWT signature verification failed.");
                    }
                } catch (JOSEException je) {
                    LOG.warn("Error while validating signature", je);
                }
            }
        }
        return valid;
    }

    @Nonnull
    private PublicKey readPublicKey(@Nonnull JwtKeyStoreType keyStoreType, @Nullable JwtCertificateType certType,
                                    Properties config) throws ServletException {
        try {
            switch (keyStoreType) {
                case PROPERTY:
                case URL:
                    if (certType == null) {
                        throw new IllegalArgumentException("Please provide the algorithm with: " + KEY_ALGORITHM);
                    } else {
                        return parsePublicKey(config, keyStoreType, certType);
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
    PublicKey parsePublicKey(Properties config, JwtKeyStoreType keyStoreType, @Nonnull JwtCertificateType certType) throws IOException {
        String result;
        switch (keyStoreType) {
            case PROPERTY:
                result = config.getProperty(PUBLIC_KEY_PROPERTY);
                break;
            case URL:
                result = httpClient.readKeyFromUrl(config, config.getProperty(PUBLIC_KEY_URL));
                break;
            // store type KEYSTORE is handled elsewhere
            default:
                throw new RuntimeException("Unsupported keystore type: " + keyStoreType);
        }

        if (StringUtils.isBlank(result)) {
            throw new RuntimeException("Failed to read the key of type " + keyStoreType + ", empty string returned.");
        }

        switch (certType) {
            case RSA:
                result = result
                        .replaceAll("-----BEGIN PUBLIC KEY-----", "")
                        .replaceAll("-----END PUBLIC KEY-----", "")
                        .replaceAll("\n", "");
                return parseRSAPublicKey(result);
            default:
                throw new IllegalArgumentException("Unsupported certificate type: " + config.getProperty(KEY_ALGORITHM));
        }
    }

    @VisibleForTesting
    @Nonnull
    PublicKey readFromKeystore(Properties config) throws KeyStoreException, IOException {
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

    @Nonnull
    private RSAPublicKey parseRSAPublicKey(String publicKeyText) {
        try {

            byte[] decoded = Base64.getDecoder().decode(publicKeyText);

            X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            RSAPublicKey result = (RSAPublicKey) kf.generatePublic(spec);
            if (result == null) {
                throw new RuntimeException("Could not generate public RSA key.");
            }
            return result;
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ce) {
            throw new RuntimeException("Failed to parse certificate.", ce);
        }
    }

}
