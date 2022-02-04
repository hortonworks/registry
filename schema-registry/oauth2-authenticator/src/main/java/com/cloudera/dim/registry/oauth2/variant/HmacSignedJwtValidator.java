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
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.SignedJWT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.HMAC_SECRET_KEY_PROPERTY;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.HMAC_SECRET_KEY_URL;

/**
 * <p>Validate JWT with a HMAC secret key.</p>
 * <p>The secret key can be located in one of the following storages:
 *   <ul>
 *       <li>property (eg. in registry.yaml)</li>
 *       <li>url (it would be downloaded from this URL)</li>
 *   </ul>
 * </p>
 */
public class HmacSignedJwtValidator implements JwtValidatorVariant {

    private static final Logger LOG = LoggerFactory.getLogger(HmacSignedJwtValidator.class);

    private final HttpClientForOAuth2 httpClient;
    private final String hmacSecret;

    public HmacSignedJwtValidator(JwtKeyStoreType keyStoreType, Properties config, @Nullable HttpClientForOAuth2 httpClient) throws ServletException {
        this.httpClient = httpClient;

        hmacSecret = readHmacSecret(keyStoreType, config);
        if (hmacSecret == null) {
            throw new RuntimeException("HMAC secret key is null.");
        }
    }

    private String readHmacSecret(JwtKeyStoreType keyStoreType, Properties config) throws ServletException {
        switch (keyStoreType) {
            case PROPERTY:
                return config.getProperty(HMAC_SECRET_KEY_PROPERTY);
            case URL:
                try {
                    String url = config.getProperty(HMAC_SECRET_KEY_URL);
                    if (url == null) {
                        throw new IllegalArgumentException("Property is required: " + HMAC_SECRET_KEY_URL);
                    }
                    return httpClient.readKeyFromUrl(config, url);
                } catch (Exception ex) {
                    throw new ServletException("Failed to download secret key from URL.", ex);
                }
            default:
                throw new IllegalArgumentException("Unsupported store type for HMAC: " + keyStoreType);
        }
    }

    @Override
    public boolean validateSignature(SignedJWT jwtToken) {
        try {
            JWSVerifier verifier = new MACVerifier(hmacSecret);
            return jwtToken.verify(verifier);
        } catch (Exception ex) {
            LOG.warn("Failed to verify HMAC signature of the JWT.", ex);
        }
        return false;
    }
}
