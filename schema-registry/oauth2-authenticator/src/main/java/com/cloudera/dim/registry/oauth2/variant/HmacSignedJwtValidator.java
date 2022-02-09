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
import org.jose4j.base64url.Base64Url;
import org.jose4j.keys.AesKey;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import java.security.Key;
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
public class HmacSignedJwtValidator extends StoredKeyValidator {

    private final HttpClientForOAuth2 httpClient;

    public HmacSignedJwtValidator(JwtKeyStoreType keyStoreType, Properties config, @Nullable HttpClientForOAuth2 httpClient) throws ServletException {
        super(readHmacSecret(keyStoreType, config, httpClient), config);
        this.httpClient = httpClient;
    }

    private static Key readHmacSecret(JwtKeyStoreType keyStoreType, Properties config,
                                      @Nullable HttpClientForOAuth2 httpClient) throws ServletException {
        final String hmacSecret;
        switch (keyStoreType) {
            case PROPERTY:
                hmacSecret = config.getProperty(HMAC_SECRET_KEY_PROPERTY);
                break;
            case URL:
                try {
                    String url = config.getProperty(HMAC_SECRET_KEY_URL);
                    if (url == null) {
                        throw new IllegalArgumentException("Property is required: " + HMAC_SECRET_KEY_URL);
                    } else if (httpClient == null) {
                        throw new IllegalArgumentException("HTTP Client was not created.");
                    }
                    hmacSecret = httpClient.readKeyFromUrl(config, url);
                } catch (Exception ex) {
                    throw new ServletException("Failed to download secret key from URL.", ex);
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported store type for HMAC: " + keyStoreType);
        }

        Base64Url base64Url = new Base64Url();
        byte[] octetSequence = base64Url.base64UrlDecode(hmacSecret);
        return new AesKey(octetSequence);
    }

    @Override
    public void close() {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
