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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

class RsaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RsaUtil.class);

    @Nonnull
    public static RSAPublicKey parseRSAPublicKey(@Nonnull String publicKeyText) {
        try {
            publicKeyText = publicKeyText
                .replaceAll("-----BEGIN PUBLIC KEY-----", "")
                .replaceAll("-----END PUBLIC KEY-----", "")
                .replaceAll("\n", "");

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

    public static boolean validateSignature(RSAPublicKey publicKey, SignedJWT jwtToken) {
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

    private RsaUtil() { }
}
