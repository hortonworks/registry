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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

class RsaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RsaUtil.class);

    @Nonnull
    public static RSAPublicKey parseRSAPublicKey(@Nonnull String publicKeyText) {
        // The input can be a public key OR a certificate. Attempt to guess what we got.
        boolean isPublicKey = publicKeyText.contains("-----BEGIN PUBLIC KEY-----");

        publicKeyText = publicKeyText
            .replaceAll("-----BEGIN PUBLIC KEY-----", "")
            .replaceAll("-----END PUBLIC KEY-----", "")
            .replaceAll("\n", "");

        byte[] decoded = Base64.getDecoder().decode(publicKeyText);

        RSAPublicKey result;
        if (isPublicKey) {
            result = readBytesAsPublicKey(decoded);
            if (result == null) {
                result = readBytesAsX509Certificate(decoded);
            }
        } else {
            result = readBytesAsX509Certificate(decoded);
            if (result == null) {
                result = readBytesAsPublicKey(decoded);
            }
        }

        if (result == null) {
            throw new RuntimeException("Could not generate public RSA key.");
        }

        return result;
    }
    private static RSAPublicKey readBytesAsX509Certificate(byte[] decoded) {
        try {
            InputStream certstream = new ByteArrayInputStream(decoded);
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(certstream);
            return (RSAPublicKey) cert.getPublicKey();
        } catch (Exception ex) {
            LOG.warn("Failed to parse certificate: {}", ex.toString());
        }
        return null;
    }

    private static RSAPublicKey readBytesAsPublicKey(byte[] decoded) {
        try {
            X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (Exception ex) {
            LOG.warn("Failed to parse public key: {}", ex.toString());
        }
        return null;
    }

    private RsaUtil() { }
}
