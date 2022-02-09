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

import com.nimbusds.jose.JWSAlgorithm;
import org.apache.commons.io.IOUtils;
import org.jose4j.base64url.Base64Url;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;
import org.jose4j.lang.JoseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Generate JWT tokens signed with RSA or HMAC. */
public class TestJwtGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TestJwtGenerator.class);

    public static String generateSignedJwt(JWSAlgorithm alg, @Nullable String kid, String subject, String secretKey) throws Exception {
        return generateSignedJwt(alg, kid, subject, secretKey, null);
    }

    public static String generateSignedJwt(JWSAlgorithm alg, @Nullable String kid, String subject, String secretKey,
                                           @Nullable Map<String, String> customClaims) throws Exception {
        if (JWSAlgorithm.HS256.equals(alg) || JWSAlgorithm.HS384.equals(alg) || JWSAlgorithm.HS512.equals(alg)) {
            return generateHmacSignedJwt(alg, kid, subject, secretKey, customClaims);
        }
        return generateRsaSignedJwt(alg, kid, subject, customClaims);
    }

    public static String generateSignedJwt(JWSAlgorithm alg, @Nullable String kid, String subject) throws Exception {
        return generateSignedJwt(alg, kid, subject, (Map<String, String>) null);
    }

    public static String generateSignedJwt(JWSAlgorithm alg, @Nullable String kid, String subject,
                                           @Nullable Map<String, String> customClaims) throws Exception {
        return generateRsaSignedJwt(alg, kid, subject, customClaims);
    }

    private static String generateHmacSignedJwt(JWSAlgorithm alg, @Nullable String kid, String subject,
                                                String secretKey, @Nullable Map<String, String> customClaims) throws JoseException {

        Base64Url base64Url = new Base64Url();
        byte[] octetSequence = base64Url.base64UrlDecode(secretKey);

        Key key = new HmacKey(octetSequence);

        String jwt = generateJwt(alg, kid, subject, customClaims, key);

        // validate right now to ensure we're generating a valid jwt
        JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder()
                .setAllowedClockSkewInSeconds(30)
                .setRequireExpirationTime()
                .setRequireSubject()
                .setVerificationKey(key)
                .setJwsAlgorithmConstraints(AlgorithmConstraints.ConstraintType.PERMIT, alg.getName());

        if (customClaims != null && customClaims.containsKey("aud")) {
            jwtConsumerBuilder.setExpectedAudience(customClaims.get("aud"));
        }
        if (customClaims != null && customClaims.containsKey("iss")) {
            jwtConsumerBuilder.setExpectedAudience(customClaims.get("iss"));
        }

        JwtConsumer jwtConsumer = jwtConsumerBuilder.build();

        try {
            jwtConsumer.processToClaims(jwt);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        LOG.info("Generated {} signed JWT: {}", alg.getName(), jwt);
        return jwt;
    }


    private static String generateRsaSignedJwt(JWSAlgorithm alg, @Nullable String kid, String subject,
                                               @Nullable Map<String, String> customClaims) throws Exception {
        String rsaPrivKeyTxt;
        try (InputStream in = TestJwtGenerator.class.getResourceAsStream("/test.key.pcks8")) {
            rsaPrivKeyTxt = IOUtils.toString(in, StandardCharsets.UTF_8);
        }

        RSAPrivateKey rsaPrivateKey = readPrivateKey(rsaPrivKeyTxt);
        assertNotNull(rsaPrivateKey);

        String jwt = generateJwt(alg, kid, subject, customClaims, rsaPrivateKey);

        LOG.info("Generated {} signed JWT: {}", alg.getName(), jwt);
        return jwt;
    }

    private static String generateJwt(JWSAlgorithm alg, String kid, String subject, Map<String, String> customClaims, Key rsaPrivateKey) throws JoseException {
        // Create the Claims, which will be the content of the JWT
        JwtClaims claims = new JwtClaims();
        claims.setExpirationTimeMinutesInTheFuture(5);
        claims.setGeneratedJwtId();
        claims.setIssuedAtToNow();
        claims.setSubject(subject);
        if (customClaims != null && !customClaims.isEmpty()) {
            customClaims.forEach(claims::setClaim);
        }

        // sign the JWT
        JsonWebSignature jws = new JsonWebSignature();
        jws.setPayload(claims.toJson());
        jws.setKey(rsaPrivateKey);
        if (kid != null) {
            jws.setKeyIdHeaderValue(kid);
        }
        jws.setAlgorithmHeaderValue(alg.getName());

        // this is the result
        String jwt = jws.getCompactSerialization();
        return jwt;
    }

    private static RSAPrivateKey readPrivateKey(String privateKeyTxt) {
        try {
            privateKeyTxt = privateKeyTxt
                    .replaceAll("-----BEGIN PRIVATE KEY-----", "")
                    .replaceAll("-----END PRIVATE KEY-----", "")
                    .replaceAll("\n", "");

            byte[] decoded = Base64.getDecoder().decode(privateKeyTxt);

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");

            EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(decoded);
            return (RSAPrivateKey) keyFactory.generatePrivate(privateKeySpec);
        } catch (Exception ex) {
            throw new RuntimeException("Error reading the private key.", ex);
        }
    }

    private TestJwtGenerator() { }
}
