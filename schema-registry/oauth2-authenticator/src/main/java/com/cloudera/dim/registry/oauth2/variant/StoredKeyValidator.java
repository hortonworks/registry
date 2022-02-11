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

import com.nimbusds.jwt.SignedJWT;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Key;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.OAuth2Config.CLOCK_SKEW;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.EXPECTED_JWT_AUDIENCES;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.EXPECTED_JWT_ISSUER;
import static org.jose4j.jwa.AlgorithmConstraints.DISALLOW_NONE;

abstract class StoredKeyValidator implements JwtValidatorVariant {

    private static final Logger LOG = LoggerFactory.getLogger(StoredKeyValidator.class);

    private final Key key;
    private final Integer clockSkew;
    private final String[] expectedAudiences;
    private final String expectedIssuer;

    protected StoredKeyValidator(Key key, Properties config) {
        this.key = key;
        this.clockSkew = config.containsKey(CLOCK_SKEW) ? Integer.parseInt(config.getProperty(CLOCK_SKEW)) : null;
        this.expectedAudiences = config.containsKey(EXPECTED_JWT_AUDIENCES) ? ((String) config.get(EXPECTED_JWT_AUDIENCES)).split("\\n") : null;
        this.expectedIssuer = config.containsKey(EXPECTED_JWT_ISSUER) ? (String) config.get(EXPECTED_JWT_ISSUER) : null;
    }

    public boolean validateSignature(SignedJWT jwtToken) {
        try {
            final JwtConsumerBuilder jwtConsumerBuilder = new JwtConsumerBuilder();

            if (clockSkew != null) {
                jwtConsumerBuilder.setAllowedClockSkewInSeconds(clockSkew);
            }
            if (expectedAudiences != null && expectedAudiences.length > 0) {
                jwtConsumerBuilder.setExpectedAudience(expectedAudiences);
            }

            if (expectedIssuer != null) {
                jwtConsumerBuilder.setExpectedIssuer(expectedIssuer);
            }

            JwtConsumer jwtConsumer = jwtConsumerBuilder
                    .setJwsAlgorithmConstraints(DISALLOW_NONE)
                    .setRequireExpirationTime()
                    .setRequireIssuedAt()
                    .setRequireSubject()
                    .setVerificationKey(key)
                    .build();

            JwtContext jwtContext = jwtConsumer.process(jwtToken.getParsedString());
            if (jwtContext == null) {
                throw new RuntimeException("Could not validate JWT.");
            }

            return true;
        } catch (Exception ex) {
            LOG.warn("Failed to verify HMAC signature of the JWT.", ex);
        }
        return false;
    }

}
