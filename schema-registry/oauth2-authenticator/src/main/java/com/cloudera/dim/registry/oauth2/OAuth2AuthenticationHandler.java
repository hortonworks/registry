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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.server.AuthenticationHandler;
import com.hortonworks.registries.auth.server.AuthenticationToken;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class OAuth2AuthenticationHandler implements AuthenticationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth2AuthenticationHandler.class);

    private static final String AUTHORIZATION = "Authorization";
    public static final String PUBLIC_KEY_ALGORITHM = "public.key.algorithm";
    // url, property, keystore
    public static final String PUBLIC_KEY_STORE_TYPE = "public.key.store.type";
    public static final String PUBLIC_KEY_URL = "public.key.url";
    public static final String PUBLIC_KEY_PROPERTY = "public.key.property";
    public static final String PUBLIC_KEY_KEYSTORE = "public.key.keystore";
    public static final String PUBLIC_KEY_KEYSTORE_ALIAS = "public.key.keystore.alias";
    public static final String PUBLIC_KEY_KEYSTORE_PASSWORD = "public.key.keystore.password";
    public static final String HEADER_PREFIX = "header.prefix";
    public static final String EXPECTED_JWT_AUDIENCES = "expected.jwt.audiences";

    private PublicKey publicKey = null;
    private String bearerPrefix = "Bearer ";
    private List<String> audiences = null;
    private JwtCertificateType certType;

    @Override
    public String getType() {
        return "oauth2";
    }

    @Override
    public void init(Properties config) throws ServletException {
        LOG.info("Initializing OAuth2 based authentication ...");
        if (publicKey == null) {
            certType = JwtCertificateType.parseString(config.getProperty(PUBLIC_KEY_ALGORITHM, JwtCertificateType.RSA.getValue()));
            publicKey = readPublicKey(certType, config);
        }

        if (config.containsKey(HEADER_PREFIX)) {
            bearerPrefix = config.getProperty(HEADER_PREFIX);
            if (bearerPrefix == null) {
                bearerPrefix = "";
            }
        }

        // setup the list of valid audiences for token validation
        String auds = config.getProperty(EXPECTED_JWT_AUDIENCES);
        if (auds != null) {
            // parse into the list
            String[] audArray = auds.split(",");
            audiences = new ArrayList<>();
            for (String a : audArray) {
                audiences.add(a);
            }
        }
    }

    @Nonnull
    private PublicKey readPublicKey(@Nullable JwtCertificateType certType, Properties config) throws ServletException {
        try {
            JwtKeyStoreType keyStoreType = JwtKeyStoreType.parseString(config.getProperty(PUBLIC_KEY_STORE_TYPE));
            if (keyStoreType == null) {
                throw new RuntimeException("Property is required: " + PUBLIC_KEY_STORE_TYPE);
            }

            switch (keyStoreType) {
                case PROPERTY:
                case URL:
                    if (certType == null) {
                        throw new IllegalArgumentException("Please provide the algorithm with: " + PUBLIC_KEY_ALGORITHM);
                    } else {
                        return parseKey(config, keyStoreType, certType);
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
    PublicKey parseKey(Properties config, JwtKeyStoreType keyStoreType, @Nonnull JwtCertificateType certType) throws IOException {
        String result;
        switch (keyStoreType) {
            case PROPERTY:
                result = config.getProperty(PUBLIC_KEY_PROPERTY);
                break;
            case URL:
                // TODO CDPD-34171
                String url = config.getProperty(PUBLIC_KEY_URL);
                if (StringUtils.isBlank(url)) {
                    throw new RuntimeException("Property is required: " + PUBLIC_KEY_URL);
                }
                URL keyUrl = new URL(url);
                result = IOUtils.toString(keyUrl, StandardCharsets.UTF_8.name());
                break;
            // store type KEYSTORE is handled elsewhere
            default:
                throw new RuntimeException("Unsupported keystore type: " + keyStoreType);
        }

        if (StringUtils.isBlank(result)) {
            throw new RuntimeException("Failed to read the key of type " + keyStoreType);
        }

        result = result
                .replaceAll("-----BEGIN PUBLIC KEY-----", "")
                .replaceAll("-----END PUBLIC KEY-----", "")
                .replaceAll("\n", "");

        switch (certType) {
            case RSA:
                return parseRSAPublicKey(result);
            case HMAC:
                // certType
                throw new RuntimeException("Not implemented yet"); // TODO CDPD-34174
            default:
                throw new IllegalArgumentException("Unsupported certificate type: " + config.getProperty(PUBLIC_KEY_ALGORITHM));
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

    @Override
    public void destroy() { }

    @Override
    public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
        return true;
    }

    @Override
    public boolean shouldAuthenticate(HttpServletRequest request) {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
        String authHeader = StringUtils.trimToNull(request.getHeader(AUTHORIZATION));
        if (authHeader == null || authHeader.length() <= bearerPrefix.length()) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }

        AuthenticationToken token = null;
        try {
            token = validateJwt(authHeader);
        } catch (Exception ex) {
            LOG.error("Failed to validate auth token.", ex);
        }
        if (token == null) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }

        return token;
    }

    @Nullable
    private AuthenticationToken validateJwt(@Nonnull String authHeader) {
        String jwt = StringUtils.trimToNull(authHeader.substring(bearerPrefix.length()));
        if (jwt == null) {
            return null;
        }

        String userName = null;
        SignedJWT jwtToken = null;
        boolean valid = false;
        Long exp = null;
        try {
            jwtToken = SignedJWT.parse(jwt);
            valid = validateToken(jwtToken);
            if (valid) {
                userName = jwtToken.getJWTClaimsSet().getSubject();
                LOG.info("USERNAME: " + userName);
            } else {
                LOG.warn("jwtToken failed validation: " + jwtToken.serialize());
            }
            Date expirationTime = jwtToken.getJWTClaimsSet().getExpirationTime();
            if (expirationTime.toInstant().isBefore(Instant.now())) {
                LOG.info("JWT token has expired.");
                return null;
            }
            exp = expirationTime.getTime();
        } catch (ParseException pe) {
            // unable to parse the token let's try and get another one
            LOG.warn("Unable to parse the JWT token", pe);
        }

        if (valid) {
            LOG.debug("Issuing AuthenticationToken for user.");
            AuthenticationToken result = new AuthenticationToken(userName, userName, getType());
            if (exp != null) {
                result.setExpires(exp);
            }
            return result;
        }

        return null;
    }

    /**
     * This method provides a single method for validating the JWT for use in
     * request processing. It provides for the override of specific aspects of
     * this implementation through submethods used within but also allows for the
     * override of the entire token validation algorithm.
     *
     * @param jwtToken the token to validate
     * @return true if valid
     */
    protected boolean validateToken(SignedJWT jwtToken) {
        boolean sigValid = validateSignature(jwtToken);
        if (!sigValid) {
            LOG.warn("Signature could not be verified");
        }
        boolean audValid = validateAudiences(jwtToken);
        if (!audValid) {
            LOG.warn("Audience validation failed.");
        }
        boolean expValid = validateExpiration(jwtToken);
        if (!expValid) {
            LOG.info("Expiration validation failed.");
        }

        return sigValid && audValid && expValid;
    }

    /**
     * Verify the signature of the JWT token in this method. This method depends
     * on the public key that was established during init based upon the
     * provisioned public key. Override this method in subclasses in order to
     * customize the signature verification behavior.
     *
     * @param jwtToken the token that contains the signature to be validated
     * @return valid true if signature verifies successfully; false otherwise
     */
    private boolean validateSignature(SignedJWT jwtToken) {
        boolean valid = false;
        if (JWSObject.State.SIGNED == jwtToken.getState()) {
            LOG.debug("JWT token is in a SIGNED state");
            if (jwtToken.getSignature() != null) {
                LOG.debug("JWT token signature is not null");
                if (publicKey == null) {
                    throw new RuntimeException("Public key is null, cannot verify signature.");
                }
                switch (certType) {
                    case RSA:
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
                        break;
                    case HMAC:
                        valid = false;  // TODO CDPD-34174
                        break;
                    default:
                        valid = false;  // TODO
                        break;
                }
            }
        }
        return valid;
    }

    /**
     * Validate whether any of the accepted audience claims is present in the
     * issued token claims list for audience. Override this method in subclasses
     * in order to customize the audience validation behavior.
     *
     * @param jwtToken
     *          the JWT token where the allowed audiences will be found
     * @return true if an expected audience is present, otherwise false
     */
    private boolean validateAudiences(SignedJWT jwtToken) {
        boolean valid = false;
        try {
            List<String> tokenAudienceList = jwtToken.getJWTClaimsSet()
                    .getAudience();
            // if there were no expected audiences configured then just
            // consider any audience acceptable
            if (audiences == null) {
                valid = true;
            } else {
                // if any of the configured audiences is found then consider it
                // acceptable
                boolean found = false;
                for (String aud : tokenAudienceList) {
                    if (audiences.contains(aud)) {
                        LOG.debug("JWT token audience has been successfully validated");
                        valid = true;
                        break;
                    }
                }
                if (!valid) {
                    LOG.warn("JWT audience validation failed.");
                }
            }
        } catch (ParseException pe) {
            LOG.warn("Unable to parse the JWT token.", pe);
        }
        return valid;
    }

    /**
     * Validate that the expiration time of the JWT token has not been violated.
     * If it has then throw an AuthenticationException. Override this method in
     * subclasses in order to customize the expiration validation behavior.
     *
     * @param jwtToken the token that contains the expiration date to validate
     * @return valid true if the token has not expired; false otherwise
     */
    private boolean validateExpiration(SignedJWT jwtToken) {
        boolean valid = false;
        try {
            Date expires = jwtToken.getJWTClaimsSet().getExpirationTime();
            if (expires == null || new Date().before(expires)) {
                LOG.debug("JWT token expiration date has been "
                        + "successfully validated");
                valid = true;
            } else {
                LOG.warn("JWT expiration date validation failed.");
            }
        } catch (ParseException pe) {
            LOG.warn("JWT expiration date validation failed.", pe);
        }
        return valid;
    }

}