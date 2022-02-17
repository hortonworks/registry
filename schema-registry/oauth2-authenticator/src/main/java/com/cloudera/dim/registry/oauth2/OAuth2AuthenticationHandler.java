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

import com.cloudera.dim.registry.oauth2.variant.HmacSignedJwtValidator;
import com.cloudera.dim.registry.oauth2.variant.JwkValidator;
import com.cloudera.dim.registry.oauth2.variant.JwtValidatorVariant;
import com.cloudera.dim.registry.oauth2.variant.RsaSignedJwtValidator;
import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.server.AuthenticationHandler;
import com.hortonworks.registries.auth.server.AuthenticationToken;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.cloudera.dim.registry.oauth2.HttpClientForOAuth2.PROPERTY_PREFIX;
import static com.cloudera.dim.registry.oauth2.OAuth2Config.*;

public class OAuth2AuthenticationHandler implements AuthenticationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth2AuthenticationHandler.class);

    public static final String TYPE = "oauth2";
    public static final String AUTHORIZATION = "Authorization";

    private String bearerPrefix = "Bearer ";
    private String jwtPrincipalClaim = "sub";
    private List<String> audiences = null;
    private HttpClientForOAuth2 httpClient;
    private JwtValidatorVariant handlerImpl;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties config) throws ServletException {
        LOG.info("Initializing OAuth2 based authentication ...");
        if (handlerImpl == null) {
            JwtKeyStoreType keyStoreType = JwtKeyStoreType.parseString(config.getProperty(KEY_STORE_TYPE));
            if (keyStoreType == null) {
                throw new RuntimeException("Property is required: " + KEY_STORE_TYPE);
            }
            if (httpClient == null && (keyStoreType == JwtKeyStoreType.URL || keyStoreType == JwtKeyStoreType.JWK)) {
                httpClient = initHttpClient(config);
            }

            if (keyStoreType == JwtKeyStoreType.JWK) {
                handlerImpl = new JwkValidator(config, httpClient);
            } else {
                if (StringUtils.isBlank(config.getProperty(KEY_ALGORITHM))) {
                    throw new IllegalArgumentException("Property is required: " + KEY_ALGORITHM);
                }
                JWSAlgorithm algType = JWSAlgorithm.parse(config.getProperty(KEY_ALGORITHM));

                if (algType == JWSAlgorithm.HS256 || algType == JWSAlgorithm.HS384 || algType == JWSAlgorithm.HS512) {
                    handlerImpl = new HmacSignedJwtValidator(keyStoreType, config, httpClient);
                } else {
                    handlerImpl = new RsaSignedJwtValidator(keyStoreType, algType, config, httpClient);
                }
            }
        }

        if (config.containsKey(HEADER_PREFIX)) {
            bearerPrefix = config.getProperty(HEADER_PREFIX);
            if (bearerPrefix == null) {
                bearerPrefix = "";
            }
        }

        if (config.containsKey(JWT_PRINCIPAL_CLAIM)) {
            jwtPrincipalClaim = config.getProperty(JWT_PRINCIPAL_CLAIM);
            if (jwtPrincipalClaim == null) {
                jwtPrincipalClaim = "sub";
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

    private HttpClientForOAuth2 initHttpClient(Properties config) {
        Map<String, String> clientSettings = new HashMap<>();
        Enumeration<?> names = config.propertyNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            if (name.startsWith(PROPERTY_PREFIX)) {
                String value = config.getProperty(name);
                clientSettings.put(name.substring(PROPERTY_PREFIX.length()), value);
            }
        }

        return new HttpClientForOAuth2(clientSettings);
    }

    @Override
    public void destroy() {
        if (handlerImpl != null) {
            handlerImpl.close();
        }
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
        SignedJWT jwtToken;
        boolean valid = false;
        Long exp = null;
        try {
            jwtToken = SignedJWT.parse(jwt);
            valid = validateToken(jwtToken);
            if (valid) {
                if (jwtPrincipalClaim == null || jwtPrincipalClaim.equals("sub")) {
                    userName = jwtToken.getJWTClaimsSet().getSubject();
                } else {
                    userName = jwtToken.getJWTClaimsSet().toJSONObject().getAsString(jwtPrincipalClaim);
                }
                LOG.debug("USERNAME: " + userName);
            } else {
                LOG.warn("jwtToken failed validation: " + jwtToken.serialize());
            }
            Date expirationTime = jwtToken.getJWTClaimsSet().getExpirationTime();
            if (expirationTime.toInstant().isBefore(Instant.now())) {
                LOG.debug("JWT token has expired.");
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
        boolean sigValid = handlerImpl.validateSignature(jwtToken);
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