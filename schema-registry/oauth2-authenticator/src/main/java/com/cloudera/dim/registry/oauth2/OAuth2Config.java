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

public final class OAuth2Config {

    // -------------------------------- common properties ---------------------

    /** Authorization header prefix, default: "Bearer " (<b>Note:</b> make sure to include the trailing space if required) */
    public static final String HEADER_PREFIX = "header.prefix";
    /** Expected JWT audiences, used for validation. */
    public static final String EXPECTED_JWT_AUDIENCES = "expected.jwt.audiences";

    /** Algorithm used for validating the signature, eg. rsa, hmac */
    public static final String KEY_ALGORITHM = "key.algorithm";
    /** Storage type, where the key is stored, eg. property, keystore, url */
    public static final String KEY_STORE_TYPE = "key.store.type";

    // -------------------------------- rsa properties ---------------------

    /** If storage type is "url" then provide the path where the key can be downloaded from. */
    public static final String PUBLIC_KEY_URL = "public.key.url";
    /** If storage type is "property" then provide the value of the key here */
    public static final String PUBLIC_KEY_PROPERTY = "public.key.property";
    /** If storage type is "keystore" then provide the path to they keystore file */
    public static final String PUBLIC_KEY_KEYSTORE = "public.key.keystore";
    /** Keystore alias */
    public static final String PUBLIC_KEY_KEYSTORE_ALIAS = "public.key.keystore.alias";
    /** Keystore password */
    public static final String PUBLIC_KEY_KEYSTORE_PASSWORD = "public.key.keystore.password";

    // -------------------------------- hmac properties ---------------------

    /** If storage type is "property" then provide the value of the secret key here. */
    public static final String HMAC_SECRET_KEY_PROPERTY = "hmac.secret.key";
    /** If storage type is "url" then provide the path where the secret key can be downloaded from */
    public static final String HMAC_SECRET_KEY_URL = "hmac.secret.key";

    // -------------------------------- HTTP Client properties ---------------------

    public static final String OAUTH_GET_CERT_METHOD = "oauth2.get.cert.method";
    public static final String OAUTH_GET_CERT_METHOD_BODY = "oauth2.get.cert.body";
    public static final String OAUTH_GET_CERT_METHOD_CONTENT_TYPE = "oauth2.get.cert.content.type";

    public static final String OAUTH_HTTP_BASIC_USER = "basic.user";
    public static final String OAUTH_HTTP_BASIC_PASSWORD = "basic.password";
    public static final String OAUTH_HTTP_KEY_STORE_PATH = "keyStorePath";
    public static final String OAUTH_HTTP_KEY_STORE_TYPE = "keyStoreType";
    public static final String OAUTH_HTTP_KEY_STORE_PASSWORD = "keyStorePassword";
    public static final String OAUTH_HTTP_KEY_STORE_PROVIDER = "keyStoreProvider";
    public static final String OAUTH_HTTP_KEY_MANAGER_FACTORY_ALGORITHM = "keyManagerFactoryAlgorithm";
    public static final String OAUTH_HTTP_KEY_MANAGER_FACTORY_PROVIDER = "keyManagerFactoryProvider";
    public static final String OAUTH_HTTP_KEY_PASSWORD = "keyPassword";
    public static final String OAUTH_HTTP_TRUST_STORE_TYPE = "trustStoreType";
    public static final String OAUTH_HTTP_TRUST_STORE_PATH = "trustStorePath";
    public static final String OAUTH_HTTP_TRUST_STORE_PASSWORD = "trustStorePassword";
    public static final String OAUTH_HTTP_TRUST_STORE_PROVIDER = "trustStoreProvider";
    public static final String OAUTH_HTTP_TRUST_MANAGER_FACTORY_ALGORITHM = "trustManagerFactoryAlgorithm";
    public static final String OAUTH_HTTP_TRUST_MANAGER_FACTORY_PROVIDER = "trustManagerFactoryProvider";
    public static final String OAUTH_HTTP_SECURITY_PROTOCOL = "protocol";


    // private constructor
    private OAuth2Config() { }
}
