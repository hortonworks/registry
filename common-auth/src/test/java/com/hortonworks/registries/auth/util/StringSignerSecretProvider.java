/**
 * Copyright 2016-2021 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.hortonworks.registries.auth.util;

import java.nio.charset.Charset;
import java.util.Properties;
import javax.servlet.ServletContext;

import com.hortonworks.registries.auth.server.AuthenticationFilter;

/**
 * A SignerSecretProvider that simply creates a secret based on a given String.
 */
class StringSignerSecretProvider extends SignerSecretProvider {

    private byte[] secret;
    private byte[][] secrets;

    public StringSignerSecretProvider() {
    }

    @Override
    public void init(Properties config, ServletContext servletContext,
                     long tokenValidity) throws Exception {
        String signatureSecret = config.getProperty(
                AuthenticationFilter.SIGNATURE_SECRET, null);
        secret = signatureSecret.getBytes(Charset.forName("UTF-8"));
        secrets = new byte[][]{secret};
    }

    @Override
    public byte[] getCurrentSecret() {
        return secret;
    }

    @Override
    public byte[][] getAllSecrets() {
        return secrets;
    }
}
