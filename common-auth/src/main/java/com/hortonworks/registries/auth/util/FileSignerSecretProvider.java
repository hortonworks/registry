/**
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

import com.hortonworks.registries.auth.server.AuthenticationFilter;

import javax.servlet.ServletContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * A SignerSecretProvider that simply loads a secret from a specified file.
 */
public class FileSignerSecretProvider extends SignerSecretProvider {

    private byte[] secret;
    private byte[][] secrets;

    public FileSignerSecretProvider() {
    }

    @Override
    public void init(Properties config, ServletContext servletContext,
                     long tokenValidity) throws Exception {

        String signatureSecretFile = config.getProperty(
                AuthenticationFilter.SIGNATURE_SECRET_FILE, null);

        Reader reader = null;
        if (signatureSecretFile != null) {
            try {
                StringBuilder sb = new StringBuilder();
                reader = new InputStreamReader(
                        new FileInputStream(signatureSecretFile), StandardCharsets.UTF_8);
                int c = reader.read();
                while (c > -1) {
                    sb.append((char) c);
                    c = reader.read();
                }
                secret = sb.toString().getBytes(Charset.forName("UTF-8"));
            } catch (IOException ex) {
                throw new RuntimeException("Could not read signature secret file: " +
                        signatureSecretFile);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        // nothing to do
                    }
                }
            }
        }

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
