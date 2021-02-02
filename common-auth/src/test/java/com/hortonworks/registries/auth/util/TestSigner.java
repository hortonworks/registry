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

import java.util.Properties;
import javax.servlet.ServletContext;

import com.hortonworks.registries.auth.server.AuthenticationFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSigner {

    @Test
    public void testNullAndEmptyString() throws Exception {
        Signer signer = new Signer(createStringSignerSecretProvider());
        try {
            signer.sign(null);
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        try {
            signer.sign("");
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
    }

    @Test
    public void testSignature() throws Exception {
        Signer signer = new Signer(createStringSignerSecretProvider());
        String s1 = signer.sign("ok");
        String s2 = signer.sign("ok");
        String s3 = signer.sign("wrong");
        Assertions.assertEquals(s1, s2);
        Assertions.assertNotEquals(s1, s3);
    }

    @Test
    public void testVerify() throws Exception {
        Signer signer = new Signer(createStringSignerSecretProvider());
        String t = "test";
        String s = signer.sign(t);
        String e = signer.verifyAndExtract(s);
        Assertions.assertEquals(t, e);
    }

    @Test
    public void testInvalidSignedText() throws Exception {
        Signer signer = new Signer(createStringSignerSecretProvider());
        try {
            signer.verifyAndExtract("test");
            Assertions.fail();
        } catch (SignerException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
    }

    @Test
    public void testTampering() throws Exception {
        Signer signer = new Signer(createStringSignerSecretProvider());
        String t = "test";
        String s = signer.sign(t);
        s += "x";
        try {
            signer.verifyAndExtract(s);
            Assertions.fail();
        } catch (SignerException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
    }

    private StringSignerSecretProvider createStringSignerSecretProvider() throws Exception {
        StringSignerSecretProvider secretProvider = new StringSignerSecretProvider();
        Properties secretProviderProps = new Properties();
        secretProviderProps.setProperty(AuthenticationFilter.SIGNATURE_SECRET, "secret");
        secretProvider.init(secretProviderProps, null, -1);
        return secretProvider;
    }

    @Test
    public void testMultipleSecrets() throws Exception {
        TestSignerSecretProvider secretProvider = new TestSignerSecretProvider();
        Signer signer = new Signer(secretProvider);
        secretProvider.setCurrentSecret("secretB");
        String t1 = "test";
        String s1 = signer.sign(t1);
        String e1 = signer.verifyAndExtract(s1);
        Assertions.assertEquals(t1, e1);
        secretProvider.setPreviousSecret("secretA");
        String t2 = "test";
        String s2 = signer.sign(t2);
        String e2 = signer.verifyAndExtract(s2);
        Assertions.assertEquals(t2, e2);
        Assertions.assertEquals(s1, s2); //check is using current secret for signing
        secretProvider.setCurrentSecret("secretC");
        secretProvider.setPreviousSecret("secretB");
        String t3 = "test";
        String s3 = signer.sign(t3);
        String e3 = signer.verifyAndExtract(s3);
        Assertions.assertEquals(t3, e3);
        Assertions.assertNotEquals(s1, s3); //check not using current secret for signing
        String e1b = signer.verifyAndExtract(s1);
        Assertions.assertEquals(t1, e1b); // previous secret still valid
        secretProvider.setCurrentSecret("secretD");
        secretProvider.setPreviousSecret("secretC");
        try {
            signer.verifyAndExtract(s1);  // previous secret no longer valid
            Assertions.fail();
        } catch (SignerException ex) {
            // Expected
        }
    }

    class TestSignerSecretProvider extends SignerSecretProvider {

        private byte[] currentSecret;
        private byte[] previousSecret;

        @Override
        public void init(Properties config, ServletContext servletContext,
                         long tokenValidity) {
        }

        @Override
        public byte[] getCurrentSecret() {
            return currentSecret;
        }

        @Override
        public byte[][] getAllSecrets() {
            return new byte[][]{currentSecret, previousSecret};
        }

        public void setCurrentSecret(String secretStr) {
            currentSecret = secretStr.getBytes();
        }

        public void setPreviousSecret(String previousSecretStr) {
            previousSecret = previousSecretStr.getBytes();
        }
    }
}
