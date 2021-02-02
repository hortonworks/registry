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

import com.hortonworks.registries.auth.client.AuthenticationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthToken {

    @Test
    public void testConstructor() throws Exception {
        try {
            new AuthToken(null, "p", "t");
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        try {
            new AuthToken("", "p", "t");
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        try {
            new AuthToken("u", null, "t");
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        try {
            new AuthToken("u", "", "t");
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        try {
            new AuthToken("u", "p", null);
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        try {
            new AuthToken("u", "p", "");
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // Expected
        } catch (Throwable ex) {
            Assertions.fail();
        }
        new AuthToken("u", "p", "t");
    }

    @Test
    public void testGetters() throws Exception {
        long expires = System.currentTimeMillis() + 50;
        AuthToken token = new AuthToken("u", "p", "t");
        token.setExpires(expires);
        Assertions.assertEquals("u", token.getUserName());
        Assertions.assertEquals("p", token.getName());
        Assertions.assertEquals("t", token.getType());
        Assertions.assertEquals(expires, token.getExpires());
        Assertions.assertFalse(token.isExpired());
        Thread.sleep(70);               // +20 msec fuzz for timer granularity.
        Assertions.assertTrue(token.isExpired());
    }

    @Test
    public void testToStringAndParse() throws Exception {
        long expires = System.currentTimeMillis() + 50;
        AuthToken token = new AuthToken("u", "p", "t");
        token.setExpires(expires);
        String str = token.toString();
        token = AuthToken.parse(str);
        Assertions.assertEquals("p", token.getName());
        Assertions.assertEquals("t", token.getType());
        Assertions.assertEquals(expires, token.getExpires());
        Assertions.assertFalse(token.isExpired());
        Thread.sleep(70);               // +20 msec fuzz for timer granularity.
        Assertions.assertTrue(token.isExpired());
    }

    @Test
    public void testParseValidAndInvalid() throws Exception {
        long expires = System.currentTimeMillis() + 50;
        AuthToken token = new AuthToken("u", "p", "t");
        token.setExpires(expires);
        String ostr = token.toString();

        String str1 = "\"" + ostr + "\"";
        AuthToken.parse(str1);

        String str2 = ostr + "&s=1234";
        AuthToken.parse(str2);

        String str = ostr.substring(0, ostr.indexOf("e="));
        try {
            AuthToken.parse(str);
            Assertions.fail();
        } catch (AuthenticationException ex) {
            // Expected
        } catch (Exception ex) {
            Assertions.fail();
        }
    }
}
