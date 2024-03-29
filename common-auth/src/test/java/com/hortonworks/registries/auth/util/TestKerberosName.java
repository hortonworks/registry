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



import com.hortonworks.registries.auth.KerberosTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Assertions;

public class TestKerberosName {

    @BeforeEach
    public void setUp() throws Exception {
        System.setProperty("java.security.krb5.realm", KerberosTestUtils.getRealm());
        System.setProperty("java.security.krb5.kdc", "localhost:88");

        String rules =
                "RULE:[1:$1@$0](.*@YAHOO\\.COM)s/@.*//\n" +
                        "RULE:[2:$1](johndoe)s/^.*$/guest/\n" +
                        "RULE:[2:$1;$2](^.*;admin$)s/;admin$//\n" +
                        "RULE:[2:$2](root)\n" +
                        "DEFAULT";
        KerberosName.setRules(rules);
        KerberosName.printRules();
    }

    private void checkTranslation(String from, String to) throws Exception {
        System.out.println("Translate " + from);
        KerberosName nm = new KerberosName(from);
        String simple = nm.getShortName();
        System.out.println("to " + simple);
        Assertions.assertEquals(to, simple, "short name incorrect");
    }

    @Test
    public void testRules() throws Exception {
        checkTranslation("omalley@" + KerberosTestUtils.getRealm(), "omalley");
        checkTranslation("hdfs/10.0.0.1@" + KerberosTestUtils.getRealm(), "hdfs");
        checkTranslation("oom@YAHOO.COM", "oom");
        checkTranslation("johndoe/zoo@FOO.COM", "guest");
        checkTranslation("joe/admin@FOO.COM", "joe");
        checkTranslation("joe/root@FOO.COM", "root");
    }

    private void checkBadName(String name) {
        System.out.println("Checking " + name + " to ensure it is bad.");
        try {
            new KerberosName(name);
            Assertions.fail("didn't get exception for " + name);
        } catch (IllegalArgumentException iae) {
            // PASS
        }
    }

    @Test
    public void testAntiPatterns() throws Exception {
        checkBadName("owen/owen/owen@FOO.COM");
        checkBadName("owen@foo/bar.com");
        // no rules applied, these should pass
        checkTranslation("foo@ACME.COM", "foo@ACME.COM");
        checkTranslation("root/joe@FOO.COM", "root/joe@FOO.COM");
    }

    @Test
    public void testToLowerCase() throws Exception {
        String rules =
                "RULE:[1:$1]/L\n" +
                        "RULE:[2:$1]/L\n" +
                        "RULE:[2:$1;$2](^.*;admin$)s/;admin$///L\n" +
                        "RULE:[2:$1;$2](^.*;guest$)s/;guest$//g/L\n" +
                        "DEFAULT";
        KerberosName.setRules(rules);
        KerberosName.printRules();
        checkTranslation("Joe@FOO.COM", "joe");
        checkTranslation("Joe/root@FOO.COM", "joe");
        checkTranslation("Joe/admin@FOO.COM", "joe");
        checkTranslation("Joe/guestguest@FOO.COM", "joe");
    }

    @AfterEach
    public void clear() {
        System.clearProperty("java.security.krb5.realm");
        System.clearProperty("java.security.krb5.kdc");
    }
}
