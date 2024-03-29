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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKerberosUtil {
    static String testKeytab = "test.keytab";
    static String[] testPrincipals = new String[]{
            "HTTP@testRealm",
            "test/testhost@testRealm",
            "HTTP/testhost@testRealm",
            "HTTP1/testhost@testRealm",
            "HTTP/testhostanother@testRealm"
    };

    @AfterEach
    public void deleteKeytab() {
        File keytabFile = new File(testKeytab);
        if (keytabFile.exists()) {
            keytabFile.delete();
        }
    }

    @Test
    public void testGetServerPrincipal() throws IOException {
        String service = "TestKerberosUtil";
        String localHostname = KerberosUtil.getLocalHostName();
        String testHost = "FooBar";

        // send null hostname
        Assertions.assertEquals(service + "/" + localHostname.toLowerCase(Locale.ENGLISH),
                KerberosUtil.getServicePrincipal(service, null), "When no hostname is sent");
        // send empty hostname
        Assertions.assertEquals(service + "/" + localHostname.toLowerCase(Locale.ENGLISH),
                KerberosUtil.getServicePrincipal(service, ""), "When empty hostname is sent");
        // send 0.0.0.0 hostname
        Assertions.assertEquals(service + "/" + localHostname.toLowerCase(Locale.ENGLISH),
                KerberosUtil.getServicePrincipal(service, "0.0.0.0"), "When 0.0.0.0 hostname is sent");
        // send uppercase hostname
        Assertions.assertEquals(service + "/" + testHost.toLowerCase(Locale.ENGLISH),
                KerberosUtil.getServicePrincipal(service, testHost), "When uppercase hostname is sent");
        // send lowercase hostname
        Assertions.assertEquals(service + "/" + testHost.toLowerCase(Locale.ENGLISH),
                KerberosUtil.getServicePrincipal(
                        service, testHost.toLowerCase(Locale.ENGLISH)), "When lowercase hostname is sent");
    }

    @Test
    public void testGetPrincipalNamesMissingKeytab() {
        try {
            KerberosUtil.getPrincipalNames(testKeytab);
            Assertions.fail("Exception should have been thrown");
        } catch (IOException e) {
            //expects exception
        }
    }

    @Test
    public void testGetPrincipalNamesMissingPattern() throws IOException {
        createKeyTab(testKeytab, new String[]{"test/testhost@testRealm"});
        try {
            KerberosUtil.getPrincipalNames(testKeytab, null);
            Assertions.fail("Exception should have been thrown");
        } catch (Exception e) {
            //expects exception
        }
    }

    @Test
    public void testGetPrincipalNamesFromKeytab() throws IOException {
        createKeyTab(testKeytab, testPrincipals);
        // read all principals in the keytab file
        String[] principals = KerberosUtil.getPrincipalNames(testKeytab);
        Assertions.assertNotNull(principals, "principals cannot be null");

        int expectedSize = 0;
        List<String> principalList = Arrays.asList(principals);
        for (String principal : testPrincipals) {
            Assertions.assertTrue(principalList.contains(principal),
                    "missing principal " + principal);
            expectedSize++;
        }
        Assertions.assertEquals(expectedSize, principals.length);
    }

    @Test
    public void testGetPrincipalNamesFromKeytabWithPattern() throws IOException {
        createKeyTab(testKeytab, testPrincipals);
        // read the keytab file
        // look for principals with HTTP as the first part
        Pattern httpPattern = Pattern.compile("HTTP/.*");
        String[] httpPrincipals =
                KerberosUtil.getPrincipalNames(testKeytab, httpPattern);
        Assertions.assertNotNull(httpPrincipals, "principals cannot be null");

        int expectedSize = 0;
        List<String> httpPrincipalList = Arrays.asList(httpPrincipals);
        for (String principal : testPrincipals) {
            if (httpPattern.matcher(principal).matches()) {
                Assertions.assertTrue(httpPrincipalList.contains(principal),
                        "missing principal " + principal);
                expectedSize++;
            }
        }
        Assertions.assertEquals(expectedSize, httpPrincipals.length);
    }

    private void createKeyTab(String fileName, String[] principalNames)
            throws IOException {
        //create a test keytab file
        List<KeytabEntry> lstEntries = new ArrayList<KeytabEntry>();
        for (String principal : principalNames) {
            // create 3 versions of the key to ensure methods don't return
            // duplicate principals
            for (int kvno = 1; kvno <= 3; kvno++) {
                EncryptionKey key = new EncryptionKey(
                        EncryptionType.UNKNOWN, "samplekey1".getBytes(), kvno);
                KeytabEntry keytabEntry = new KeytabEntry(
                        principal, 1, new KerberosTime(), (byte) 1, key);
                lstEntries.add(keytabEntry);
            }
        }
        Keytab keytab = Keytab.getInstance();
        keytab.setEntries(lstEntries);
        keytab.write(new File(testKeytab));
    }
}
