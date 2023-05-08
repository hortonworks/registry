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
 * limitations under the License. See accompanying LICENSE file.
 */
package com.cloudera.dim.registry.ssl;

import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.server.AuthenticationToken;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

class MutualSslFilterTest {

    private MutualSslFilter underTest = new MutualSslFilter();
    private Properties properties = new Properties();


    void setUp(boolean isDefault, String testType) {
        properties.put("type", "com.cloudera.dim.registry.ssl.MutualSslFilter");
        if (isDefault) {
            properties.put("rules", "DEFAULT");
        } else if (testType.equals("RuleTests")) {
            properties.put("rules", "\"RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L\"," +
                    "RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L," +
                    "\"RULE:^cn=(.*?),ou=(.*?),dc=(.*?),dc=(.*?)$/$1@$2/U\"," +
                    "\"RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U\"," +
                    "\"DEFAULT\"");
        } else if (testType.equals("Authentication_Valid")) {
            properties.put("rules", "RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U");
        } else if (testType.equals("Authentication_Invalid")) {
            properties.put("rules", "RULE:^.*[Cc][Nn]=(CN*).*$/$1/U");
        } else if (testType.equals("Second_Cert_Valid")) {
            properties.put("rules", "RULE:.*?ST=(B.*?),C=(.*?)$/$1,C=$2/U");
        }
    }

    @Test
    void testInitEqualRuleLists() throws Exception {
        //given
        setUp(false, "RuleTests");
        List<MutualSslFilter.Rule> expectedRules = new ArrayList<>();
        MutualSslFilter.Rule rule1 = new MutualSslFilter.Rule("^CN=(.*?),OU=ServiceUsers.*$", "$1", true, false);
        MutualSslFilter.Rule rule2 = new MutualSslFilter.Rule("^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$", "$1@$2", true, false);
        MutualSslFilter.Rule rule3 = new MutualSslFilter.Rule("^cn=(.*?),ou=(.*?),dc=(.*?),dc=(.*?)$", "$1@$2", false, true);
        MutualSslFilter.Rule rule4 = new MutualSslFilter.Rule("^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$", "$1", false, true);
        MutualSslFilter.Rule rule5 = new MutualSslFilter.Rule();
        expectedRules.add(rule1);
        expectedRules.add(rule2);
        expectedRules.add(rule3);
        expectedRules.add(rule4);
        expectedRules.add(rule5);

        //when
        underTest.init(properties);

        //then
        assertEquals(expectedRules, underTest.getRules());

    }

    @Test
    void testInitNotEqualRuleLists() throws Exception {
        //given
        setUp(false, "RuleTests");
        List<MutualSslFilter.Rule> expectedRules = new ArrayList<>();
        MutualSslFilter.Rule rule1 = new MutualSslFilter.Rule("^CN=(.*?),OU=ServiceUsers.*$", "$1", true, false);
        MutualSslFilter.Rule rule2 = new MutualSslFilter.Rule("^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$", "$1@$2", true, false);
        MutualSslFilter.Rule rule3 = new MutualSslFilter.Rule("^cn=(.*?),ou=(.*?),dc=(.*?),dc=(.*?)$", "$1@$2", true, true);
        MutualSslFilter.Rule rule4 = new MutualSslFilter.Rule("^.*[Cc][Nn]=([a-zA-Z0-9.],L=(.*?)*).*$", "$5", false, true);
        MutualSslFilter.Rule rule5 = new MutualSslFilter.Rule();
        expectedRules.add(rule1);
        expectedRules.add(rule2);
        expectedRules.add(rule3);
        expectedRules.add(rule4);
        expectedRules.add(rule5);

        //when
        underTest.init(properties);

        //then
        assertNotEquals(expectedRules, underTest.getRules());
    }

    @Test
    void authenticateDefaultRules() throws Exception {
        //given
        setUp(true, "");
        String resourceName = "/test.cer";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);

        InputStream resourceAsStream = getClass().getResourceAsStream(resourceName);
        Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(resourceAsStream);
        X509Certificate[] expectedCerts = new X509Certificate[]{(X509Certificate) cert};
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(expectedCerts);
        AuthenticationToken expectedToken = new AuthenticationToken("CN=localhost,OU=localhost,O=localhost,L=Budapest,ST=Budapest,C=HU", "CN=localhost,OU=localhost,O=localhost,L=Budapest,ST=Budapest,C=HU", "mTLS");
        expectedToken.setExpires(expectedCerts[0].getNotAfter().getTime());

        //when
        underTest.init(properties);
        AuthenticationToken authenticate = underTest.authenticate(request, responseMock);

        //then
        assertEquals(expectedToken.getName(), authenticate.getName());
        assertEquals(expectedToken.getType(), authenticate.getType());
        assertEquals(expectedToken.getUserName(), authenticate.getUserName());
        assertEquals(expectedToken.getExpires(), authenticate.getExpires());
    }

    @Test
    void authenticateWithValidRules() throws Exception {
        //given
        setUp(false, "Authentication_Valid");
        String resourceName = "/test.cer";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);

        InputStream resourceAsStream = getClass().getResourceAsStream(resourceName);
        Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(resourceAsStream);
        X509Certificate[] expectedCerts = new X509Certificate[]{(X509Certificate) cert};
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(expectedCerts);
        AuthenticationToken expectedToken = new AuthenticationToken("LOCALHOST", "LOCALHOST", "mTLS");
        expectedToken.setExpires(expectedCerts[0].getNotAfter().getTime());

        //when
        underTest.init(properties);
        AuthenticationToken authenticate = underTest.authenticate(request, responseMock);

        //then
        assertEquals(expectedToken.getName(), authenticate.getName());
        assertEquals(expectedToken.getType(), authenticate.getType());
        assertEquals(expectedToken.getUserName(), authenticate.getUserName());
        assertEquals(expectedToken.getExpires(), authenticate.getExpires());
    }

    @Test
    void authenticateWithValidRulesTwoCerts() throws Exception {
        //given
        setUp(false, "Authentication_Valid");
        String secondCert = "/test.cer";
        String firstCert = "/another_test.cer";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);

        InputStream secondCertAsStream = getClass().getResourceAsStream(secondCert);
        InputStream firstCertAsStream = getClass().getResourceAsStream(firstCert);
        Certificate certSecond = CertificateFactory.getInstance("X.509").generateCertificate(secondCertAsStream);
        Certificate certFirst = CertificateFactory.getInstance("X.509").generateCertificate(firstCertAsStream);
        X509Certificate[] expectedCerts = new X509Certificate[]{(X509Certificate) certFirst, (X509Certificate) certSecond};
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(expectedCerts);
        AuthenticationToken expectedToken = new AuthenticationToken("TEST", "TEST", "mTLS");
        expectedToken.setExpires(expectedCerts[0].getNotAfter().getTime());

        //when
        underTest.init(properties);
        AuthenticationToken authenticate = underTest.authenticate(request, responseMock);

        //then
        assertEquals(expectedToken.getName(), authenticate.getName());
        assertEquals(expectedToken.getType(), authenticate.getType());
        assertEquals(expectedToken.getUserName(), authenticate.getUserName());
        assertEquals(expectedToken.getExpires(), authenticate.getExpires());
    }

    @Test
    void authenticateWithValidRulesTwoCertsMatchInSecondCert() throws Exception {
        //given
        setUp(false, "Second_Cert_Valid");
        String secondCert = "/test.cer";
        String firstCert = "/another_test.cer";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);

        InputStream secondCertAsStream = getClass().getResourceAsStream(secondCert);
        InputStream firstCertAsStream = getClass().getResourceAsStream(firstCert);
        Certificate certSecond = CertificateFactory.getInstance("X.509").generateCertificate(secondCertAsStream);
        Certificate certFirst = CertificateFactory.getInstance("X.509").generateCertificate(firstCertAsStream);
        X509Certificate[] expectedCerts = new X509Certificate[]{(X509Certificate) certFirst, (X509Certificate) certSecond};
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(expectedCerts);
        AuthenticationToken expectedToken = new AuthenticationToken("BUDAPEST,C=HU", "BUDAPEST,C=HU", "mTLS");
        expectedToken.setExpires(expectedCerts[1].getNotAfter().getTime());

        //when
        underTest.init(properties);
        AuthenticationToken authenticate = underTest.authenticate(request, responseMock);

        //then
        assertEquals(expectedToken.getName(), authenticate.getName());
        assertEquals(expectedToken.getType(), authenticate.getType());
        assertEquals(expectedToken.getUserName(), authenticate.getUserName());
        assertEquals(expectedToken.getExpires(), authenticate.getExpires());
    }

    @Test
    void authenticateWithInvalidRules() throws Exception {
        //given
        setUp(false, "Authentication_Invalid");
        String resourceName = "/test.cer";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);

        InputStream resourceAsStream = getClass().getResourceAsStream(resourceName);
        Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(resourceAsStream);
        X509Certificate[] expectedCerts = new X509Certificate[]{(X509Certificate) cert};
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(expectedCerts);

        //when
        underTest.init(properties);
        assertThrows(AuthenticationException.class, () -> underTest.authenticate(request, responseMock));
    }

    @Test
    void authenticateExpiredCert() throws Exception {
        //given
        setUp(false, "Authentication_Invalid");
        String resourceName = "/notvalidtest.cer";
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);

        InputStream resourceAsStream = getClass().getResourceAsStream(resourceName);
        Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(resourceAsStream);
        X509Certificate[] expectedCerts = new X509Certificate[]{(X509Certificate) cert};
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(expectedCerts);

        //when
        underTest.init(properties);
        assertThrows(AuthenticationException.class, () -> underTest.authenticate(request, responseMock));
    }

    @Test
    void authenticateNullCert() throws Exception {
        //given
        setUp(true, "Authentication_Invalid");
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse responseMock = Mockito.mock(HttpServletResponse.class);
        
        when(request.getAttribute("javax.servlet.request.X509Certificate")).thenReturn(null);

        //when
        assertThrows(AuthenticationException.class, () -> underTest.authenticate(request, responseMock));
    }
}