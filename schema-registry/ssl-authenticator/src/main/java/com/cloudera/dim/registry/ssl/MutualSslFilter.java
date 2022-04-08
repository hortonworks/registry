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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.registries.auth.client.AuthenticationException;
import com.hortonworks.registries.auth.server.AuthenticationHandler;
import com.hortonworks.registries.auth.server.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.x500.X500Principal;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// parts of this code is copied from Apache Kafka SslPrincipalMapper class: 
// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/security/ssl/SslPrincipalMapper.java
public class MutualSslFilter implements AuthenticationHandler {
    public static final String TYPE = "mTLS";
    public static final String SSL_PRINCIPAL_MAPPING_RULES = "rules";
    private static final Logger LOG = LoggerFactory.getLogger(MutualSslFilter.class);
    private static final Pattern RULE_PARSER = Pattern.compile("((DEFAULT)|(RULE:(([^/]*)/([^/]*))/([LU])?))");

    private List<Rule> rules;

    private static List<Rule> parseRules(List<String> rules) {
        List<Rule> result = new ArrayList<>();
        for (String rule : rules) {
            String normalizedRule = normalizeRule(rule);
            if (normalizedRule.isEmpty()) {
                normalizedRule = rule;
            }
            Matcher matcher = RULE_PARSER.matcher(normalizedRule);
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException("Invalid rule: " + normalizedRule);
            }
            if (normalizedRule.length() != matcher.end()) {
                throw new IllegalArgumentException("Invalid rule: `" + normalizedRule + "`, unmatched substring: `" + normalizedRule.substring(matcher.end()) + "`");
            }
            if (matcher.group(2) != null) {
                result.add(new Rule());
            } else {
                result.add(new Rule(matcher.group(5),
                        matcher.group(6),
                        "L".equals(matcher.group(7)),
                        "U".equals(matcher.group(7))));
            }
        }
        return result;
    }

    private static String normalizeRule(String rule) {
        String result = "";
        if (rule.startsWith("\"") && rule.endsWith("\",")) {
            result = rule.substring(1, rule.length() - 2);
        } else if (rule.startsWith("\"") && rule.endsWith("\"")) {
            result = rule.substring(1, rule.length() - 1);
        }
        return result;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties config) throws ServletException {
        LOG.info("Initializing mutual TLS authentication ...");
        if (config.containsKey(SSL_PRINCIPAL_MAPPING_RULES)) {
            rules = parseRules(Arrays.asList(config.getProperty(SSL_PRINCIPAL_MAPPING_RULES).split("(?=\"DEFAULT)|(?=\"RULE:)")));
        }
    }

    public String getName(String distinguishedName) throws IOException {
        for (Rule r : rules) {
            String principalName = r.apply(distinguishedName);
            if (principalName != null) {
                return principalName;
            }
        }
        throw new NoMatchingRule("No rules apply to " + distinguishedName + ", rules " + rules);
    }

    @Override
    public void destroy() {

    }

    @Override
    public boolean shouldAuthenticate(HttpServletRequest request) {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        AuthenticationToken token = null;
        if (certs.length != 0) {
            X509Certificate clientCert = certs[0];
            long expirationDate = getExpiration(clientCert);
            X500Principal subjectDN = clientCert.getSubjectX500Principal();
            if (!rules.isEmpty()) {
                for (Rule rule : rules) {
                    String principal = rule.apply(subjectDN.getName());
                    if (principal != null) {
                        AuthenticationToken authenticationToken = new AuthenticationToken(principal, principal, getType());
                        authenticationToken.setExpires(expirationDate);
                        return authenticationToken;
                    }
                }
            }
        }
        return token;
    }

    private Long getExpiration(X509Certificate clientCert) throws AuthenticationException {
        Date notAfter = clientCert.getNotAfter();
        try {
            clientCert.checkValidity();
        } catch (CertificateNotYetValidException | CertificateExpiredException e) {
            throw new AuthenticationException("SSL certificate is not valid.", e);
        }
        return notAfter.getTime();
    }

    @VisibleForTesting
    List<Rule> getRules() {
        return rules;
    }

    static class Rule {
        private static final Pattern BACK_REFERENCE_PATTERN = Pattern.compile("\\$(\\d+)");

        private final boolean isDefault;
        private final Pattern pattern;
        private final String replacement;
        private final boolean toLowerCase;
        private final boolean toUpperCase;

        Rule() {
            isDefault = true;
            pattern = null;
            replacement = null;
            toLowerCase = false;
            toUpperCase = false;
        }

        Rule(String pattern, String replacement, boolean toLowerCase, boolean toUpperCase) {
            isDefault = false;
            this.pattern = pattern == null ? null : Pattern.compile(pattern);
            this.replacement = replacement;
            this.toLowerCase = toLowerCase;
            this.toUpperCase = toUpperCase;
        }

        String apply(String distinguishedName) {
            if (isDefault) {
                return distinguishedName;
            }

            String result = null;
            final Matcher m = pattern.matcher(distinguishedName);

            if (m.matches()) {
                result = distinguishedName.replaceAll(pattern.pattern(), escapeLiteralBackReferences(replacement, m.groupCount()));
            }

            if (toLowerCase && result != null) {
                result = result.toLowerCase(Locale.ENGLISH);
            } else if (toUpperCase & result != null) {
                result = result.toUpperCase(Locale.ENGLISH);
            }

            return result;
        }

        //If we find a back reference that is not valid, then we will treat it as a literal string. For example, if we have 3 capturing
        //groups and the Replacement Value has the value is "$1@$4", then we want to treat the $4 as a literal "$4", rather
        //than attempting to use it as a back reference.
        //This method was taken from Apache Nifi project : org.apache.nifi.authorization.util.IdentityMappingUtil
        private String escapeLiteralBackReferences(final String unescaped, final int numCapturingGroups) {
            if (numCapturingGroups == 0) {
                return unescaped;
            }

            String value = unescaped;
            final Matcher backRefMatcher = BACK_REFERENCE_PATTERN.matcher(value);
            while (backRefMatcher.find()) {
                final String backRefNum = backRefMatcher.group(1);
                if (backRefNum.startsWith("0")) {
                    continue;
                }
                final int originalBackRefIndex = Integer.parseInt(backRefNum);
                int backRefIndex = originalBackRefIndex;


                // if we have a replacement value like $123, and we have less than 123 capturing groups, then
                // we want to truncate the 3 and use capturing group 12; if we have less than 12 capturing groups,
                // then we want to truncate the 2 and use capturing group 1; if we don't have a capturing group then
                // we want to truncate the 1 and get 0.
                while (backRefIndex > numCapturingGroups && backRefIndex >= 10) {
                    backRefIndex /= 10;
                }

                if (backRefIndex > numCapturingGroups) {
                    final StringBuilder sb = new StringBuilder(value.length() + 1);
                    final int groupStart = backRefMatcher.start(1);

                    sb.append(value.substring(0, groupStart - 1));
                    sb.append("\\");
                    sb.append(value.substring(groupStart - 1));
                    value = sb.toString();
                }
            }

            return value;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            if (isDefault) {
                buf.append("DEFAULT");
            } else {
                buf.append("RULE:");
                if (pattern != null) {
                    buf.append(pattern);
                }
                if (replacement != null) {
                    buf.append("/");
                    buf.append(replacement);
                }
                if (toLowerCase) {
                    buf.append("/L");
                } else if (toUpperCase) {
                    buf.append("/U");
                }
            }
            return buf.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Rule rule = (Rule) o;
            boolean defaultEqual = isDefault == rule.isDefault;
            boolean lowerCaseEqual = toLowerCase == rule.toLowerCase;
            boolean upperCaseEqual = toUpperCase == rule.toUpperCase;
            boolean patternEqual = (pattern == null && rule.pattern == null) || pattern.toString().equals(rule.pattern.toString());
            boolean replacementEqual = (replacement == null && rule.replacement == null) || Objects.equals(replacement, rule.replacement);
            return defaultEqual && lowerCaseEqual && upperCaseEqual && patternEqual && replacementEqual;
        }

        @Override
        public int hashCode() {
            return Objects.hash(isDefault, pattern, replacement, toLowerCase, toUpperCase);
        }
    }

    public static class NoMatchingRule extends IOException {
        NoMatchingRule(String msg) {
            super(msg);
        }
    }
}