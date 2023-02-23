/**
 * Copyright 2016-2023 Cloudera, Inc.
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

package com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
public class KerberosSetup {
    private static final String KDC_REALM = "K8S.COM";
    private static final String SR_SERVER_PRINCIPAL_HOSTNAME = "localhost";
    private static final String SR_SERVER_PINCIPAL_KEYTAB_FILENAME = "sr_server_principal.keytab";
    private static final String TEST_PRINCIPAL_NAME_WITHOUT_REALM = "test_principal";
    private static final String TEST_PRINCIPAL_NAME = TEST_PRINCIPAL_NAME_WITHOUT_REALM + "@" +
            KDC_REALM;
    private static final String TEST_PRINCIPAL_KEYTAB_FILENAME = TEST_PRINCIPAL_NAME_WITHOUT_REALM + ".keytab";
    private static final String COMMON_PRINCIPAL_PASSWORD = "pass1234";

    @Getter
    @Builder.Default
    private String srServerPrincipalPassword = COMMON_PRINCIPAL_PASSWORD;

    @Getter
    @Builder.Default
    private String testPrincipalPassword = COMMON_PRINCIPAL_PASSWORD;

    @Getter
    @Builder.Default
    private String srServerPrincipalKeytabFilename = SR_SERVER_PINCIPAL_KEYTAB_FILENAME;


    @Getter
    @Builder.Default
    private String testPrincipalKeytabFilename = TEST_PRINCIPAL_KEYTAB_FILENAME;


    @Setter
    @Getter
    private String srServerPrincipalKeytabPath;

    @Getter
    @Builder.Default
    private String testPrincipalName = TEST_PRINCIPAL_NAME;

    @Setter
    @Getter
    private String testPrincipalKeytabPath;

    @Getter
    @Builder.Default
    private String srServerPrincipalHostname = SR_SERVER_PRINCIPAL_HOSTNAME;

    @Getter
    @Setter
    private String krb5ConfFilePathForTest;


    @Getter
    @Setter
    private String krb5ConfFilePathForContainerizedSrServer;


    public String getSrServerPrincipalName() {
        return "HTTP/" + srServerPrincipalHostname + "@" + KDC_REALM;
    }

}
