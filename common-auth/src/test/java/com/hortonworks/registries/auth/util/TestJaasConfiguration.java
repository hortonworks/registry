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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.Map;

public class TestJaasConfiguration {

    @Test
    public void testJaasConfiguration() {
        JaasConfiguration conf = new JaasConfiguration("RegistryClient", "com.sun.security.auth.module.Krb5LoginModule required doNotPrompt=true " +
                "service = kafka useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true " +
                "keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true;");

        Assertions.assertNotNull(conf.getAppConfigurationEntry("RegistryClient"));
        Assertions.assertNull(conf.getAppConfigurationEntry("RegistryCl"));
        Assertions.assertEquals(1, conf.getAppConfigurationEntry("RegistryClient").length);
        Assertions.assertEquals("com.sun.security.auth.module.Krb5LoginModule", conf.getAppConfigurationEntry("RegistryClient")[0].getLoginModuleName());
        Assertions.assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, conf.getAppConfigurationEntry("RegistryClient")[0].getControlFlag());

        Map<String, ?> options = conf.getAppConfigurationEntry("RegistryClient")[0].getOptions();
        Assertions.assertEquals("HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE", options.get("principal"));
        Assertions.assertEquals("true", options.get("useKeyTab"));
        Assertions.assertEquals("true", options.get("doNotPrompt"));
        Assertions.assertEquals("false", options.get("useTicketCache"));
        Assertions.assertEquals("/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab", options.get("keyTab"));
        Assertions.assertEquals("true", options.get("debug"));
        Assertions.assertEquals("kafka", options.get("service"));
    }

    @Test
    public void testJaasConfigurationFail() {
        //; missing at the end.
        try {
            new JaasConfiguration("RegistryClient",
                    "com.sun.security.auth.module.Krb5LoginModule required doNotPrompt=true useTicketCache=false " +
                            "principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true");
            Assertions.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assertions.assertEquals(IllegalArgumentException.class, ex.getClass());
        }

        //controlFlag missing
        try {
            new JaasConfiguration("RegistryClient",
                    "com.sun.security.auth.module.Krb5LoginModule " +
                            "doNotPrompt=true useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" " +
                            "useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true;");
            Assertions.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assertions.assertEquals(IllegalArgumentException.class, ex.getClass());
        }

        //loginModuleClass missing
        try {
            new JaasConfiguration("RegistryClient",
                    " required doNotPrompt=true useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" " +
                            "useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true;");
            Assertions.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assertions.assertEquals(IllegalArgumentException.class, ex.getClass());
        }

        //= replaced with : for some of the options.
        try {
            new JaasConfiguration("RegistryClient", "com.sun.security.auth.module.Krb5LoginModule required doNotPrompt:true useTicketCache:false " +
                    "principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true");
            Assertions.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assertions.assertEquals(IllegalArgumentException.class, ex.getClass());
        }
    }
}
