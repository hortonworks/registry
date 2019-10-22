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

import org.junit.Test;
import org.junit.Assert;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.Map;

public class TestJaasConfiguration {

    @Test
    public void testJaasConfiguration() {
        JaasConfiguration conf = new JaasConfiguration("RegistryClient", "com.sun.security.auth.module.Krb5LoginModule required doNotPrompt=true " +
                "service = kafka useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true " +
                "keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true;");

        Assert.assertNotNull(conf.getAppConfigurationEntry("RegistryClient"));
        Assert.assertNull(conf.getAppConfigurationEntry("RegistryCl"));
        Assert.assertEquals(conf.getAppConfigurationEntry("RegistryClient").length, 1);
        Assert.assertEquals(conf.getAppConfigurationEntry("RegistryClient")[0].getLoginModuleName(), "com.sun.security.auth.module.Krb5LoginModule");
        Assert.assertEquals(conf.getAppConfigurationEntry("RegistryClient")[0].getControlFlag(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED);

        Map<String, ?> options = conf.getAppConfigurationEntry("RegistryClient")[0].getOptions();
        Assert.assertEquals(options.get("principal"), "HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE");
        Assert.assertEquals(options.get("useKeyTab"), "true");
        Assert.assertEquals(options.get("doNotPrompt"), "true");
        Assert.assertEquals(options.get("useTicketCache"), "false");
        Assert.assertEquals(options.get("keyTab"), "/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab");
        Assert.assertEquals(options.get("debug"), "true");
        Assert.assertEquals(options.get("service"), "kafka");
    }

    @Test
    public void testJaasConfigurationFail() {
        //; missing at the end.
        try {
            new JaasConfiguration("RegistryClient", "com.sun.security.auth.module.Krb5LoginModule required doNotPrompt=true useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true");
            Assert.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getClass(), IllegalArgumentException.class);
        }

        //controlFlag missing
        try {
            new JaasConfiguration("RegistryClient", "com.sun.security.auth.module.Krb5LoginModule " +
                    "doNotPrompt=true useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true;");
            Assert.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getClass(), IllegalArgumentException.class);
        }

        //loginModuleClass missing
        try {
            new JaasConfiguration("RegistryClient", " required doNotPrompt=true useTicketCache=false principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true;");
            Assert.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getClass(), IllegalArgumentException.class);
        }

        //= replaced with : for some of the options.
        try {
            new JaasConfiguration("RegistryClient", "com.sun.security.auth.module.Krb5LoginModule required doNotPrompt:true useTicketCache:false " +
                    "principal=\"HTTP/ip-10-97-81-205.cloudera.site@CLOUDERA.SITE\" useKeyTab=true keyTab=\"/var/run/cloudera-scm-agent/process/1546331895-schemaregistry-SCHEMA_REGISTRY_SERVER/schemaregistry.keytab\" debug=true");
            Assert.fail("Jaas Configuration should have failed.");
        } catch (Exception ex) {
            Assert.assertEquals(ex.getClass(), IllegalArgumentException.class);
        }
    }
}
