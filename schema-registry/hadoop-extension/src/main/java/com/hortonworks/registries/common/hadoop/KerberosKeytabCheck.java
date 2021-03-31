/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.common.hadoop;

import com.hortonworks.registries.common.ServiceAuthenticationConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class KerberosKeytabCheck extends AbstractHadoopPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosKeytabCheck.class);

    public KerberosKeytabCheck() { }

    @Override
    public void loadKerberosUser(ServiceAuthenticationConfiguration conf) {
        String authenticationType = conf.getType();
        if (authenticationType != null && authenticationType.equals("kerberos")) {
            Map<String, String> serviceAuthenticationProperties = conf.getProperties();
            if (serviceAuthenticationProperties != null) {
                String principal = serviceAuthenticationProperties.get("principal");
                String keytab = serviceAuthenticationProperties.get("keytab");

                if (StringUtils.isNotEmpty(principal) && StringUtils.isNotEmpty(keytab)) {
                    LOG.debug("Login with principal = '{}' and keyTab = '{}'", principal, keytab);
                    try {
                        UserGroupInformation.loginUserFromKeytab(principal, keytab);
                        LOG.debug("Successfully logged in");
                    } catch (Exception e) {
                        LOG.error("Failed to log in", e);
                    }
                } else {
                    LOG.error("Invalid service authentication configuration for 'kerberos' principal = '{}' and keytab = '{}'", principal, keytab);
                }
            } else {
                LOG.error("No service authentication properties were configured for 'kerberos'");
            }
        } else {
            LOG.error("Invalid service authentication type : {}", authenticationType);
        }
    }

    @Override
    public Set<String> getGroupsForUser(String kerberosShortName) {
        return UserGroupInformation.createRemoteUser(kerberosShortName).getGroupsSet();
    }
}
