/**
 * Copyright 2016-2019 Cloudera, Inc.
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
package com.hortonworks.registries.auth.util;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;


/**
 * Class representing the Dynamic JAAS configuration.
 * JAAS configuration can be provided to an application using a static file provided by java.security.auth.login.config property.
 * Alternately, the JAAS Configuration can be constructed dynamically using this class which parses the configuration string.
 * <p/>
 * JAAS configuration file format is described <a href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html">here</a>.
 * The format of the property value is:
 * <pre>
 * {@code
 *   <loginModuleClass> <controlFlag> (<optionName>=<optionValue>)*;
 * }
 * </pre>
 */
public class JaasConfiguration extends Configuration {

    private final String loginContextName;
    private final List<AppConfigurationEntry> configEntries;

    public JaasConfiguration(String loginContextName, String jaasConfigParams) {
        StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(jaasConfigParams));
        tokenizer.slashSlashComments(true);
        tokenizer.slashStarComments(true);
        tokenizer.wordChars('-', '-');
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('$', '$');

        try {
            configEntries = new ArrayList<>();
            while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
                configEntries.add(parseAppConfigurationEntry(tokenizer));
            }
            if (configEntries.isEmpty())
                throw new IllegalArgumentException("Login module not specified in JAAS config");

            this.loginContextName = loginContextName;

        } catch (IOException e) {
            throw new IllegalArgumentException("Unexpected exception while parsing JAAS config", e);
        }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (this.loginContextName.equals(name))
            return configEntries.toArray(new AppConfigurationEntry[0]);
        else
            return  null;
    }

    private LoginModuleControlFlag loginModuleControlFlag(String flag) {
        LoginModuleControlFlag controlFlag;
        switch (flag.toUpperCase(Locale.ROOT)) {
            case "REQUIRED":
                controlFlag = LoginModuleControlFlag.REQUIRED;
                break;
            case "REQUISITE":
                controlFlag = LoginModuleControlFlag.REQUISITE;
                break;
            case "SUFFICIENT":
                controlFlag = LoginModuleControlFlag.SUFFICIENT;
                break;
            case "OPTIONAL":
                controlFlag = LoginModuleControlFlag.OPTIONAL;
                break;
            default:
                throw new IllegalArgumentException("Invalid login module control flag '" + flag + "' in JAAS config");
        }
        return controlFlag;
    }

    private AppConfigurationEntry parseAppConfigurationEntry(StreamTokenizer tokenizer) throws IOException {
        String loginModule = tokenizer.sval;
        if (tokenizer.nextToken() == StreamTokenizer.TT_EOF)
            throw new IllegalArgumentException("Login module control flag not specified in JAAS config");
        LoginModuleControlFlag controlFlag = loginModuleControlFlag(tokenizer.sval);
        Map<String, String> options = new HashMap<>();
        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF && tokenizer.ttype != ';') {
            String key = tokenizer.sval;
            if (tokenizer.nextToken() != '=' || tokenizer.nextToken() == StreamTokenizer.TT_EOF || tokenizer.sval == null)
                throw new IllegalArgumentException("Value not specified for key '" + key + "' in JAAS config");
            String value = tokenizer.sval;
            options.put(key, value);
        }
        if (tokenizer.ttype != ';')
            throw new IllegalArgumentException("JAAS config entry not terminated by semi-colon");
        return new AppConfigurationEntry(loginModule, controlFlag, options);
    }
}

