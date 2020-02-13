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

package com.hortonworks.registries.common;

import com.hortonworks.registries.auth.KerberosLogin;
import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.auth.NOOPLogin;
import com.hortonworks.registries.auth.util.JaasConfiguration;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.auth.login.LoginException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SecureClient {

    private Client client;
    private Login login;

    private SecureClient(Client client, Login login) {
        this.client = client;
        this.login = login;
    }

    public Client getClient() {
        return client;
    }

    public Login getLogin() {
        return login;
    }

    public Client register(Class<?> componentClass) {
       return client.register(componentClass);
    }

    public Client register(Object component) {
        return client.register(component);
    }

    public WebTarget target(String uri) {
        return client.target(uri);
    }

    public <T> T doAction(PrivilegedAction<T> action) throws LoginException {
        return login.doAction(action);
    }

    public void close() {
        client.close();
    }

    public static class Builder {

        private static final Logger LOG = LoggerFactory.getLogger(SecureClient.class);

        private ClientBuilder clientBuilder;

        private static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
        private static final int DEFAULT_READ_TIMEOUT = 30 * 1000;


        private static final String SSL_KEY_PASSWORD = "keyPassword";
        private static final String SSL_KEY_STORE_PATH = "keyStorePath";
        private static final String SSL_KEY_STORE_TYPE = "keyStoreType";
        private static final String SSL_KEY_STORE_PASSWORD = "keyStorePassword";
        private static final String SSL_KEY_STORE_PROVIDER = "keyStoreProvider";
        private static final String SSL_KEY_MANAGER_FACTORY_ALGORITHM = "keyManagerFactoryAlgorithm";
        private static final String SSL_KEY_MANAGER_FACTORY_PROVIDER = "keyManagerFactoryProvider";

        private static final String SSL_TRUST_STORE_TYPE = "trustStoreType";
        private static final String SSL_TRUST_STORE_PATH = "trustStorePath";
        private static final String SSL_TRUST_STORE_PASSWORD = "trustStorePassword";
        private static final String SSL_TRUST_STORE_PROVIDER = "trustStoreProvider";
        private static final String SSL_TRUST_MANAGER_FACTORY_ALGORITHM = "trustManagerFactoryAlgorithm";
        private static final String SSL_TRUST_MANAGER_FACTORY_PROVIDER = "trustManagerFactoryProvider";

        private static final String HOSTNAME_VERIFIER_CLASS_KEY = "hostnameVerifierClass";

        private static final String SSL_PROTOCOL = "protocol";

        private Client client;
        private Login login;

        public Builder() {
            clientBuilder = JerseyClientBuilder.newBuilder();
        }

        public Builder authentication(String saslJaasConfig, String loginContextName, long kerberosSynchronizationTimeoutMs) {
            if (saslJaasConfig != null) {
                KerberosLogin kerberosLogin = new KerberosLogin(kerberosSynchronizationTimeoutMs);
                try {
                    kerberosLogin.configure(new HashMap<>(), loginContextName, new JaasConfiguration(loginContextName, saslJaasConfig));
                    kerberosLogin.login();
                    login = kerberosLogin;
                } catch (LoginException e) {
                    LOG.error("Failed to initialize the dynamic JAAS config: " + saslJaasConfig + ". Attempting static JAAS config.");
                } catch (Exception e) {
                    LOG.error("Failed to parse the dynamic JAAS config. Attempting static JAAS config.", e);
                }
            }

            String jaasConfigFile = System.getProperty("java.security.auth.login.config");
            if (jaasConfigFile != null && !jaasConfigFile.trim().isEmpty()) {
                KerberosLogin kerberosLogin = new KerberosLogin(kerberosSynchronizationTimeoutMs);
                kerberosLogin.configure(new HashMap<>(), loginContextName);
                try {
                    kerberosLogin.login();
                    login = kerberosLogin;
                } catch (LoginException e) {
                    LOG.error("Could not login using jaas config  section " + loginContextName);
                    login = new NOOPLogin();
                }
            } else {
                LOG.warn("System property for jaas config file is not defined. Its okay if schema registry is not running in secured mode");
                login = new NOOPLogin();
            }

            return this;
        }

        public Builder ssl(Map<String, String> sslConfig) {
            clientBuilder.sslContext(createSSLContext(sslConfig));
            if (sslConfig.containsKey(HOSTNAME_VERIFIER_CLASS_KEY)) {
                String hostNameVerifierClassName = sslConfig.get(HOSTNAME_VERIFIER_CLASS_KEY);
                clientBuilder.hostnameVerifier(createHostnameVerifier(hostNameVerifierClassName));
            }

            return this;
        }

        private SSLContext createSSLContext(Map<String, String> sslConfigurations) {
            SslConfigurator sslConfigurator = SslConfigurator.newInstance();
            if (sslConfigurations.containsKey(SSL_KEY_STORE_PATH)) {
                sslConfigurator.keyStoreType(sslConfigurations.get(SSL_KEY_STORE_TYPE))
                        .keyStoreFile(sslConfigurations.get(SSL_KEY_STORE_PATH))
                        .keyStorePassword(sslConfigurations.get(SSL_KEY_STORE_PASSWORD))
                        .keyStoreProvider(sslConfigurations.get(SSL_KEY_STORE_PROVIDER))
                        .keyManagerFactoryAlgorithm(sslConfigurations.get(SSL_KEY_MANAGER_FACTORY_ALGORITHM))
                        .keyManagerFactoryProvider(sslConfigurations.get(SSL_KEY_MANAGER_FACTORY_PROVIDER));
                if (sslConfigurations.containsKey(SSL_KEY_PASSWORD)) {
                    sslConfigurator.keyPassword(sslConfigurations.get(SSL_KEY_PASSWORD));
                }
            }

            sslConfigurator.trustStoreType(sslConfigurations.get(SSL_TRUST_STORE_TYPE))
                    .trustStoreFile(sslConfigurations.get(SSL_TRUST_STORE_PATH))
                    .trustStorePassword(sslConfigurations.get(SSL_TRUST_STORE_PASSWORD))
                    .trustStoreProvider(sslConfigurations.get(SSL_TRUST_STORE_PROVIDER))
                    .trustManagerFactoryAlgorithm(sslConfigurations.get(SSL_TRUST_MANAGER_FACTORY_ALGORITHM))
                    .trustManagerFactoryProvider(sslConfigurations.get(SSL_TRUST_MANAGER_FACTORY_PROVIDER));

            sslConfigurator.securityProtocol(sslConfigurations.get(SSL_PROTOCOL));

            return sslConfigurator.createSSLContext();
        }

        private HostnameVerifier createHostnameVerifier(String hostnameVerifierClassName) {
            try {
                return (HostnameVerifier) Class.forName(hostnameVerifierClassName).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate hostnameVerifierClass : " + hostnameVerifierClassName, e);
            }
        }

        public SecureClient build(Map<String, ?> conf) {
            ClientConfig config = new ClientConfig();
            config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
            config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
            config.property(ClientProperties.FOLLOW_REDIRECTS, true);
            for (Map.Entry<String, ?> entry : conf.entrySet()) {
                config.property(entry.getKey(), entry.getValue());
            }

            client = clientBuilder.withConfig(config).property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE).build();

            return new SecureClient(client, login);
        }

        public SecureClient build() {
            return build(Collections.EMPTY_MAP);
        }
    }
}
