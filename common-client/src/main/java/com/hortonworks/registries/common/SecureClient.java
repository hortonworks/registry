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
import java.util.HashMap;
import java.util.Map;

public class SecureClient implements AutoCloseable {

    private Client client;
    private Login login;

    private SecureClient(Client client, Login login) {
        this.client = client;
        this.login = login;
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

        private static final Logger LOG = LoggerFactory.getLogger(Builder.class);

        // ---------------------------------------------
        //   Jersey client connection properties
        // ---------------------------------------------

        private static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;
        private static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

        // -----------------------------------------
        //   SSL configuration properties
        // -----------------------------------------

        private static final String SSL_PROTOCOL = "protocol";

        // SSL key store properties
        private static final String SSL_KEY_PASSWORD = "keyPassword";
        private static final String SSL_KEY_STORE_PATH = "keyStorePath";
        private static final String SSL_KEY_STORE_TYPE = "keyStoreType";
        private static final String SSL_KEY_STORE_PASSWORD = "keyStorePassword";
        private static final String SSL_KEY_STORE_PROVIDER = "keyStoreProvider";
        private static final String SSL_KEY_MANAGER_FACTORY_ALGORITHM = "keyManagerFactoryAlgorithm";
        private static final String SSL_KEY_MANAGER_FACTORY_PROVIDER = "keyManagerFactoryProvider";

        // SSL trust store properties
        private static final String SSL_TRUST_STORE_TYPE = "trustStoreType";
        private static final String SSL_TRUST_STORE_PATH = "trustStorePath";
        private static final String SSL_TRUST_STORE_PASSWORD = "trustStorePassword";
        private static final String SSL_TRUST_STORE_PROVIDER = "trustStoreProvider";
        private static final String SSL_TRUST_MANAGER_FACTORY_ALGORITHM = "trustManagerFactoryAlgorithm";
        private static final String SSL_TRUST_MANAGER_FACTORY_PROVIDER = "trustManagerFactoryProvider";

        private static final String HOSTNAME_VERIFIER_CLASS_KEY = "hostnameVerifierClass";

        private String jaasConfig = null;
        private String loginContextName = null;
        private long saslReloginSynchronizationTimeoutMs = 180000L;

        private Map<String, String> sslConfig = null;
        private Map<String, ?> clientConfig = null;

        public Builder jaasConfig(String jaasConfig) {
            this.jaasConfig = jaasConfig;
            return this;
        }

        public Builder loginContextName(String loginContextName) {
            this.loginContextName = loginContextName;
            return this;
        }

        public Builder saslReloginSynchronizationTimeoutMs(long saslReloginSynchronizationTimeoutMs) {
            this.saslReloginSynchronizationTimeoutMs = saslReloginSynchronizationTimeoutMs;
            return this;
        }

        public Builder sslConfig(Map<String, String> sslConfig) {
            this.sslConfig = sslConfig;
            return this;
        }

        public Builder clientConfig(Map<String, ?> clientConfig) {
            if (clientConfig != null) {
                this.clientConfig = clientConfig;
            }
            return this;
        }

        public SecureClient build() {
            return new SecureClient(createClient(), createAuthenticationContext());
        }

        private Client createClient() {
            ClientConfig config = new ClientConfig();
            config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
            config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
            config.property(ClientProperties.FOLLOW_REDIRECTS, true);
            if (clientConfig != null) {
                for (Map.Entry<String, ?> entry : clientConfig.entrySet()) {
                    config.property(entry.getKey(), entry.getValue());
                }
            }

            ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder()
                    .withConfig(config)
                    .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE);

            return imbueSSLContext(clientBuilder).build();
        }

        private ClientBuilder imbueSSLContext(ClientBuilder clientBuilder) {
            if (sslConfig == null || sslConfig.isEmpty()) {
                return clientBuilder;
            } else {
                clientBuilder.sslContext(createSSLContext(sslConfig));
                if (sslConfig.containsKey(HOSTNAME_VERIFIER_CLASS_KEY)) {
                    String hostNameVerifierClassName = sslConfig.get(HOSTNAME_VERIFIER_CLASS_KEY);
                    clientBuilder.hostnameVerifier(createHostnameVerifier(hostNameVerifierClassName));
                }
                return clientBuilder;
            }
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

        private Login createAuthenticationContext() {
            if (jaasConfig != null && loginContextName != null) {
                KerberosLogin kerberosLogin = new KerberosLogin(saslReloginSynchronizationTimeoutMs);
                try {
                    kerberosLogin.configure(new HashMap<>(), loginContextName, new JaasConfiguration(loginContextName, jaasConfig));
                    kerberosLogin.login();
                    return kerberosLogin;
                } catch (LoginException e) {
                    LOG.error("Failed to initialize the dynamic JAAS config: " + jaasConfig + ". Attempting static JAAS config.");
                } catch (Exception e) {
                    LOG.error("Failed to parse the dynamic JAAS config. Attempting static JAAS config.", e);
                }
            }

            String jaasConfigFile = System.getProperty("java.security.auth.login.config");
            if (loginContextName != null && jaasConfigFile != null && !jaasConfigFile.trim().isEmpty()) {
                KerberosLogin kerberosLogin = new KerberosLogin(saslReloginSynchronizationTimeoutMs);
                kerberosLogin.configure(new HashMap<>(), loginContextName);
                try {
                    kerberosLogin.login();
                    return kerberosLogin;
                } catch (LoginException e) {
                    LOG.error("Could not login using jaas config  section " + loginContextName);
                    return new NOOPLogin();
                }
            } else {
                LOG.warn("System property for jaas config file is not defined. Its okay if schema registry is not running in secured mode");
                return new NOOPLogin();
            }
        }
    }
}
