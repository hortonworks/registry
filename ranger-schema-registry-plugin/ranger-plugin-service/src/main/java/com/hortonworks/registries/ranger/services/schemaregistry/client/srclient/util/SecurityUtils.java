package com.hortonworks.registries.ranger.services.schemaregistry.client.srclient.util;

import com.hortonworks.registries.auth.KerberosLogin;
import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.auth.NOOPLogin;
import com.hortonworks.registries.auth.util.JaasConfiguration;
import com.hortonworks.registries.ranger.services.schemaregistry.RangerServiceSchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

public class SecurityUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityUtils.class);
    private static final long KERBEROS_SYNCHRONIZATION_TIMEOUT_MS = 180000;
    private static final String REGISTY_CLIENT_JAAS_SECTION = "RegistryClient";

    public static boolean isHttpsConnection(Map<String, ?> conf) {
        String urls = conf.get(SCHEMA_REGISTRY_URL.name()).toString();
        return urls.trim().startsWith("https://");
    }

    public static SSLContext createSSLContext(Map<String, ?> sslConfigurations, String sslAlgorithm) throws Exception {

        SSLContext context = SSLContext.getInstance(sslAlgorithm);

        KeyManager[] km = null;

        String keyStorePath = (String)sslConfigurations.get("keyStorePath");
        if (keyStorePath == null || keyStorePath.isEmpty()) {
            keyStorePath = System.getProperty("javax.net.ssl.keyStore");
        }
        String keyStorePassword = (String)sslConfigurations.get("keyStorePassword");
        if (keyStorePassword == null || keyStorePath.isEmpty()) {
            keyStorePassword = Optional.ofNullable(System.getProperty("javax.net.ssl.keyStorePassword")).orElse("");
        }
        String keyStoreType = (String)sslConfigurations.get("keyStoreType");
        if (keyStoreType == null || keyStoreType.isEmpty()) {
            keyStoreType = System.getProperty("javax.net.ssl.keyStoreType");
        }

        String trustStorePath = (String)sslConfigurations.get("trustStorePath");
        if (trustStorePath == null || trustStorePath.isEmpty()) {
            trustStorePath = System.getProperty("javax.net.ssl.trustStore");
        }
        String trustStorePassword = (String)sslConfigurations.get("trustStorePassword");
        if (trustStorePassword == null || trustStorePassword.isEmpty()) {
            trustStorePassword = Optional.ofNullable(System.getProperty("javax.net.ssl.trustStorePassword")).orElse("");
        }
        String trustStoreType = (String)sslConfigurations.get("trustStoreType");
        if (trustStoreType == null || trustStoreType.isEmpty()) {
            trustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
        }

        Object obj = sslConfigurations.get("serverCertValidation");
        boolean serverCertValidation = (obj == null) || Boolean.parseBoolean(obj.toString());

        if (keyStorePath != null) {
            KeyStore ks = KeyStore.getInstance(keyStoreType != null ?
                    keyStoreType : KeyStore.getDefaultType());

            InputStream in;

            in = getFileInputStream(keyStorePath);

            try {
                ks.load(in, keyStorePassword.toCharArray());
            } finally {
                if (in != null) {
                    in.close();
                }
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyStorePassword.toCharArray());
            km = kmf.getKeyManagers();
        }

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        TrustManager[] tm = null;

        if (serverCertValidation) {
            if (trustStorePath != null) {
                KeyStore trustStoreKeyStore = KeyStore.getInstance(trustStoreType != null ?
                        trustStoreType : KeyStore.getDefaultType());

                InputStream in;

                in = getFileInputStream(trustStorePath);

                try {
                    trustStoreKeyStore.load(in, trustStorePassword.toCharArray());

                    trustManagerFactory.init(trustStoreKeyStore);

                    tm = trustManagerFactory.getTrustManagers();

                } finally {
                    if (in != null) {
                        in.close();
                    }
                }
            }
        } else {
            TrustManager ignoreValidationTM = new X509TrustManager() {
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    // Ignore Server Certificate Validation
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkServerTrusted(X509Certificate[] chain,
                                               String authType)
                        throws CertificateException {
                    // Ignore Server Certificate Validation
                }
            };

            tm  = new TrustManager[] {ignoreValidationTM};
        }

        SecureRandom random = new SecureRandom();

        context.init(km, tm, random);

        return context;

    }

    static private InputStream getFileInputStream(String path) throws FileNotFoundException {

        InputStream ret;

        File f = new File(path);

        if (f.exists()) {
            ret = new FileInputStream(f);
        } else {
            ret = SecurityUtils.class.getResourceAsStream(path);

            if (ret == null) {
                if (! path.startsWith("/")) {
                    ret = SecurityUtils.class.getResourceAsStream("/" + path);
                }
            }

            if (ret == null) {
                ret = ClassLoader.getSystemClassLoader().getResourceAsStream(path);
                if (ret == null) {
                    if (! path.startsWith("/")) {
                        ret = ClassLoader.getSystemResourceAsStream("/" + path);
                    }
                }
            }
        }

        return ret;
    }

    private static String getJaasConfigForClientPrincipal(Map<String, ?> conf) {
        String keytabFile = (String)conf.get(RangerServiceSchemaRegistry.LOOKUP_KEYTAB);
        String principal = (String)conf.get(RangerServiceSchemaRegistry.LOOKUP_PRINCIPAL);

        if(keytabFile == null || keytabFile.isEmpty()
                || principal == null || principal.isEmpty()) {
            return null;
        }

        return "com.sun.security.auth.module.Krb5LoginModule required useTicketCache=false principal="
                + principal
                + "useKeyTab=true keyTab=\""
                + keytabFile;
    }

    public static Login initializeSecurityContext(Map<String, ?> conf) {
        String saslJaasConfig = getJaasConfigForClientPrincipal(conf);
        boolean kerberosOn = isKerberosEnabled(conf);
        if (kerberosOn && saslJaasConfig != null) {
            KerberosLogin kerberosLogin = new KerberosLogin(KERBEROS_SYNCHRONIZATION_TIMEOUT_MS);
            try {
                kerberosLogin.configure(new HashMap<>(), REGISTY_CLIENT_JAAS_SECTION, new JaasConfiguration(REGISTY_CLIENT_JAAS_SECTION, saslJaasConfig));
                kerberosLogin.login();
                return kerberosLogin;
            } catch (LoginException e) {
                LOG.error("Failed to initialize the dynamic JAAS config: " + saslJaasConfig + ". Attempting static JAAS config.");
            } catch (Exception e) {
                LOG.error("Failed to parse the dynamic JAAS config. Attempting static JAAS config.", e);
            }
        }

        String jaasConfigFile = System.getProperty("java.security.auth.login.config");
        if (kerberosOn && jaasConfigFile != null && !jaasConfigFile.trim().isEmpty()) {
            KerberosLogin kerberosLogin = new KerberosLogin(KERBEROS_SYNCHRONIZATION_TIMEOUT_MS);
            kerberosLogin.configure(new HashMap<>(), REGISTY_CLIENT_JAAS_SECTION);
            try {
                kerberosLogin.login();
                return kerberosLogin;
            } catch (LoginException e) {
                LOG.error("Could not login using jaas config  section " + REGISTY_CLIENT_JAAS_SECTION);
                return new NOOPLogin();
            }
        } else {
            LOG.info("Kerberos is not enabled. Its okay if schema registry is not running in secured mode");
            return new NOOPLogin();
        }
    }

    private static boolean isKerberosEnabled(Map<String, ?> conf) {
        String rangerAuthType = (String) conf.get(RangerServiceSchemaRegistry.RANGER_AUTH_TYPE);
        String pluginAuthType = (String) conf.get("schema-registry.authentication");

        return rangerAuthType != null
                && rangerAuthType.equals(RangerServiceSchemaRegistry.KERBEROS_TYPE)
                && pluginAuthType.equalsIgnoreCase(RangerServiceSchemaRegistry.KERBEROS_TYPE);
    }

}
