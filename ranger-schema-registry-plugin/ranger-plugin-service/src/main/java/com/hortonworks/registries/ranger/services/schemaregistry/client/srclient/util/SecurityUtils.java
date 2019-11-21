package com.hortonworks.registries.ranger.services.schemaregistry.client.srclient.util;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Optional;

public class SecurityUtils {

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
}
